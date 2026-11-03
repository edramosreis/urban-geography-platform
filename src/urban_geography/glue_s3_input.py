"""Fetch only a pinned prepared input from S3 through an explicitly supplied client."""

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from urban_geography.glue_input import (
    MAX_PARQUET_FILES,
    MAX_PAYLOAD_BYTES,
    MAX_ROWS,
    MAX_SUMMARY_BYTES,
    PROJECT_ROOT,
    load_prepared_input,
)
from urban_geography.glue_runtime import runtime_plan


class S3InputClient(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


def _sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _fetch(
    client: S3InputClient, bucket: str, owner: str, key: str, limit: int
) -> tuple[bytes, str]:
    response = client.get_object(
        Bucket=bucket, Key=key, ExpectedBucketOwner=owner, Range=f"bytes=0-{limit}"
    )
    body = response["Body"]
    try:
        size, version = response["ContentLength"], response.get("VersionId")
        if type(size) is not int or not 0 <= size <= limit:
            raise ValueError("S3 response exceeds the bounded object length")
        if not isinstance(version, str) or not version or version == "null":
            raise ValueError("S3 input requires a versioned object")
        blocks = []
        received = 0
        while block := body.read(min(64 * 1024, limit + 1 - received)):
            received += len(block)
            if received > limit:
                raise ValueError("S3 response body exceeds the object limit")
            blocks.append(block)
        if received != size:
            raise ValueError("S3 response body length differs from its header")
        return b"".join(blocks), version
    finally:
        body.close()


def _inventory(manifest: dict[str, Any], pin: str) -> dict[str, Any]:
    if (
        manifest.get("status") != "complete"
        or type(manifest.get("contract_version")) is not int
        or manifest["contract_version"] != 1
        or manifest.get("source_run_sha256") != pin
    ):
        raise ValueError("S3 manifest identity or completion contract differs")
    files = manifest.get("files")
    if not isinstance(files, dict) or not 2 <= len(files) <= MAX_PARQUET_FILES + 1:
        raise ValueError("S3 manifest file count is outside the pilot limit")
    total = 0
    for name, item in files.items():
        if name != "summary.json" and not re.fullmatch(
            r"(building_proximity|grid_proximity)/[A-Za-z0-9][A-Za-z0-9._-]*\.parquet", name
        ):
            raise ValueError("S3 manifest contains an unsafe input path")
        if not isinstance(item, dict) or not _sha256(item.get("sha256")):
            raise ValueError("S3 manifest file checksum is invalid")
        size = item.get("bytes")
        cap = MAX_SUMMARY_BYTES if name == "summary.json" else MAX_PAYLOAD_BYTES
        if type(size) is not int or not 0 < size <= cap:
            raise ValueError("S3 manifest file size exceeds the pilot limit")
        total += size
    if "summary.json" not in files or files["summary.json"]["sha256"] != pin:
        raise ValueError("S3 manifest must contain the pinned summary")
    ceiling = manifest.get("limits", {}).get("payload_bytes_including_summary")
    if type(ceiling) is not int or not 0 < total <= ceiling <= MAX_PAYLOAD_BYTES:
        raise ValueError("S3 manifest payload exceeds its bounded pilot ceiling")
    if type(manifest.get("payload_bytes")) is not int or manifest["payload_bytes"] != total:
        raise ValueError("S3 manifest payload byte total differs")
    rows = manifest.get("mapped_buildings")
    if type(rows) is not int or not 0 < rows <= MAX_ROWS:
        raise ValueError("S3 manifest row count exceeds the pilot limit")
    return files


def download_prepared_input(
    client: S3InputClient,
    *,
    deployment_name: str,
    account_id: str,
    expected_summary_sha256: str,
    expected_manifest_sha256: str,
    output: Path,
    region: str = "eu-north-1",
) -> dict[str, Any]:
    """Reconstruct one prepared input using exact keys, bounded GETs and verified bytes."""
    output = output.resolve()
    if output.exists():
        raise ValueError("S3 input output already exists; choose a fresh directory")
    if any(output.is_relative_to(PROJECT_ROOT / name) for name in ("data", "src", ".git")):
        raise ValueError("S3 input output must be outside retained data, source and Git metadata")
    if not _sha256(expected_summary_sha256) or not _sha256(expected_manifest_sha256):
        raise ValueError("Explicit summary and manifest SHA256 pins are required")
    recipe = runtime_plan(deployment_name=deployment_name, account_id=account_id, region=region)
    bucket = urlsplit(recipe["warehouse"]).netloc
    prefix = "inputs/" + expected_summary_sha256 + "/"
    manifest_key = prefix + "input-manifest.json"
    content, version = _fetch(client, bucket, account_id, manifest_key, MAX_SUMMARY_BYTES)
    if hashlib.sha256(content).hexdigest() != expected_manifest_sha256:
        raise ValueError("S3 manifest checksum differs from its explicit pin")
    files = _inventory(json.loads(content), expected_summary_sha256)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.parent / (".download-" + uuid.uuid4().hex)
    stage.mkdir()
    versions = {"input-manifest.json": version}
    received = len(content)
    for name, item in sorted(files.items()):
        data, version = _fetch(client, bucket, account_id, prefix + name, item["bytes"])
        if len(data) != item["bytes"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise ValueError("S3 payload checksum or size differs: " + name)
        target = stage / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        versions[name] = version
        received += len(data)
    (stage / "input-manifest.json").write_bytes(content)
    plan = load_prepared_input(stage, expected_summary_sha256)
    receipt = {
        "status": "complete",
        "source_run_sha256": expected_summary_sha256,
        "input_manifest_sha256": expected_manifest_sha256,
        "bucket": bucket,
        "expected_bucket_owner": account_id,
        "prefix": prefix,
        "object_versions": versions,
        "get_requests": len(versions),
        "downloaded_body_bytes": received,
        "payload_bytes": plan.payload_bytes,
        "mapped_buildings": plan.run.summary["mapped_buildings"],
    }
    (stage / "transfer-receipt.json").write_text(
        json.dumps(receipt, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    stage.rename(output)
    return receipt
