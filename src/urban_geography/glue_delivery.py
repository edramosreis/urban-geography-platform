"""Create-only release delivery through an injected S3 client, with exact readback."""

import base64
import hashlib
from pathlib import Path
from typing import Any, Protocol

from urban_geography.glue_release import load_release
from urban_geography.glue_s3_input import _fetch


class S3DeliveryClient(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


def delivery_plan(path: Path, expected_sha256: str) -> dict[str, Any]:
    manifest = load_release(path, expected_sha256)
    names = sorted(manifest["keys"], key=lambda name: (name == "input/input-manifest.json", name))
    return {
        "release_id": manifest["release_id"],
        "bucket": manifest["bucket"],
        "expected_bucket_owner": manifest["identity"]["target"]["account_id"],
        "objects": [
            {"path": name, "key": manifest["keys"][name], **manifest["identity"]["files"][name]}
            for name in names
        ]
        + [
            {
                "path": "release.json",
                "key": manifest["completion_key"],
                "sha256": expected_sha256,
                "bytes": (path / "release.json").stat().st_size,
            }
        ],
    }


def _read_checked(path: Path, item: dict[str, Any]) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Delivery requires a regular release file")
    with path.open("rb") as stream:
        content = stream.read(item["bytes"] + 1)
    if len(content) != item["bytes"] or hashlib.sha256(content).hexdigest() != item["sha256"]:
        raise ValueError("Release file changed before upload")
    return content


def _deliver(
    client: S3DeliveryClient, plan: dict[str, Any], item: dict[str, Any], content: bytes
) -> dict[str, Any]:
    created = True
    version = None
    try:
        response = client.put_object(
            Bucket=plan["bucket"],
            Key=item["key"],
            ExpectedBucketOwner=plan["expected_bucket_owner"],
            Body=content,
            ContentLength=len(content),
            ChecksumSHA256=base64.b64encode(bytes.fromhex(item["sha256"])).decode("ascii"),
            ServerSideEncryption="AES256",
            IfNoneMatch="*",
        )
        version = response.get("VersionId")
        if not isinstance(version, str) or not version or version == "null":
            raise ValueError("Delivery requires versioned S3 objects")
    except Exception as exc:
        # boto3 remains optional: recognize only its precise create-only conflict shape.
        error = getattr(exc, "response", {})
        if (
            error.get("Error", {}).get("Code") not in {"PreconditionFailed", "412"}
            or error.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412
        ):
            raise
        created = False
    restored, read_version = _fetch(
        client, plan["bucket"], plan["expected_bucket_owner"], item["key"], item["bytes"]
    )
    if len(restored) != item["bytes"] or hashlib.sha256(restored).hexdigest() != item["sha256"]:
        raise ValueError("S3 readback differs from the frozen release")
    if created and read_version != version:
        raise ValueError("S3 object version changed before readback")
    return {
        "key": item["key"],
        "sha256": item["sha256"],
        "bytes": item["bytes"],
        "version_id": read_version,
        "created": created,
    }


def upload_release(client: S3DeliveryClient, path: Path, expected_sha256: str) -> dict[str, Any]:
    """No overwrite, listing, deletion, job launch or caller-owned client cleanup."""
    plan = delivery_plan(path, expected_sha256)
    delivered = []
    for item in plan["objects"]:
        if item["path"] == "release.json":
            # Revalidate everything before making this release discoverably complete.
            load_release(path, expected_sha256)
        content = _read_checked(path / item["path"], item)
        delivered.append(_deliver(client, plan, item, content))
    return {
        "status": "complete",
        "release_id": plan["release_id"],
        "bucket": plan["bucket"],
        "manifest_sha256": expected_sha256,
        "objects": delivered,
        "put_requests": len(delivered),
        "get_requests": len(delivered),
        "created": sum(item["created"] for item in delivered),
        "reused": sum(not item["created"] for item in delivered),
    }
