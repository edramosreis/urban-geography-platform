"""Release-bound, read-only S3 inventory; never delete, stop work or discover credentials."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from athena_control import canonical, write_once

from urban_geography.glue_input import PROJECT_ROOT
from urban_geography.glue_release import load_release
from urban_geography.glue_runtime import runtime_plan

ARTIFACTS = PROJECT_ROOT / "artifacts"
PAGE_SIZE = 100
MAX_PAGES = 10
MAX_ENTRIES = 1000
MAX_PAGE_BYTES = 256 * 1024
MAX_REPORT_BYTES = 4 * 1024 * 1024


class InventoryClient(Protocol):
    def get_bucket_location(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]: ...
    def list_object_versions(self, **kwargs: Any) -> dict[str, Any]: ...
    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]: ...
    def list_multipart_uploads(self, **kwargs: Any) -> dict[str, Any]: ...


def cleanup_scope(release: Path, pin: str) -> dict[str, Any]:
    manifest = load_release(release, pin)
    target = manifest["identity"]["target"]
    recipe = runtime_plan(**target)
    deployment, owner, region = (target[k] for k in ("deployment_name", "account_id", "region"))
    return {
        "version": 1,
        "status": "local_cleanup_inventory_plan",
        "target": target,
        "release_id": manifest["release_id"],
        "release_manifest_sha256": pin,
        "cleanup_role_arn": f"arn:aws:iam::{owner}:role/{deployment}-trial-cleanup",
        "buckets": {
            "lake": {
                "name": manifest["bucket"],
                "versioning": "Enabled",
                "exact_keys": sorted([*manifest["keys"].values(), manifest["completion_key"]]),
                "prefixes": [
                    "warehouse/building_proximity/",
                    "scratch/glue/" + manifest["release_id"] + "/",
                ],
            },
            "results": {
                "name": f"{deployment}-results-{owner}-{region}",
                "versioning": None,
                "exact_keys": [],
                "prefixes": ["queries/"],
            },
        },
        "expected_services": {
            "database": recipe["database"],
            "tables": ["building_proximity", "grid_proximity", "release_proximity"],
            "workgroup": deployment + "-m4",
            "job": deployment + "-pilot-writer",
        },
        "limits": {
            "page_size": PAGE_SIZE,
            "max_pages_per_listing": MAX_PAGES,
            "max_total_entries": MAX_ENTRIES,
            "max_page_bytes": MAX_PAGE_BYTES,
            "max_report_bytes": MAX_REPORT_BYTES,
            "max_s3_calls": 4 + 4 * MAX_PAGES,
        },
        "read_only": True,
        "deletion_authorized": False,
        "quiescence_and_catalog_observed": False,
    }


def in_scope(scope: dict[str, Any], kind: str, key: str) -> bool:
    bucket = scope["buckets"][kind]
    # S3 keys are opaque: do not normalize paths or decode percent sequences again.
    return key in bucket["exact_keys"] or any(key.startswith(p) for p in bucket["prefixes"])


def _text(value: Any, maximum: int = 1024) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        raise ValueError("Missing or oversized inventory identifier")
    return value


def _date(value: Any) -> str:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Inventory requires an aware SDK timestamp")
    return value.astimezone(UTC).isoformat()


def _entry(row: dict[str, Any], scope: dict[str, Any], bucket: str, kind: str) -> dict[str, Any]:
    key = _text(row.get("Key"))
    value = {
        "bucket": scope["buckets"][bucket]["name"],
        "bucket_kind": bucket,
        "kind": kind,
        "key": key,
        "in_cleanup_scope": in_scope(scope, bucket, key),
    }
    if kind == "multipart_upload":
        return value | {
            "upload_id": _text(row.get("UploadId")),
            "initiated_utc": _date(row.get("Initiated")),
            "bytes": None,
        }
    if kind in {"version", "delete_marker"}:
        if type(row.get("IsLatest")) is not bool:
            raise ValueError("Missing version latest flag")
        value.update(version_id=_text(row.get("VersionId")), is_latest=row["IsLatest"])
    value["modified_utc"] = _date(row.get("LastModified"))
    if kind == "delete_marker":
        return value | {"bytes": 0}
    size = row.get("Size")
    if type(size) is not int or not 0 <= size <= 2**63 - 1:
        raise ValueError("Invalid inventory byte count")
    return value | {"bytes": size, "etag": _text(row.get("ETag"))}


def _save(path: Path, value: dict[str, Any], ceiling: int) -> str:
    content = canonical(value)
    if len(content) > ceiling:
        raise ValueError("Inventory record exceeds its local byte ceiling")
    write_once(path, content)
    return hashlib.sha256(content).hexdigest()


def inventory_s3(client: InventoryClient, release: Path, pin: str, output: Path) -> dict[str, Any]:
    """Four bounded listing streams with explicit owner; the caller owns its SDK client."""
    scope = cleanup_scope(release, pin)
    destination, source = output.resolve(), release.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or destination == ARTIFACTS.resolve()
        or not destination.is_relative_to(ARTIFACTS.resolve())
        or destination.is_relative_to(source)
        or source.is_relative_to(destination)
    ):
        raise ValueError("Use fresh inventory output under artifacts, separate from the release")
    output.mkdir(parents=True)
    plan_pin = _save(output / "plan.json", scope, 64 * 1024)
    entries: list[dict[str, Any]] = []
    pages: dict[str, str] = {}
    identities: set[tuple[str, ...]] = set()
    calls = 0
    stage = "bucket_preflight"
    observations = {}
    owner = scope["target"]["account_id"]

    def request(method: str, args: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls > scope["limits"]["max_s3_calls"]:
            raise ValueError("Inventory API call ceiling reached")
        return getattr(client, method)(**args)

    try:
        for bucket_kind in ("lake", "results"):
            bucket = scope["buckets"][bucket_kind]
            base = {"Bucket": bucket["name"], "ExpectedBucketOwner": owner}
            stage = bucket_kind + "_preflight"
            location = request("get_bucket_location", base)
            versioning = request("get_bucket_versioning", base)
            if (
                location.get("LocationConstraint") != scope["target"]["region"]
                or versioning.get("Status") != bucket["versioning"]
                or versioning.get("MFADelete") not in (None, "Disabled")
            ):
                raise ValueError(
                    "Bucket region, versioning or MFA-delete differs from the trial plan"
                )
            observations[bucket_kind] = {
                "region": location["LocationConstraint"],
                "versioning": versioning.get("Status"),
            }
            streams = ("versions", "uploads") if bucket_kind == "lake" else ("objects", "uploads")
            for stream in streams:
                stage = bucket_kind + "_" + stream
                cursor: dict[str, str] = {}
                cursors: set[tuple[tuple[str, str], ...]] = set()
                for ordinal in range(1, MAX_PAGES + 1):
                    marker = tuple(sorted(cursor.items()))
                    if marker in cursors:
                        raise ValueError("Inventory pagination made no progress")
                    cursors.add(marker)
                    method, count_key = {
                        "versions": ("list_object_versions", "MaxKeys"),
                        "objects": ("list_objects_v2", "MaxKeys"),
                        "uploads": ("list_multipart_uploads", "MaxUploads"),
                    }[stream]
                    response = request(method, base | {count_key: PAGE_SIZE} | cursor)
                    name_key = "Bucket" if stream == "uploads" else "Name"
                    if (
                        response.get(name_key) != bucket["name"]
                        or type(response.get("IsTruncated")) is not bool
                        or response.get("CommonPrefixes")
                        or response.get("Prefix", "") != ""
                        or response.get("Delimiter", "") != ""
                    ):
                        raise ValueError("Inventory page identity, scope or truncation differs")
                    groups = {
                        "versions": (("Versions", "version"), ("DeleteMarkers", "delete_marker")),
                        "objects": (("Contents", "object"),),
                        "uploads": (("Uploads", "multipart_upload"),),
                    }[stream]
                    rows = []
                    for field, kind in groups:
                        listed = response.get(field, [])
                        if not isinstance(listed, list):
                            raise ValueError("Invalid inventory page rows")
                        for row in listed:
                            value = _entry(row, scope, bucket_kind, kind)
                            if kind == "multipart_upload":
                                identity = (bucket_kind, "upload", value["key"], value["upload_id"])
                            else:
                                identity = (
                                    bucket_kind,
                                    "object",
                                    value["key"],
                                    value.get("version_id", "current"),
                                )
                            if identity in identities:
                                raise ValueError("Duplicate inventory object/version/upload")
                            identities.add(identity)
                            rows.append(value)
                    if len(rows) > PAGE_SIZE or len(entries) + len(rows) > MAX_ENTRIES:
                        raise ValueError("Inventory entry ceiling reached")
                    next_cursor = {}
                    if stream == "objects":
                        if response.get("NextContinuationToken"):
                            next_cursor["ContinuationToken"] = _text(
                                response["NextContinuationToken"], 4096
                            )
                    else:
                        if response.get("NextKeyMarker"):
                            next_cursor["KeyMarker"] = _text(response["NextKeyMarker"])
                        token = "UploadIdMarker" if stream == "uploads" else "VersionIdMarker"
                        if response.get("Next" + token):
                            next_cursor[token] = _text(response["Next" + token])
                    truncated = response["IsTruncated"]
                    if (
                        truncated
                        and (
                            not next_cursor
                            or (stream != "objects" and "KeyMarker" not in next_cursor)
                        )
                    ) or (not truncated and next_cursor):
                        raise ValueError("Inventory continuation markers differ from truncation")
                    filename = f"{bucket_kind}-{stream}-{ordinal:02}.json"
                    pages[filename] = _save(
                        output / filename,
                        {
                            "plan_sha256": plan_pin,
                            "bucket_kind": bucket_kind,
                            "stream": stream,
                            "page": ordinal,
                            "cursor": cursor,
                            "next_cursor": next_cursor,
                            "truncated": truncated,
                            "entries": rows,
                        },
                        MAX_PAGE_BYTES,
                    )
                    entries.extend(rows)
                    if not truncated:
                        break
                    cursor = next_cursor
                else:
                    raise ValueError("Inventory page ceiling reached before enumeration completed")
        inside = all(e["in_cleanup_scope"] for e in entries)
        report = {
            "version": 1,
            "status": "s3_inventory_complete" if inside else "s3_inventory_requires_review",
            "plan_sha256": plan_pin,
            "release_manifest_sha256": pin,
            "bucket_observations": observations,
            "entries": entries,
            "pages": pages,
            "api_calls": calls,
            "enumeration_complete": True,
            "all_keys_in_cleanup_scope": inside,
            "observed_object_bytes": sum(e["bytes"] for e in entries if e["bytes"] is not None),
            "multipart_bytes_known": not any(e["kind"] == "multipart_upload" for e in entries),
            "quiescence_and_catalog_observed": False,
            "deletion_authorized": False,
        }
        _save(output / "inventory.json", report, MAX_REPORT_BYTES)
        return report
    except BaseException as error:
        try:
            _save(
                output / "failure.json",
                {
                    "status": "s3_inventory_failed",
                    "plan_sha256": plan_pin,
                    "stage": stage,
                    "api_calls": calls,
                    "entries_retained": len(entries),
                    "error_type": type(error).__name__,
                    "enumeration_complete": False,
                    "deletion_authorized": False,
                },
                64 * 1024,
            )
        except Exception:
            pass
        raise
