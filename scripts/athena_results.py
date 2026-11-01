"""Bounded parsing/comparison of the fixed Athena Gold SELECT result; no submission."""

import csv
import hashlib
import io
import json
import math
import re
from contextlib import closing
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from athena_control import Attempt, QueryClient, observation, verified_plan
from athena_read_plan import same

from urban_geography.workload_limits import PILOT_LIMITS

MAX_SUMMARY_BYTES = 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024
SHARE_TOLERANCE = 1e-12
COLUMNS = (
    "source_release",
    "area_id",
    "source_run_sha256",
    "mapped_buildings",
    "buildings_within",
    "share_within",
)


def expected_gold(summary_path: Path, pin: str) -> dict[str, Any]:
    """Read one independently pinned summary; earlier source preflight owns fact verification."""
    if not isinstance(pin, str) or re.fullmatch(r"[0-9a-f]{64}", pin) is None:
        raise ValueError("Use an explicit lowercase source summary SHA256")
    if summary_path.is_symlink() or not summary_path.is_file():
        raise ValueError("Source summary must be a regular file")
    with summary_path.open("rb") as stream:
        content = stream.read(MAX_SUMMARY_BYTES + 1)
    if len(content) > MAX_SUMMARY_BYTES or hashlib.sha256(content).hexdigest() != pin:
        raise ValueError("Source summary size or SHA256 differs")
    summary = json.loads(content)
    config = summary["config"]
    total, within = summary["mapped_buildings"], summary["buildings_within_500m"]
    share = summary["share_within"]
    if (
        summary.get("status") != "complete"
        or type(total) is not int
        or not 0 < total <= PILOT_LIMITS.max_features
        or type(within) is not int
        or not 0 <= within <= total
        or type(share) not in (float, int)
        or not math.isfinite(share)
        or not math.isclose(share, within / total, rel_tol=0, abs_tol=SHARE_TOLERANCE)
        or config.get("workload_profile", "pilot") != "pilot"
        or not isinstance(config.get("area_id"), str)
        or re.fullmatch(r"[a-z0-9_]{1,256}", config["area_id"]) is None
        or not isinstance(config.get("release"), str)
        or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]{1,8}", config["release"]) is None
    ):
        raise ValueError("Expected Gold summary has invalid pilot scope, counts or share")
    return dict(
        zip(
            COLUMNS,
            (config["release"], config["area_id"], pin, total, within, within / total),
            strict=True,
        )
    )


def compare_gold_csv(content: bytes, expected: dict[str, Any]) -> dict[str, Any]:
    """Require the exact header and one pilot group; compare integer totals and finite share."""
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_RESULT_BYTES:
        raise ValueError("Gold result exceeds the byte ceiling or is empty")
    try:
        reader = csv.reader(io.StringIO(content.decode("utf-8"), newline=""), strict=True)
        header = next(reader, None)
        row = next(reader, None)
        extra = next(reader, None)
    except (UnicodeError, csv.Error) as exc:
        raise ValueError("Gold result must be valid UTF-8 CSV") from exc
    if header != list(COLUMNS) or row is None or len(row) != len(COLUMNS) or extra is not None:
        raise ValueError("Gold result requires the exact header and one release/area row")
    if any(len(cell) > 256 for cell in row):
        raise ValueError("Gold result cell exceeds its supported length")
    if tuple(row[:3]) != tuple(expected[name] for name in COLUMNS[:3]):
        raise ValueError("Gold result release, area or source hash differs")
    if any(re.fullmatch(r"0|[1-9][0-9]{0,4}", value) is None for value in row[3:5]):
        raise ValueError("Gold result counts must be canonical nonnegative integers")
    total, within = int(row[3]), int(row[4])
    if (total, within) != (expected["mapped_buildings"], expected["buildings_within"]):
        raise ValueError("Gold result building counts differ")
    if (
        len(row[5]) > 64
        or re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", row[5])
        is None
    ):
        raise ValueError("Gold result share must be a finite numeric value")
    share = float(row[5])
    if (
        not math.isfinite(share)
        or not 0 <= share <= 1
        or not math.isclose(share, expected["share_within"], rel_tol=0, abs_tol=SHARE_TOLERANCE)
    ):
        raise ValueError("Gold result share differs")
    return dict(zip(COLUMNS, (*row[:3], total, within, share), strict=True))


class ResultS3Client(Protocol):
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


def read_result_object(
    client: ResultS3Client, *, bucket: str, key: str, owner: str
) -> tuple[bytes, dict[str, Any]]:
    target = {"Bucket": bucket, "Key": key, "ExpectedBucketOwner": owner}
    head = client.head_object(**target)
    size, etag = head.get("ContentLength"), head.get("ETag")
    if (
        type(size) is not int
        or not 0 < size <= MAX_RESULT_BYTES
        or not isinstance(etag, str)
        or re.fullmatch(r'"[A-Za-z0-9-]{1,128}"', etag) is None
        or head.get("ServerSideEncryption") != "AES256"
        or head.get("ContentEncoding") not in (None, "identity")
        or head.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200
    ):
        raise ValueError("Query result object size, encryption or HEAD response differs")
    response = client.get_object(**target, IfMatch=etag, Range=f"bytes=0-{size - 1}")
    with closing(response["Body"]) as body:
        if (
            response.get("ContentLength") != size
            or type(response.get("ContentLength")) is not int
            or response.get("ETag") != etag
            or response.get("ContentRange") != f"bytes 0-{size - 1}/{size}"
            or response.get("ServerSideEncryption") != "AES256"
            or response.get("ContentEncoding") not in (None, "identity")
            or response.get("VersionId") != head.get("VersionId")
            or response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 206
        ):
            raise ValueError("Conditional query result range response differs")
        content = body.read(size + 1)
        if not isinstance(content, bytes) or len(content) != size:
            raise ValueError("Query result body length differs from the bounded object")
    return content, {
        "bucket": bucket,
        "key": key,
        "etag": etag,
        "bytes": size,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def saved_query(attempt: Attempt) -> bytes:
    path = attempt.output / "query.json"
    if path.is_symlink() or not path.is_file():
        raise ValueError("Use the query ID saved by this attempt")
    with path.open("rb") as stream:
        content = stream.read(1025)
    if len(content) > 1024:
        raise ValueError("Saved query identity exceeds its byte ceiling")
    return content


def successful_observation(
    client: QueryClient, request: dict[str, Any], query_id: str, scan_cap: int
) -> dict[str, Any]:
    observed = observation(client, request, query_id)
    if (
        observed["state"] != "SUCCEEDED"
        or observed["drift"]
        or observed["missing"]
        or type(observed["data_scanned_bytes"]) is not int
        or not 0 <= observed["data_scanned_bytes"] <= scan_cap
    ):
        raise ValueError("Query result requires verified terminal success within the scan cutoff")
    return observed


def verify_query_result(
    athena: QueryClient,
    s3: ResultS3Client,
    attempt: Attempt,
    execution: dict[str, Any],
    *,
    summary_path: Path,
) -> dict[str, Any]:
    """Read one completed attempt's exact output object and compare its Gold summary."""
    intent = attempt.intent()
    plan, request = verified_plan(intent["plan"]), intent["request"]
    query_bytes = saved_query(attempt)
    query_id = execution.get("query_id")
    if (
        execution.get("outcome") != "query_succeeded"
        or execution.get("terminal_confirmed") is not True
        or execution.get("intent_sha256") != attempt.intent_sha256
        or execution.get("query_sha256") != plan["query_sha256"]
        or not isinstance(query_id, str)
        or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query_id) is None
        or not same(json.loads(query_bytes), {"query_id": query_id})
    ):
        raise ValueError("Query result execution does not belong to this successful attempt")
    expected = expected_gold(summary_path, plan["binding"]["source_run_sha256"])
    cap = plan["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"]
    before = successful_observation(athena, request, query_id, cap)
    if not same(before, execution.get("last_observation")):
        raise ValueError("Completed query observation differs from the execution receipt")
    prefix = urlsplit(request["ResultConfiguration"]["OutputLocation"])
    content, object_report = read_result_object(
        s3,
        bucket=prefix.netloc,
        key=prefix.path.lstrip("/") + query_id + ".csv",
        owner=plan["target"]["account_id"],
    )
    row = compare_gold_csv(content, expected)
    after = successful_observation(athena, request, query_id, cap)
    if not same(before, after):
        raise ValueError("Completed query observation changed during result verification")
    attempt.intent()
    if saved_query(attempt) != query_bytes or not same(
        expected_gold(summary_path, plan["binding"]["source_run_sha256"]), expected
    ):
        raise ValueError("Attempt identity or expected summary changed during result verification")
    return {
        "contract_version": 1,
        "status": "gold_result_matches",
        "query_id": query_id,
        "intent_sha256": attempt.intent_sha256,
        "query_sha256": plan["query_sha256"],
        "binding": plan["binding"],
        "row": row,
        "expected": expected,
        "data_scanned_bytes": after["data_scanned_bytes"],
        "result_object": object_report,
        "result_verified": True,
        "caller_identity_verified": False,
        "source_readback_verified": False,
        "writer_job_identity_verified": False,
        "execution_authorized": False,
        "scope": "Gold result versus pinned summary; caller/source preflight remains separate",
    }
