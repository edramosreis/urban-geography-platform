"""Bounded parsing/comparison of the fixed Athena Gold SELECT result; no submission."""

import csv
import hashlib
import io
import json
import math
import re
from pathlib import Path
from typing import Any

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
