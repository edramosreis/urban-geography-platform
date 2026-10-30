"""Prepare a bounded, source-pinned retained input folder for the future Glue writer."""

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from urban_geography.config import RunConfig
from urban_geography.lakehouse import CompletedRun, load_completed_run
from urban_geography.workload_limits import PILOT_LIMITS

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAX_SUMMARY_BYTES = 1024 * 1024
MAX_PAYLOAD_BYTES = PILOT_LIMITS.max_output_bytes
MAX_PARQUET_FILES = 12  # Delivery files, distinct from the raw source-file counter.
MAX_ROWS = PILOT_LIMITS.max_features


@dataclass(frozen=True)
class InputPlan:
    run: CompletedRun
    files: dict[str, dict[str, Any]]
    payload_bytes: int


def inspect_input(path: Path, expected_summary_sha256: str) -> InputPlan:
    """Check the pinned identity and pilot limits before any output or Spark startup."""
    if not re.fullmatch(r"[0-9a-f]{64}", expected_summary_sha256):
        raise ValueError("An explicit lowercase SHA256 of summary.json is required")
    path = path.resolve()
    summary_path = path / "summary.json"
    if summary_path.is_symlink():
        raise ValueError("Input summary must be a regular retained file")
    with summary_path.open("rb") as source:
        content = source.read(MAX_SUMMARY_BYTES + 1)
    if len(content) > MAX_SUMMARY_BYTES:
        raise ValueError("Input summary exceeds the byte limit")
    if hashlib.sha256(content).hexdigest() != expected_summary_sha256:
        raise ValueError("Input summary checksum differs from the explicit pin")
    summary = json.loads(content)
    files = summary.get("files")
    if not isinstance(files, dict) or not 0 < len(files) <= MAX_PARQUET_FILES:
        raise ValueError("Input Parquet file count exceeds the pilot limit or is empty")
    payload = {"summary.json": {"sha256": expected_summary_sha256, "bytes": len(content)}}
    total = len(content)
    for name, expected in sorted(files.items()):
        if not isinstance(name, str) or not re.fullmatch(
            r"(building_proximity|grid_proximity)/[A-Za-z0-9][A-Za-z0-9._-]*\.parquet", name
        ):
            raise ValueError("Input requires flat, safe fact/grid Parquet paths")
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError("Input Parquet checksum is invalid")
        source_path = path / name
        if source_path.is_symlink() or source_path.parent.is_symlink() or not source_path.is_file():
            raise ValueError("Input Parquet must be a regular retained file")
        size = source_path.stat().st_size
        total += size
        if total > MAX_PAYLOAD_BYTES:
            raise ValueError("Input payload exceeds the pilot byte limit")
        payload[name] = {"sha256": expected, "bytes": size}
    config_values = dict(summary["config"])
    config_values["bbox"] = tuple(config_values["bbox"])
    config = RunConfig(**config_values)
    if config.workload_profile != "pilot":
        raise ValueError("Glue input preparation currently supports only the pilot profile")
    for field in ("max_download_bytes", "max_output_bytes", "max_features", "max_source_files"):
        value = getattr(config, field)
        if type(value) is not int or not 0 < value <= getattr(PILOT_LIMITS, field):
            raise ValueError("Input configuration exceeds the pilot ceilings")
    if total > min(config.max_output_bytes, MAX_PAYLOAD_BYTES):
        raise ValueError("Input payload exceeds the configured pilot byte limit")
    mapped = summary.get("mapped_buildings")
    if type(mapped) is not int or not 0 < mapped <= min(config.max_features, MAX_ROWS):
        raise ValueError("Input mapped row count exceeds the pilot limit or is invalid")
    run = load_completed_run(path)
    if run.summary_sha256 != expected_summary_sha256:
        raise ValueError("Input summary changed during inspection")
    fact_rows = sum(
        pq.read_metadata(path / name).num_rows
        for name in files
        if name.startswith("building_proximity/")
    )
    grid_rows = sum(
        pq.read_metadata(path / name).num_rows
        for name in files
        if name.startswith("grid_proximity/")
    )
    if fact_rows != mapped or grid_rows > min(config.max_features, MAX_ROWS):
        raise ValueError("Input Parquet row counts differ or exceed the pilot limit")
    return InputPlan(run, payload, total)


def _copy_verified(source: Path, target: Path, expected: dict[str, Any]) -> None:
    """Bound actual copied bytes and reject a file changed after inspection."""
    checksum = hashlib.sha256()
    copied = 0
    with source.open("rb") as original, target.open("xb") as output:
        while block := original.read(1024 * 1024):
            copied += len(block)
            if copied > expected["bytes"]:
                raise ValueError("Input grew while copying")
            checksum.update(block)
            output.write(block)
    if copied != expected["bytes"] or checksum.hexdigest() != expected["sha256"]:
        raise ValueError("Input checksum or size changed while copying")


def stage_input(path: Path, expected_summary_sha256: str, output: Path) -> dict[str, Any]:
    """Copy only verified summary/Parquet bytes, then publish a fresh local folder."""
    path, output = path.resolve(), output.resolve()
    if output.exists():
        raise ValueError("Input output already exists; choose a fresh directory")
    if output.is_relative_to(path) or path.is_relative_to(output):
        raise ValueError("Input output must be disjoint from the retained run")
    if any(output.is_relative_to(PROJECT_ROOT / name) for name in ("data", "src", ".git")):
        raise ValueError("Input output must be outside retained data, source and Git metadata")
    plan = inspect_input(path, expected_summary_sha256)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.parent / (".input-" + uuid.uuid4().hex)
    stage.mkdir()
    for name, expected in plan.files.items():
        target = stage / name
        target.parent.mkdir(parents=True, exist_ok=True)
        _copy_verified(path / name, target, expected)
    staged = inspect_input(stage, expected_summary_sha256)
    if staged.files != plan.files or staged.payload_bytes != plan.payload_bytes:
        raise ValueError("Staged input inventory differs")
    receipt = _input_manifest(plan)
    (stage / "input-manifest.json").write_text(
        json.dumps(receipt, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    stage.rename(output)
    return receipt


def _input_manifest(plan: InputPlan) -> dict[str, Any]:
    """Derive the completion receipt from verified input bytes and effective limits."""
    return {
        "status": "complete",
        "contract_version": 1,
        "source_run_sha256": plan.run.summary_sha256,
        "mapped_buildings": plan.run.summary["mapped_buildings"],
        "payload_bytes": plan.payload_bytes,
        "files": plan.files,
        "limits": {
            "summary_bytes": MAX_SUMMARY_BYTES,
            "payload_bytes_including_summary": min(
                plan.run.summary["config"]["max_output_bytes"], MAX_PAYLOAD_BYTES
            ),
            "parquet_files": MAX_PARQUET_FILES,
            "mapped_buildings": min(plan.run.summary["config"]["max_features"], MAX_ROWS),
            "grid_rows": min(plan.run.summary["config"]["max_features"], MAX_ROWS),
        },
        "aws_api_or_upload": False,
    }


def load_prepared_input(path: Path, expected_summary_sha256: str) -> InputPlan:
    """Verify both the prepared receipt and its source-pinned payload before Spark."""
    path = path.resolve()
    if path.name.startswith(".input-"):
        raise ValueError("Use the requested completed input directory, not unpublished staging")
    manifest_path = path / "input-manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("Prepared input requires a regular completion manifest")
    with manifest_path.open("rb") as source:
        content = source.read(MAX_SUMMARY_BYTES + 1)
    if len(content) > MAX_SUMMARY_BYTES:
        raise ValueError("Prepared input manifest exceeds the byte limit")
    manifest = json.loads(content)
    plan = inspect_input(path, expected_summary_sha256)
    # Canonical JSON distinguishes numeric/boolean types but permits key/whitespace changes.
    if json.dumps(manifest, sort_keys=True, allow_nan=False) != json.dumps(
        _input_manifest(plan), sort_keys=True, allow_nan=False
    ):
        raise ValueError("Prepared input manifest differs from its verified payload")
    return plan
