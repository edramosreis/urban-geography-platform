"""Bounded offline QA of comparable processed releases; ID differences are map records."""

import math
from typing import Any

import pyarrow.parquet as pq

from urban_geography.lakehouse import FACT_TYPES, CompletedRun, analysis_definition, analysis_sha256


def _records(run: CompletedRun) -> dict[str, dict[str, Any]]:
    config = run.summary["config"]
    files = [
        run.path / name
        for name in sorted(run.summary["files"])
        if name.startswith("building_proximity/")
    ]
    total = sum(pq.read_metadata(path).num_rows for path in files)
    if not 0 < total <= config["max_features"] or total != run.summary["mapped_buildings"]:
        raise ValueError("Release fact count differs or exceeds the pilot limit")
    expected_types = FACT_TYPES | {"within_threshold": "bool"}
    for path in files:
        if {field.name: str(field.type) for field in pq.read_schema(path)} != expected_types:
            raise ValueError("Release fact schema differs from the supported contract")
    selection = run.summary["selection"]
    boundary = selection["boundary"] or {}
    expected = {
        "source_release": config["release"],
        "area_id": config["area_id"],
        "metric_version": run.summary["metric_version"],
        "threshold_m": config["threshold_m"],
        "selection_version": selection["version"],
        "boundary_id": boundary.get("area_id"),
        "boundary_release": boundary.get("source_release"),
        "boundary_geometry_sha256": boundary.get("geometry_sha256"),
    }
    records = {}
    for row in pq.read_table([str(path) for path in files]).to_pylist():
        key = row["building_id"]
        if not key or key in records:
            raise ValueError("Missing or duplicate comparison building IDs")
        if not row["grid_id"]:
            raise ValueError("Missing comparison grid ID")
        if any(row[name] != value for name, value in expected.items()):
            raise ValueError("Release fact metadata differs from its run summary")
        matched, distance, park = row["within_threshold"], row["distance_m"], row["park_id"]
        if not isinstance(matched, bool):
            raise ValueError("Release match flag must be non-null")
        if matched:
            if (
                not park
                or distance is None
                or not math.isfinite(distance)
                or not 0 <= distance <= config["threshold_m"]
            ):
                raise ValueError("Release matched result violates proximity semantics")
        elif park is not None or distance is not None:
            raise ValueError("Release unmatched result must have null park and distance")
        records[key] = row
    count_key = "buildings_within_500m" if config["threshold_m"] == 500 else "buildings_within"
    if sum(row["within_threshold"] for row in records.values()) != run.summary[count_key]:
        raise ValueError("Release matched count differs from its summary")
    return records


def _run_summary(run: CompletedRun, rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    matched = sum(row["within_threshold"] for row in rows.values())
    return {
        "release": run.summary["config"]["release"],
        "run": str(run.path),
        "source_run_sha256": run.summary_sha256,
        "raw_manifest_sha256": run.summary.get("raw_manifest_sha256"),
        "completed_at_utc": run.summary.get("completed_at_utc"),
        "mapped_buildings": len(rows),
        "candidate_parks": run.summary["candidate_parks"],
        "matched_buildings": matched,
        "share_within": matched / len(rows),
    }


def compare_runs(
    first: CompletedRun, second: CompletedRun, *, distance_tolerance_m: float = 1e-4
) -> dict[str, Any]:
    """Compare all records and a shared-ID cohort under the identical analysis definition."""
    if not math.isfinite(distance_tolerance_m) or distance_tolerance_m < 0:
        raise ValueError("distance_tolerance_m must be finite and nonnegative")
    if first.summary["config"]["release"] == second.summary["config"]["release"]:
        raise ValueError("Choose two distinct source releases")
    definition_hash = analysis_sha256(first)
    if definition_hash != analysis_sha256(second):
        raise ValueError(
            "Release analysis definitions differ; comparisons would mix study definitions"
        )
    a, b = _records(first), _records(second)
    common = sorted(a.keys() & b.keys())
    cohort = {
        "buildings": len(common),
        "within_both": 0,
        "outside_both": 0,
        "gained_match": 0,
        "lost_match": 0,
        "distance_changes_over_tolerance": 0,
        "nearest_park_id_changes": 0,
        "grid_id_changes": 0,
    }
    distances, changed = [], []
    for key in common:
        left, right = a[key], b[key]
        changes = []
        if left["within_threshold"] and right["within_threshold"]:
            cohort["within_both"] += 1
            delta = abs(right["distance_m"] - left["distance_m"])
            distances.append(delta)
            if delta > distance_tolerance_m:
                cohort["distance_changes_over_tolerance"] += 1
                changes.append("distance")
            if left["park_id"] != right["park_id"]:
                cohort["nearest_park_id_changes"] += 1
                changes.append("park_id")
        elif not left["within_threshold"] and not right["within_threshold"]:
            cohort["outside_both"] += 1
        else:
            direction = "gained_match" if right["within_threshold"] else "lost_match"
            cohort[direction] += 1
            changes.append(direction)
        if left["grid_id"] != right["grid_id"]:
            cohort["grid_id_changes"] += 1
            changes.append("grid_id")
        if changes:
            changed.append(
                {
                    "building_id": key,
                    "changes": changes,
                    "first": {
                        name: left[name]
                        for name in ("park_id", "distance_m", "within_threshold", "grid_id")
                    },
                    "second": {
                        name: right[name]
                        for name in ("park_id", "distance_m", "within_threshold", "grid_id")
                    },
                }
            )
    return {
        "contract_version": 1,
        "compared_fields": ["park_id", "distance_m", "within_threshold", "grid_id"],
        "analysis_sha256": definition_hash,
        "analysis_definition": analysis_definition(first),
        "distance_tolerance_m": distance_tolerance_m,
        "first": _run_summary(first, a),
        "second": _run_summary(second, b),
        "only_in_first": sorted(a.keys() - b.keys()),
        "only_in_second": sorted(b.keys() - a.keys()),
        "common_cohort": {
            **cohort,
            "changed_result_records": len(changed),
            "unchanged_within_tolerance": len(common) - len(changed),
            "max_absolute_distance_change_m": max(distances) if distances else None,
        },
        "changed_common_records": changed,
        "interpretation": (
            "Differences in mapped records and straight-line proximity; "
            "ID appearance/disappearance "
            "does not establish real-world construction/demolition or walking access."
        ),
    }
