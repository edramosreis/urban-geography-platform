"""Offline real-Parquet comparison tests, with no Spark session or network required."""

import json
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from urban_geography.ingest import digest
from urban_geography.lakehouse import load_completed_run
from urban_geography.release_comparison import compare_runs


def rewrite(run, specifications):
    facts = run.path / "building_proximity" / "part-00000.parquet"
    original = pq.read_table(facts)
    template = original.to_pylist()[0]
    rows = []
    for key, park, distance, grid in specifications:
        rows.append(
            {
                **template,
                "building_id": key,
                "park_id": park,
                "distance_m": distance,
                "within_threshold": park is not None,
                "grid_id": grid,
            }
        )
    pq.write_table(pa.Table.from_pylist(rows, schema=original.schema), facts)
    matched = sum(row["within_threshold"] for row in rows)
    grid = run.path / "grid_proximity" / "part-00000.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"mapped_buildings": len(rows), "buildings_within": matched}]), grid
    )
    summary = json.loads((run.path / "summary.json").read_text())
    summary.update(
        mapped_buildings=len(rows),
        buildings_within_500m=matched,
        share_within=matched / len(rows),
        candidate_parks=len({row["park_id"] for row in rows if row["park_id"] is not None}),
    )
    summary["files"] = {
        p.relative_to(run.path).as_posix(): digest(p) for p in run.path.rglob("*.parquet")
    }
    (run.path / "summary.json").write_text(json.dumps(summary))
    return load_completed_run(run.path)


def input_hashes(*runs):
    return {str(p): digest(p) for run in runs for p in run.path.rglob("*") if p.is_file()}


def test_comparison_separates_id_churn_status_distance_and_grid_changes(history_run_factory):
    first = rewrite(
        history_run_factory(),
        [
            ("kept", "p0", 50.0, "g0"),
            ("lost", "p1", 100.0, "g0"),
            ("gained", None, None, "g0"),
            ("outside", None, None, "g0"),
            ("changed", "p0", 10.0, "g0"),
            ("removed", "p2", 25.0, "g0"),
        ],
    )
    second = rewrite(
        history_run_factory("2026-10-21.0"),
        [
            ("kept", "p0", 50.0 + 1e-7, "g0"),
            ("lost", None, None, "g0"),
            ("gained", "p3", 20.0, "g0"),
            ("outside", None, None, "g1"),
            ("changed", "p9", 60.0, "g0"),
            ("added", "p4", 5.0, "g0"),
        ],
    )
    before = input_hashes(first, second)
    report = compare_runs(first, second)
    assert report["first"]["mapped_buildings"] == report["second"]["mapped_buildings"] == 6
    assert report["first"]["matched_buildings"] == report["second"]["matched_buildings"] == 4
    assert report["only_in_first"] == ["removed"] and report["only_in_second"] == ["added"]
    cohort = report["common_cohort"]
    assert cohort["buildings"] == 5
    assert cohort["within_both"] == 2 and cohort["outside_both"] == 1
    assert cohort["gained_match"] == cohort["lost_match"] == 1
    assert cohort["distance_changes_over_tolerance"] == 1
    assert cohort["nearest_park_id_changes"] == 1
    assert cohort["grid_id_changes"] == 1
    assert cohort["max_absolute_distance_change_m"] == 50.0
    assert cohort["changed_result_records"] == 4 and cohort["unchanged_within_tolerance"] == 1
    changed = {row["building_id"]: row for row in report["changed_common_records"]}
    assert set(changed) == {"lost", "gained", "outside", "changed"}
    assert changed["outside"]["changes"] == ["grid_id"]
    assert changed["changed"]["changes"] == ["distance", "park_id"]
    assert changed["lost"]["second"]["distance_m"] is None
    assert report["first"]["source_run_sha256"] == first.summary_sha256
    assert report["second"]["source_run_sha256"] == second.summary_sha256
    assert input_hashes(first, second) == before


def test_disjoint_ids_do_not_fabricate_common_distance_statistics(history_run_factory):
    first = history_run_factory()
    second = rewrite(
        history_run_factory("2026-10-21.0"), [("x", None, None, "g0"), ("y", None, None, "g0")]
    )
    report = compare_runs(first, second)
    assert report["common_cohort"]["buildings"] == 0
    assert report["common_cohort"]["max_absolute_distance_change_m"] is None
    assert report["changed_common_records"] == []
    assert len(report["only_in_first"]) == len(report["only_in_second"]) == 2


def test_comparison_refuses_different_analysis_or_the_same_release(history_run_factory):
    first = history_run_factory()
    with pytest.raises(ValueError, match="two distinct source releases"):
        compare_runs(first, history_run_factory(revision=1))
    changed = history_run_factory("2026-10-21.0", bbox=(-8.434, 40.195, -8.415, 40.215))
    with pytest.raises(ValueError, match="analysis definitions differ"):
        compare_runs(first, changed)


@pytest.mark.parametrize("defect", ["duplicate", "bad_null", "wrong_release"])
def test_checksum_valid_bad_facts_are_rejected(history_run_factory, defect):
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    path = second.path / "building_proximity" / "part-00000.parquet"
    original = pq.read_table(path)
    rows = original.to_pylist()
    if defect == "duplicate":
        rows[1]["building_id"] = rows[0]["building_id"]
        message = "duplicate comparison building IDs"
    elif defect == "bad_null":
        rows[1]["distance_m"] = 0.0
        message = "unmatched result must have null"
    else:
        rows[0]["source_release"] = "2026-01-21.0"
        message = "fact metadata differs"
    pq.write_table(pa.Table.from_pylist(rows, schema=original.schema), path)
    summary_path = second.path / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary["files"][path.relative_to(second.path).as_posix()] = digest(path)
    summary_path.write_text(json.dumps(summary))
    with pytest.raises(ValueError, match=message):
        compare_runs(first, load_completed_run(second.path))


@pytest.mark.parametrize("tolerance", [float("nan"), -1.0])
def test_comparison_rejects_invalid_tolerance(history_run_factory, tolerance):
    with pytest.raises(ValueError, match="finite and nonnegative"):
        compare_runs(
            history_run_factory(),
            history_run_factory("2026-10-21.0"),
            distance_tolerance_m=tolerance,
        )


def test_comparison_cli_preserves_inputs_and_existing_report(history_run_factory, tmp_path):
    first, second = history_run_factory(), history_run_factory("2026-10-21.0", revision=1)
    output = tmp_path / "comparison.json"
    command = [
        sys.executable,
        "scripts/compare_releases.py",
        "--first-run",
        str(first.path),
        "--second-run",
        str(second.path),
        "--output",
        str(output),
    ]
    before = input_hashes(first, second)
    result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "complete"
    report = json.loads(output.read_text())
    assert report["common_cohort"]["buildings"] == 1
    assert report["common_cohort"]["changed_result_records"] == 1
    report_hash = digest(output)
    repeated = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    assert repeated.returncode != 0 and "output already exists" in repeated.stderr
    assert digest(output) == report_hash
    assert input_hashes(first, second) == before
