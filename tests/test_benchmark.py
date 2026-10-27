"""Offline baseline equivalence, cancellation, input guards and real Spark instrumentation."""

import json
import os
import subprocess
import sys
from pathlib import Path
from threading import Event

import pytest
from processing_fixtures import make_processing_case

from urban_geography.benchmark import prepare_baseline, time_action, verify_results
from urban_geography.ingest import digest


def test_equivalence_accepts_only_matching_keys_flags_parks_grids_and_distances():
    expected = {
        "b1": {
            "building_id": "b1",
            "park_id": "park",
            "distance_m": 100.0,
            "within_threshold": True,
            "grid_id": "grid-1",
        },
        "b2": {
            "building_id": "b2",
            "park_id": None,
            "distance_m": None,
            "within_threshold": False,
            "grid_id": "grid-2",
        },
    }
    rows = list(expected.values())
    accepted = verify_results([{**rows[0], "distance_m": 100.0000005}, rows[1]], expected, 500)
    assert accepted["rows"] == 2 and accepted["matched"] == 1
    assert accepted["max_distance_error_m"] <= 1e-6
    assert (
        verify_results(rows[::-1], expected, 500)["output_sha256"]
        == verify_results(rows, expected, 500)["output_sha256"]
    )
    for changed in (
        rows[:1],
        rows + rows[:1],
        [{**rows[0], "park_id": "other"}, rows[1]],
        [{**rows[0], "grid_id": "other"}, rows[1]],
        [{**rows[0], "within_threshold": False}, rows[1]],
        [{**rows[0], "distance_m": 100.001}, rows[1]],
        [{**rows[0], "distance_m": float("nan")}, rows[1]],
        [{**rows[0], "distance_m": None}, rows[1]],
        [rows[0], {**rows[1], "distance_m": 0}],
    ):
        with pytest.raises(ValueError):
            verify_results(changed, expected, 500)


def test_failed_action_is_retained_with_error_and_elapsed_time():
    def fail():
        raise ValueError("injected failure")

    rows, sample = time_action(fail, lambda: None, 1)
    assert rows is None and sample["status"] == "failed"
    assert "injected failure" in sample["error"]
    assert sample["collect_wall_seconds"] >= 0 and not sample["cancellation_still_running"]


def test_expired_action_is_censored_even_if_cancellation_returns_rows():
    cancelled = Event()

    def action():
        assert cancelled.wait(timeout=2)
        return []

    rows, sample = time_action(action, cancelled.set, 0.01)
    assert rows == [] and cancelled.is_set() and sample["status"] == "censored"
    assert sample["cancellation_errors"] == [] and not sample["cancellation_still_running"]


@pytest.mark.parametrize("defect", ["trials-low", "trials-high", "timeout", "existing"])
def test_cli_rejects_bad_options_before_starting_spark(tmp_path, defect):
    output = tmp_path / "attempt"
    options = {
        "trials-low": ["--trials", "4"],
        "trials-high": ["--trials", "11"],
        "timeout": ["--trial-timeout", "0"],
        "existing": [],
    }[defect]
    if defect == "existing":
        output.mkdir()
        (output / "keep.txt").write_text("preserve")
    result = subprocess.run(
        [
            sys.executable,
            "scripts/benchmark_proximity.py",
            "--config",
            "missing.json",
            "--output",
            str(output),
            *options,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode != 0 and "SparkContext" not in result.stderr
    assert (
        "output exists"
        if defect == "existing"
        else ("Trial timeout" if defect == "timeout" else "measured trials")
    ) in result.stderr
    if defect == "existing":
        assert (output / "keep.txt").read_text() == "preserve"
        assert list(output.iterdir()) == [output / "keep.txt"]
    else:
        assert not output.exists()


def test_reference_configuration_must_match_before_loading_raw(history_run_factory, tmp_path):
    reference = history_run_factory()
    output = tmp_path / "baseline"
    with pytest.raises(ValueError, match="Reference configuration differs"):
        prepare_baseline(
            Path("configs/coimbra_pilot_october.json"),
            reference.path,
            raw_root=tmp_path / "no-raw",
            output=output,
        )
    assert not output.exists()


@pytest.mark.integration
def test_offline_baseline_records_equivalent_trials_actual_plans_and_task_metrics(tmp_path):
    case = make_processing_case(tmp_path / "case")
    processed = subprocess.run(
        [
            sys.executable,
            "-m",
            "urban_geography.cli",
            "process",
            "--config",
            str(case.config_path),
            "--raw-root",
            str(case.raw_root),
            "--output",
            str(case.output),
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "PROJ_NETWORK": "OFF"},
    )
    assert processed.returncode == 0, processed.stderr
    originals = {str(p): digest(p) for p in case.config_path.parent.rglob("*") if p.is_file()}
    output = tmp_path / "baseline"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/benchmark_proximity.py",
            "--config",
            str(case.config_path),
            "--reference-run",
            str(case.output),
            "--raw-root",
            str(case.raw_root),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "complete" and report["inputs_unchanged"]
    assert report["buildings"] == 2 and report["candidate_parks"] == 1
    assert report["all_pairs_upper_bound"] == 2 and report["evaluated_pair_count"] is None
    samples = report["trials"]
    assert len(samples) == 6 and sum(s["warmup"] for s in samples) == 1
    assert len({s["equivalence"]["output_sha256"] for s in samples}) == 1
    for sample in samples:
        assert sample["status"] == "complete"
        assert sample["equivalence"]["rows"] == 2 and sample["equivalence"]["matched"] == 1
        assert sample["equivalence"]["max_distance_error_m"] <= 1e-6
        assert sample["collect_wall_seconds"] > 0
        assert "== Physical Plan ==" in (output / (sample["group"] + ".plan.txt")).read_text()
        assert json.loads((output / (sample["group"] + ".json")).read_text()) == sample
    for group in ["baseline.prepare", *(s["group"] for s in samples)]:
        metrics = report["task_metrics"][group]
        assert metrics["task_metrics_available"] and metrics["task_attempts"] > 0
        assert metrics["failed_task_attempts"] == 0 and metrics["killed_task_attempts"] == 0
        assert metrics["executor_run_ms"] >= 0 and metrics["executor_cpu_seconds"] >= 0
        assert metrics["shuffle_read_bytes"] >= 0 and metrics["disk_spill_bytes"] >= 0
    timing = report["measured_collect_seconds"]
    measured = sorted(s["collect_wall_seconds"] for s in samples if not s["warmup"])
    assert timing["min"] == measured[0] and timing["median"] == measured[2]
    assert timing["max"] == measured[-1]
    assert report["session_startup_seconds"] > 0 and report["input_preparation_seconds"] > 0
    assert all(digest(Path(path)) == value for path, value in originals.items())

    # A timeout remains a failed attempt with evidence, even if Spark returns after cancellation.
    censored_output = tmp_path / "censored-baseline"
    command = [
        sys.executable,
        "scripts/benchmark_proximity.py",
        "--config",
        str(case.config_path),
        "--reference-run",
        str(case.output),
        "--raw-root",
        str(case.raw_root),
        "--output",
        str(censored_output),
        "--trial-timeout",
        "0.001",
    ]
    censored = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
    assert censored.returncode != 0
    failed = json.loads((censored_output / "report.json").read_text())
    assert failed["status"] == "failed" and failed["inputs_unchanged"]
    assert failed["measured_collect_seconds"] is None
    assert len(failed["trials"]) == 1 and failed["trials"][0]["status"] == "censored"
    assert (censored_output / "baseline.warmup.json").exists()
    assert (censored_output / "baseline.warmup.plan.txt").exists()
