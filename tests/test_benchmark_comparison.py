"""Seeded comparison scheduling and complete/censored real Spark experiments."""

import json
import statistics
import subprocess
import sys

import pytest
from processing_fixtures import make_processing_case

from urban_geography.benchmark import trial_schedule
from urban_geography.ingest import digest


@pytest.mark.parametrize("comparison", [False, True])
def test_schedule_has_separate_warmups_and_interleaved_reproducible_rounds(comparison):
    schedule = trial_schedule(5, comparison, 1729)
    variants = {"current_proximity", "envelope_prefilter"} if comparison else {"current_proximity"}
    assert schedule == trial_schedule(5, comparison, 1729)
    assert len(schedule) == len(variants) * 6
    assert len({s["group"] for s in schedule}) == len(schedule)
    assert all(s["warmup"] for s in schedule[: len(variants)])
    assert not any(s["warmup"] for s in schedule[len(variants) :])
    for index in range(6):
        current = schedule[index * len(variants) : (index + 1) * len(variants)]
        assert {s["variant"] for s in current} == variants
        assert all(s["round"] == index for s in current)


@pytest.mark.integration
def test_cli_comparison_preserves_results_and_suppresses_partial_timing_summaries(tmp_path):
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
    )
    assert processed.returncode == 0, processed.stderr
    originals = {p: digest(p) for p in case.config_path.parent.rglob("*") if p.is_file()}
    output = tmp_path / "comparison"
    command = [
        sys.executable,
        "scripts/benchmark_proximity.py",
        "--config",
        str(case.config_path),
        "--reference-run",
        str(case.output),
        "--raw-root",
        str(case.raw_root),
        "--compare-envelope",
        "--order-seed",
        "1729",
    ]
    result = subprocess.run(
        [*command, "--output", str(output)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "complete" and report["inputs_unchanged"]
    assert report["variant"] == "current_vs_envelope" and report["trial_order_seed"] == 1729
    samples = report["trials"]
    assert len(samples) == 12 and sum(s["warmup"] for s in samples) == 2
    assert [s["group"] for s in samples] == [s["group"] for s in report["scheduled_trials"]]
    assert len({s["equivalence"]["output_sha256"] for s in samples}) == 1
    for sample in samples:
        assert sample["status"] == "complete"
        assert sample["equivalence"]["rows"] == 2 and sample["equivalence"]["matched"] == 1
        assert (output / (sample["group"] + ".plan.txt")).exists()
        metrics = report["task_metrics"][sample["group"]]
        assert metrics["task_metrics_available"] and metrics["task_attempts"] > 0
        assert metrics["failed_task_attempts"] == 0
    for variant, timing in report["variant_collect_seconds"].items():
        measured = [
            s["collect_wall_seconds"]
            for s in samples
            if not s["warmup"] and s["variant"] == variant
        ]
        assert len(measured) == 5 and timing["median"] == statistics.median(measured)
    assert (
        report["measured_collect_seconds"] == report["variant_collect_seconds"]["current_proximity"]
    )
    assert report["candidate_counts"] == {"all_pairs_upper_bound": 2, "envelope_candidate_rows": 1}
    assert report["candidate_diagnostic_seconds"] > 0
    assert report["evaluated_pair_count"] is None
    assert report["task_metrics"]["benchmark.diagnostics"]["task_metrics_available"]
    assert all(digest(p) == value for p, value in originals.items())

    failed_output = tmp_path / "censored"
    failed = subprocess.run(
        [*command, "--output", str(failed_output), "--trial-timeout", "0.001"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert failed.returncode != 0
    censored = json.loads((failed_output / "report.json").read_text())
    assert censored["status"] == "failed" and censored["inputs_unchanged"]
    assert len(censored["trials"]) == 1 and censored["trials"][0]["status"] == "censored"
    assert censored["measured_collect_seconds"] is None
    assert censored["variant_collect_seconds"] == {
        "current_proximity": None,
        "envelope_prefilter": None,
    }
    assert censored["candidate_counts"] is None
    group = censored["trials"][0]["group"]
    assert (failed_output / (group + ".json")).exists()
    assert (failed_output / (group + ".plan.txt")).exists()
