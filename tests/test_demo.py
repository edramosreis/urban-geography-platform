"""Guarded offline demonstration and real raw-to-Gold integration on tiny geometry inputs."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from processing_fixtures import make_processing_case

import urban_geography.demo as demo
from urban_geography.ingest import digest


@pytest.fixture
def demo_inputs(tmp_path):
    raw_root = tmp_path / "raw"
    first = make_processing_case(tmp_path / "first", raw_root=raw_root)
    second = make_processing_case(tmp_path / "second", release="2026-10-21.0", raw_root=raw_root)
    return first, second, tmp_path / "demo"


def test_demo_preflight_sorts_releases_without_creating_outputs(demo_inputs):
    first, second, output = demo_inputs
    plan = demo.prepare_demo(
        [second.config_path, first.config_path], raw_root=first.raw_root, output=output
    )
    assert [c.release for c in plan.configs] == ["2026-09-23.1", "2026-10-21.0"]
    assert plan.input_hashes and plan.boundary is None
    assert not output.exists()


@pytest.mark.parametrize(
    "defect", ["existing", "duplicate", "incompatible", "corrupt_raw", "missing_raw", "overlap"]
)
def test_demo_preflight_rejects_unsafe_or_unusable_inputs(demo_inputs, defect):
    first, second, output = demo_inputs
    configs = [first.config_path, second.config_path]
    if defect == "existing":
        output.mkdir()
        (output / "keep.txt").write_text("preserve")
        message = "output exists"
    elif defect == "duplicate":
        configs = [first.config_path, first.config_path]
        message = "distinct retained source releases"
    elif defect == "incompatible":
        config = json.loads(second.config_path.read_text())
        config["threshold_m"] = 400
        second.config_path.write_text(json.dumps(config))
        message = "comparable analyses"
    elif defect == "corrupt_raw":
        (second.raw / "building.parquet").write_bytes(b"changed")
        message = "checksum mismatch"
    elif defect == "missing_raw":
        (second.raw / "manifest.json").unlink()
        message = "manifest.json"
    else:
        output = first.raw_root / "new-demo"
        message = "disjoint"
    with pytest.raises((ValueError, FileNotFoundError), match=message):
        demo.prepare_demo(configs, raw_root=first.raw_root, output=output)
    if defect == "existing":
        assert (output / "keep.txt").read_text() == "preserve"
    else:
        assert not output.exists()


def test_retained_row_budget_is_checked_from_parquet_metadata(demo_inputs):
    first, second, output = demo_inputs
    path = first.raw / "building.parquet"
    template = pq.read_table(path).to_pylist()[0]
    rows = [{**template, "id": f"building-{i}"} for i in range(20001)]
    pq.write_table(pa.Table.from_pylist(rows), path)
    manifest_path = first.raw / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["extracts"]["building"].update(rows=len(rows), sha256=digest(path))
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="row counts differ or exceed"):
        demo.prepare_demo(
            [first.config_path, second.config_path], raw_root=first.raw_root, output=output
        )
    assert not output.exists()


def test_failed_independent_verifier_cannot_publish_acceptance(demo_inputs, monkeypatch):
    first, second, output = demo_inputs
    plan = demo.prepare_demo(
        [first.config_path, second.config_path], raw_root=first.raw_root, output=output
    )
    monkeypatch.setattr(demo, "process", lambda *args, **kwargs: None)

    def verifier_failed(*args, **kwargs):
        assert kwargs["env"]["PYTHONOPTIMIZE"] == "0"
        assert kwargs["env"]["PROJ_NETWORK"] == "OFF"
        return SimpleNamespace(returncode=1, stderr="injected verifier failure", stdout="")

    monkeypatch.setattr(demo.subprocess, "run", verifier_failed)
    with pytest.raises(RuntimeError, match="Independent verifier failed"):
        demo.run_demo(plan)
    assert output.exists() and not (output / "acceptance.json").exists()
    assert not (output / "warehouse").exists()


@pytest.mark.integration
def test_offline_raw_to_saved_gold_demo_and_output_preservation(demo_inputs):
    first, second, output = demo_inputs
    originals = {
        str(p): digest(p)
        for root in (first.raw_root, first.config_path.parent, second.config_path.parent)
        for p in root.rglob("*")
        if p.is_file()
    }
    command = [
        sys.executable,
        "scripts/run_local_demo.py",
        "--config",
        str(first.config_path),
        "--config",
        str(second.config_path),
        "--raw-root",
        str(first.raw_root),
        "--output",
        str(output),
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=420,
        check=False,
        env={**os.environ, "PROJ_NETWORK": "OFF"},
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "complete"
    report_path = output / "acceptance.json"
    report = json.loads(report_path.read_text())
    assert report["status"] == "complete" and report["gold"]["grid_rows"] == 4
    assert [r["mapped_buildings"] for r in report["release_summaries"]] == [2, 2]
    assert [r["buildings_within"] for r in report["release_summaries"]] == [1, 1]
    assert all(
        v["replay_logically_identical"] and v["rows_checked"] == 2
        for v in report["independent_verification"]
    )
    assert report["schema_evolution"]["schema_id"] == 1
    assert report["schema_evolution"]["snapshot_unchanged"]
    assert report["old_snapshot_readback_matches"] and report["schema_evolution_payloads_unchanged"]
    assert report["processing_time_values_verified"] and report["fresh_session_readback"]
    assert report["replay_statuses"] == ["reused"] * 5
    assert report["replay_files_unchanged"] and report["retained_inputs_unchanged"]
    assert all(
        r["summary_sha256"] != r["reprocessed_summary_sha256"] for r in report["processing_runs"]
    )
    assert all(digest(Path(path)) == expected for path, expected in originals.items())
    report_hash = digest(report_path)
    retried = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    assert retried.returncode != 0 and "output exists" in retried.stderr
    assert digest(report_path) == report_hash
