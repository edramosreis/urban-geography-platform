"""Bounded retained-input staging, refusal and copy-drift checks without a Spark JVM."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import urban_geography.glue_input as inputs
from urban_geography.config import RunConfig
from urban_geography.ingest import digest
from urban_geography.lakehouse import load_completed_run
from urban_geography.workload_limits import COIMBRA_EXTENDED_BBOX

ROOT = Path(__file__).resolve().parents[1]


def rewrite(run, change):
    path = run.path / "summary.json"
    summary = json.loads(path.read_text())
    change(summary)
    path.write_text(json.dumps(summary), encoding="utf-8")
    return digest(path)


def hashes(root):
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


def test_stage_preserves_pinned_payload_and_excludes_unlisted_local_files(
    history_run_factory, tmp_path
):
    run = history_run_factory()
    (run.path / "local-note.txt").write_text("local diagnostic note")
    (run.path / "_SUCCESS").touch()
    before = hashes(run.path)
    output = tmp_path / "staged"
    plan = inputs.inspect_input(run.path, run.summary_sha256)
    result = inputs.stage_input(run.path, run.summary_sha256, output)
    assert result["status"] == "complete"
    assert result["source_run_sha256"] == run.summary_sha256
    assert result["mapped_buildings"] == 2
    assert result["payload_bytes"] == plan.payload_bytes
    assert result["files"] == plan.files
    assert set(hashes(output)) == set(plan.files) | {"input-manifest.json"}
    for name, expected in plan.files.items():
        assert digest(output / name) == expected["sha256"]
        assert (output / name).stat().st_size == expected["bytes"]
    assert json.loads((output / "input-manifest.json").read_text()) == result
    assert load_completed_run(output).summary_sha256 == run.summary_sha256
    assert hashes(run.path) == before


@pytest.mark.parametrize("pin", ["", "short", "A" * 64, "0" * 64])
def test_explicit_identity_is_required_before_outputs(history_run_factory, tmp_path, pin):
    run = history_run_factory()
    output = tmp_path / "staged"
    with pytest.raises(ValueError, match="SHA256|checksum differs"):
        inputs.stage_input(run.path, pin, output)
    assert not output.exists() and not list(tmp_path.glob(".input-*"))


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "extra",
        "corrupt",
        "unsafe",
        "bad_sha",
        "incomplete",
        "rows",
        "grid_rows",
        "too_many_files",
        "payload_bytes",
        "summary_bytes",
        "symlink",
    ],
)
def test_bad_inputs_fail_before_staging(history_run_factory, tmp_path, defect):
    run = history_run_factory()
    pin = run.summary_sha256
    fact = next((run.path / "building_proximity").glob("*.parquet"))
    if defect == "missing":
        fact.unlink()
    elif defect == "extra":
        pq.write_table(pa.table({"x": [1]}), run.path / "extra.parquet")
    elif defect == "corrupt":
        fact.write_bytes(b"changed")
    elif defect == "unsafe":
        pin = rewrite(run, lambda s: s["files"].update({"../escape.parquet": "0" * 64}))
    elif defect == "bad_sha":
        pin = rewrite(
            run, lambda s: s["files"].update({fact.relative_to(run.path).as_posix(): "bad"})
        )
    elif defect == "incomplete":
        pin = rewrite(run, lambda s: s.update(status="failed"))
    elif defect == "rows":
        pin = rewrite(run, lambda s: s.update(mapped_buildings=1))
    elif defect == "grid_rows":
        grid = next((run.path / "grid_proximity").glob("*.parquet"))
        pq.write_table(pa.table({"x": [1] * 20001}), grid)
        pin = rewrite(
            run, lambda s: s["files"].update({grid.relative_to(run.path).as_posix(): digest(grid)})
        )
    elif defect == "too_many_files":
        pin = rewrite(
            run,
            lambda s: s.update(
                files={f"building_proximity/p{i}.parquet": "0" * 64 for i in range(13)}
            ),
        )
    elif defect == "payload_bytes":
        with fact.open("r+b") as stream:
            stream.truncate(inputs.MAX_PAYLOAD_BYTES)
    elif defect == "summary_bytes":
        summary = run.path / "summary.json"
        summary.write_bytes(summary.read_bytes() + b" " * inputs.MAX_SUMMARY_BYTES)
        pin = digest(summary)
    elif defect == "symlink":
        outside = tmp_path / "outside.parquet"
        fact.rename(outside)
        fact.symlink_to(outside)
    output = tmp_path / "staged"
    with pytest.raises(ValueError):
        inputs.stage_input(run.path, pin, output)
    assert not output.exists() and not list(tmp_path.glob(".input-*"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_features", 20001),
        ("max_output_bytes", 16 * 1024**2 + 1),
        ("max_download_bytes", 64 * 1024**2 + 1),
        ("max_source_files", 13),
        ("max_features", True),
        ("max_output_bytes", 100),
        ("mapped_buildings", 20001),
    ],
)
def test_pilot_limits_are_independent_of_untrusted_summary_limits(
    history_run_factory, tmp_path, field, value
):
    run = history_run_factory()

    def change(summary):
        target = summary if field == "mapped_buildings" else summary["config"]
        target[field] = value

    pin = rewrite(run, change)
    output = tmp_path / "staged"
    with pytest.raises(ValueError, match="pilot"):
        inputs.stage_input(run.path, pin, output)
    assert not output.exists()


def test_extended_trial_is_not_implicitly_an_approved_glue_input(history_run_factory, tmp_path):
    run = history_run_factory()
    config = RunConfig(
        "coimbra_extended_performance_trial",
        "2026-10-21.0",
        COIMBRA_EXTENDED_BBOX,
        max_download_bytes=256 * 1024**2,
        max_output_bytes=64 * 1024**2,
        max_features=100000,
        max_source_files=24,
        workload_profile="coimbra_extended_v1",
    )
    pin = rewrite(run, lambda s: s.update(config=config.as_dict()))
    with pytest.raises(ValueError, match="only the pilot profile"):
        inputs.stage_input(run.path, pin, tmp_path / "staged")


@pytest.mark.parametrize("kind", ["existing", "overlap", "data", "source", "git"])
def test_output_refusals_preserve_existing_files(history_run_factory, tmp_path, kind):
    run = history_run_factory()
    output = {
        "existing": tmp_path / "staged",
        "overlap": run.path / "nested",
        "data": ROOT / "data/unapproved-glue-input",
        "source": ROOT / "src/unapproved-glue-input",
        "git": ROOT / ".git/unapproved-glue-input",
    }[kind]
    if kind == "existing":
        output.mkdir()
        (output / "keep.txt").write_text("keep")
    with pytest.raises(ValueError):
        inputs.stage_input(run.path, run.summary_sha256, output)
    if kind == "existing":
        assert (output / "keep.txt").read_text() == "keep"
    else:
        assert not output.exists()


@pytest.mark.parametrize("defect", ["grow", "same_size_change"])
def test_source_drift_during_copy_leaves_only_unpublished_staging(
    history_run_factory, tmp_path, monkeypatch, defect
):
    run = history_run_factory()
    original_inspect = inputs.inspect_input
    fact = next((run.path / "building_proximity").glob("*.parquet"))

    def inspect_then_change(path, pin):
        plan = original_inspect(path, pin)
        if path == run.path:
            content = fact.read_bytes()
            fact.write_bytes(
                content + b"x" if defect == "grow" else bytes([content[0] ^ 1]) + content[1:]
            )
        return plan

    monkeypatch.setattr(inputs, "inspect_input", inspect_then_change)
    output = tmp_path / "staged"
    with pytest.raises(ValueError, match="grew|changed while copying"):
        inputs.stage_input(run.path, run.summary_sha256, output)
    assert not output.exists()
    stages = list(tmp_path.glob(".input-*"))
    assert len(stages) == 1
    assert not (stages[0] / "input-manifest.json").exists()


def test_cli_stages_without_a_java_runtime_and_refuses_reuse(history_run_factory, tmp_path):
    run = history_run_factory()
    output = tmp_path / "staged"
    command = [
        sys.executable,
        str(ROOT / "scripts/prepare_glue_input.py"),
        "--run",
        str(run.path),
        "--summary-sha256",
        run.summary_sha256,
        "--output",
        str(output),
    ]
    result = subprocess.run(
        command,
        cwd=tmp_path,
        env=os.environ | {"JAVA_HOME": "/missing-java"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout)["status"] == "complete"
    before = hashes(output)
    failed = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True)
    assert failed.returncode != 0 and "already exists" in failed.stderr
    assert hashes(output) == before


def test_prepared_reader_matches_the_staged_identity_and_inventory(history_run_factory, tmp_path):
    run = history_run_factory()
    output = tmp_path / "staged"
    receipt = inputs.stage_input(run.path, run.summary_sha256, output)
    prepared = inputs.load_prepared_input(output, run.summary_sha256)
    assert prepared.run.summary_sha256 == run.summary_sha256
    assert prepared.files == receipt["files"]
    assert prepared.payload_bytes == receipt["payload_bytes"]


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "status",
        "version_type",
        "file_size",
        "file_hash",
        "extra",
        "oversize",
        "symlink",
        "unpublished",
    ],
)
def test_prepared_reader_refuses_a_damaged_or_unpublished_receipt(
    history_run_factory, tmp_path, defect
):
    run = history_run_factory()
    output = tmp_path / "staged"
    inputs.stage_input(run.path, run.summary_sha256, output)
    path = output / "input-manifest.json"
    manifest = json.loads(path.read_text())
    if defect == "missing":
        path.unlink()
    elif defect == "status":
        manifest["status"] = "failed"
    elif defect == "version_type":
        manifest["contract_version"] = True
    elif defect in {"file_size", "file_hash"}:
        key = next(name for name in manifest["files"] if name.endswith(".parquet"))
        field = "bytes" if defect == "file_size" else "sha256"
        manifest["files"][key][field] = 1 if field == "bytes" else "0" * 64
    elif defect == "extra":
        manifest["unexpected"] = "field"
    elif defect == "oversize":
        path.write_bytes(b" " * (inputs.MAX_SUMMARY_BYTES + 1))
    elif defect == "symlink":
        original = tmp_path / "manifest.json"
        path.rename(original)
        path.symlink_to(original)
    elif defect == "unpublished":
        other = tmp_path / ".input-unpublished"
        output.rename(other)
        output = other
    if defect not in {"missing", "oversize", "symlink", "unpublished"}:
        path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        inputs.load_prepared_input(output, run.summary_sha256)


def test_prepared_reader_rechecks_the_payload_after_preparation(history_run_factory, tmp_path):
    run = history_run_factory()
    output = tmp_path / "staged"
    inputs.stage_input(run.path, run.summary_sha256, output)
    next((output / "building_proximity").glob("*.parquet")).write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum differs"):
        inputs.load_prepared_input(output, run.summary_sha256)
