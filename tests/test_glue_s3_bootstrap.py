"""S3 bootstrap ordering and owned-resource cleanup without AWS or a Spark JVM."""

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from s3_input_fixtures import MemoryS3

from urban_geography.glue_input import load_prepared_input, stage_input
from urban_geography.glue_s3_bootstrap import run_s3_writer
from urban_geography.ingest import digest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def case(history_run_factory, tmp_path):
    run = history_run_factory()
    prepared = tmp_path / "prepared"
    stage_input(run.path, run.summary_sha256, prepared)
    client = MemoryS3(prepared, run.summary_sha256)
    client.close = Mock(wraps=client.close)
    factory = Mock(return_value=client)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (scratch / "keep.txt").write_text("keep")
    observed = []

    def write(path):
        assert client.closed and all(body.closed for body in client.bodies)
        observed.append(path)
        plan = load_prepared_input(path, run.summary_sha256)
        return {"status": "created", "rows": plan.run.summary["mapped_buildings"]}

    options = {
        "deployment_name": "urban-geo-test",
        "account_id": "123456789012",
        "expected_summary_sha256": run.summary_sha256,
        "expected_manifest_sha256": digest(prepared / "input-manifest.json"),
        "scratch_root": scratch,
        "client_factory": factory,
        "write": Mock(side_effect=write),
    }
    return SimpleNamespace(client=client, options=options, scratch=scratch, observed=observed)


def execute(case, **changes):
    return run_s3_writer(**(case.options | changes))


def assert_scratch_preserved(case):
    assert [p.name for p in case.scratch.iterdir()] == ["keep.txt"]
    assert (case.scratch / "keep.txt").read_text() == "keep"


def test_bootstrap_closes_one_client_before_writer_and_removes_only_owned_scratch(case):
    result = execute(case)
    assert result["status"] == "complete" and result["publication"]["rows"] == 2
    assert result["transfer"]["get_requests"] == 4
    assert result["sdk_client_closed_before_writer"] and result["scratch_cleaned"]
    case.options["client_factory"].assert_called_once()
    case.client.close.assert_called_once()
    assert len(case.observed) == 1 and not case.observed[0].exists()
    assert_scratch_preserved(case)


@pytest.mark.parametrize("field", ["expected_summary_sha256", "expected_manifest_sha256"])
def test_invalid_pins_never_construct_an_sdk_client_or_start_writer(case, field):
    with pytest.raises(ValueError, match="SHA256"):
        execute(case, **{field: "bad"})
    case.options["client_factory"].assert_not_called()
    case.options["write"].assert_not_called()
    assert_scratch_preserved(case)


@pytest.mark.parametrize("defect", ["checksum", "missing"])
def test_download_failure_closes_client_skips_writer_and_cleans_scratch(case, defect):
    key = case.client.prefix + "input-manifest.json"
    if defect == "checksum":
        case.client.objects[key] = b"changed"
    else:
        del case.client.objects[key]
    with pytest.raises((ValueError, FileNotFoundError)):
        execute(case)
    case.options["write"].assert_not_called()
    case.client.close.assert_called_once()
    assert all(body.closed for body in case.client.bodies)
    assert_scratch_preserved(case)


def test_writer_failure_cleans_scratch_after_client_was_closed(case):
    with pytest.raises(ValueError, match="writer failed"):
        execute(case, write=Mock(side_effect=ValueError("writer failed")))
    case.client.close.assert_called_once()
    assert_scratch_preserved(case)


def test_client_factory_failure_leaves_no_writer_or_scratch(case):
    with pytest.raises(RuntimeError, match="client failed"):
        execute(case, client_factory=Mock(side_effect=RuntimeError("client failed")))
    case.options["write"].assert_not_called()
    case.client.close.assert_not_called()
    assert_scratch_preserved(case)


@pytest.mark.parametrize("kind", ["missing", "data", "source", "git"])
def test_unsafe_scratch_fails_before_client_creation(case, tmp_path, kind):
    path = {
        "missing": tmp_path / "missing",
        "data": ROOT / "data",
        "source": ROOT / "src",
        "git": ROOT / ".git",
    }[kind]
    with pytest.raises(ValueError, match="scratch directory"):
        execute(case, scratch_root=path)
    case.options["client_factory"].assert_not_called()
    case.options["write"].assert_not_called()
    assert_scratch_preserved(case)


def test_managed_entry_point_rejects_bad_pins_without_the_optional_sdk_or_java(case):
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/run_glue_s3_writer.py"),
            "--summary-sha256",
            "bad",
            "--manifest-sha256",
            "bad",
            "--deployment-name",
            "urban-geo-test",
            "--account-id",
            "123456789012",
            "--scratch-root",
            str(case.scratch),
            "--JOB_NAME",
            "synthetic-job",
        ],
        cwd=ROOT,
        env=os.environ | {"JAVA_HOME": "/missing-java"},
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 and "SHA256 pins are required" in result.stderr
    assert "JAVA_GATEWAY_EXITED" not in result.stderr
    assert_scratch_preserved(case)
