"""Private Terraform inputs are derived from pinned releases without AWS or Spark."""

import json
import runpy
from pathlib import Path

import pytest

from urban_geography.glue_delivery import delivery_plan

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prepare_glue_iam.py"
PREPARE = runpy.run_path(str(SCRIPT))["prepare"]


def test_export_matches_the_verified_delivery_inventory(frozen):
    output = frozen.path.parent / "private" / "release.tfvars.json"
    result = PREPARE(frozen.path, frozen.pin, output)
    assert json.loads(output.read_text()) == result
    assert result["deployment_name"] == "urban-geo-test"
    assert result["aws_account_id"] == "123456789012" and result["aws_region"] == "eu-west-1"
    descriptor = result["glue_release"]
    assert descriptor["manifest_sha256"] == frozen.pin
    assert descriptor["release_id"] == frozen.manifest["release_id"]
    assert descriptor["summary_sha256"] == frozen.options["expected_summary_sha256"]
    assert descriptor["object_keys"] == [
        item["key"] for item in delivery_plan(frozen.path, frozen.pin)["objects"]
    ]
    assert all(not key.startswith("warehouse/") for key in descriptor["object_keys"])


@pytest.mark.parametrize("defect", ["pin", "extra_file", "changed_payload"])
def test_invalid_release_cannot_publish_iam_inputs(frozen, defect):
    pin = frozen.pin
    if defect == "pin":
        pin = "0" * 64
    elif defect == "extra_file":
        (frozen.path / "private-note.txt").write_text("private")
    else:
        (frozen.path / "software/run_glue_s3_writer.py").write_text("changed")
    output = frozen.path.parent / "private" / "release.tfvars.json"
    with pytest.raises(ValueError):
        PREPARE(frozen.path, pin, output)
    assert not output.exists() and not output.parent.exists()


@pytest.mark.parametrize("kind", ["exists", "extension", "in_release", "data", "source", "git"])
def test_export_refuses_overwrite_or_unsafe_locations(frozen, monkeypatch, kind):
    root = frozen.path.parent / "project"
    monkeypatch.setitem(PREPARE.__globals__, "PROJECT_ROOT", root)
    output = {
        "exists": frozen.path.parent / "exists.tfvars.json",
        "extension": frozen.path.parent / "public.json",
        "in_release": frozen.path / "private.tfvars.json",
        "data": root / "data/private.tfvars.json",
        "source": root / "src/private.tfvars.json",
        "git": root / ".git/private.tfvars.json",
    }[kind]
    if kind == "exists":
        output.write_text("keep")
    with pytest.raises(ValueError):
        PREPARE(frozen.path, frozen.pin, output)
    if kind == "exists":
        assert output.read_text() == "keep"
    else:
        assert not output.exists()


@pytest.mark.parametrize("line_ending", ["lf", "crlf"])
def test_job_export_preserves_the_exact_pinned_manifest_bytes(frozen, line_ending):
    import hashlib

    content = (frozen.path / "release.json").read_bytes()
    if line_ending == "crlf":
        content = json.dumps(frozen.manifest, indent=2).replace("\n", "\r\n").encode()
        (frozen.path / "release.json").write_bytes(content)
    pin = hashlib.sha256(content).hexdigest()
    output = frozen.path.parent / "job.tfvars.json"
    result = PREPARE(frozen.path, pin, output, include_job=True)
    assert result["glue_job_release_json"].encode() == content
    assert result["glue_release"]["manifest_sha256"] == pin
    assert json.loads(output.read_text())["glue_job_release_json"].encode() == content


def test_iam_only_export_leaves_job_omitted(frozen):
    result = PREPARE(frozen.path, frozen.pin, frozen.path.parent / "iam.tfvars.json")
    assert "glue_job_release_json" not in result


def test_synthetic_job_fixture_matches_the_current_frozen_recipe():
    from urban_geography.glue_release import render_release

    fixture = SCRIPT.parents[1] / "tests/fixtures/glue_job_release.json"
    manifest = json.loads(fixture.read_text())
    assert render_release(manifest["identity"]) == manifest
    assert manifest["identity"]["files"]["input/summary.json"]["bytes"] == 1


@pytest.mark.parametrize("drift", ["changed", "oversized"])
def test_job_export_rechecks_the_manifest_after_inventory_validation(frozen, monkeypatch, drift):
    original = PREPARE.__globals__["delivery_plan"]

    def mutate_after_validation(*args):
        result = original(*args)
        content = b"changed" if drift == "changed" else b"x" * (1024 * 1024 + 1)
        (frozen.path / "release.json").write_bytes(content)
        return result

    monkeypatch.setitem(PREPARE.__globals__, "delivery_plan", mutate_after_validation)
    output = frozen.path.parent / "not-created" / "job.tfvars.json"
    with pytest.raises(ValueError, match="changed before job export"):
        PREPARE(frozen.path, frozen.pin, output, include_job=True)
    assert not output.exists() and not output.parent.exists()
