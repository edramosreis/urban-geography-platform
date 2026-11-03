"""Private Terraform inputs are derived from pinned releases without AWS or Spark."""

import importlib.util
import json
import runpy
import sys
from pathlib import Path

import pytest

from urban_geography.glue_delivery import delivery_plan

for name in ("aws_login_bridge", "aws_role_session"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            name, Path(__file__).resolve().parents[1] / "scripts" / (name + ".py")
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prepare_glue_iam.py"
PREPARE = runpy.run_path(str(SCRIPT))["prepare"]


def test_export_matches_the_verified_delivery_inventory(frozen):
    output = frozen.path.parent / "private" / "release.tfvars.json"
    result = PREPARE(frozen.path, frozen.pin, output)
    assert json.loads(output.read_text()) == result
    assert result["deployment_name"] == "urban-geo-test"
    assert result["aws_account_id"] == "123456789012" and result["aws_region"] == "eu-north-1"
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
    assert "glue_operator_principal_arn" not in result


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


@pytest.mark.parametrize("path", ["login", "managed/project/login"])
def test_operator_export_binds_one_existing_same_account_role(frozen, path):
    principal = f"arn:aws:iam::123456789012:role/{path}"
    output = frozen.path.parent / "operator.tfvars.json"
    result = PREPARE(
        frozen.path, frozen.pin, output, include_job=True, operator_principal_arn=principal
    )
    assert result["glue_operator_principal_arn"] == principal
    assert json.loads(output.read_text()) == result
    assert "glue_job_release_json" in result


def test_operator_requires_the_frozen_job(frozen):
    output = frozen.path.parent / "private/operator.tfvars.json"
    with pytest.raises(ValueError, match="requires --include-job"):
        PREPARE(
            frozen.path,
            frozen.pin,
            output,
            operator_principal_arn="arn:aws:iam::123456789012:role/login",
        )
    assert not output.parent.exists()


@pytest.mark.parametrize(
    "principal",
    [
        "arn:aws:iam::999999999999:role/login",
        "arn:aws:sts::123456789012:assumed-role/login/session",
        "arn:aws:iam::123456789012:root",
        "arn:aws:iam::123456789012:user/login",
        "arn:aws:iam::123456789012:role/*",
        "arn:aws:iam::123456789012:role/login*",
        "arn:aws:iam::123456789012:role//login",
        "arn:aws:iam::123456789012:role/urban-geo-test-glue-operator",
        "arn:aws:iam::123456789012:role/" + "x" * 65,
        "arn:aws:iam::123456789012:role/" + "path/" * 110 + "login",
        " arn:aws:iam::123456789012:role/login",
        "arn:aws:iam::123456789012:role/login\n",
    ],
)
def test_operator_rejects_ambiguous_or_unscoped_trust_before_writing(frozen, principal):
    output = frozen.path.parent / "private/operator.tfvars.json"
    with pytest.raises(ValueError, match="same-account IAM role"):
        PREPARE(frozen.path, frozen.pin, output, include_job=True, operator_principal_arn=principal)
    assert not output.parent.exists()
