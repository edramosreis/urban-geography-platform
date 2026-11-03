"""Private fact-reader role inputs without SDK discovery, AWS calls or Spark startup."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for name in (
    "aws_login_bridge",
    "aws_role_session",
    "prepare_glue_iam",
    "prepare_athena_reader_role",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
reader = sys.modules["prepare_athena_reader_role"]
OWNER = "123456789012"


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "PROJECT_ROOT", tmp_path)
    return frozen, tmp_path / "artifacts/reader/reader.tfvars.json"


@pytest.mark.parametrize("name", ["login", "managed/project/login"])
def test_reader_inputs_bind_only_the_verified_target_and_exact_source_role(case, name):
    frozen, output = case
    principal = f"arn:aws:iam::{OWNER}:role/{name}"
    before = (frozen.path / "release.json").read_bytes()
    result = reader.prepare(frozen.path, frozen.pin, principal, output)
    assert result == {
        "aws_account_id": OWNER,
        "aws_region": "eu-north-1",
        "deployment_name": "urban-geo-test",
        "enable_athena_reader_policy": True,
        "athena_reader_principal_arn": principal,
    }
    assert json.loads(output.read_text()) == result
    assert (frozen.path / "release.json").read_bytes() == before
    assert not any(
        key in result
        for key in ["enable_dbt_view_builder_policy", "glue_operator_principal_arn", "snapshot_id"]
    )


@pytest.mark.parametrize(
    "principal",
    [
        "arn:aws:iam::999999999999:role/login",
        "arn:aws:iam::123456789012:root",
        "arn:aws:iam::123456789012:user/login",
        "arn:aws:sts::123456789012:assumed-role/login/session",
        "arn:aws:iam::123456789012:role/*",
        "arn:aws:iam::123456789012:role/login*",
        "arn:aws:iam::123456789012:role//login",
        "arn:aws:iam::123456789012:role/urban-geo-test-athena-reader",
        "arn:aws:iam::123456789012:role/" + "x" * 65,
        "arn:aws:iam::123456789012:role/login\n",
    ],
)
def test_bad_trust_does_not_create_an_output(case, principal):
    frozen, output = case
    with pytest.raises(ValueError, match="same-account IAM role"):
        reader.prepare(frozen.path, frozen.pin, principal, output)
    assert not output.parent.exists()


@pytest.mark.parametrize("defect", ["pin", "payload"])
def test_release_drift_does_not_produce_deployment_inputs(case, defect):
    frozen, output = case
    pin = frozen.pin
    if defect == "pin":
        pin = "0" * 64
    else:
        next((frozen.path / "input").rglob("*.parquet")).write_bytes(b"changed")
    with pytest.raises(ValueError):
        reader.prepare(frozen.path, pin, f"arn:aws:iam::{OWNER}:role/login", output)
    assert not output.parent.exists()


@pytest.mark.parametrize("kind", ["exists", "extension", "outside", "release"])
def test_private_export_refuses_overwrite_and_wrong_locations(case, kind):
    frozen, output = case
    if kind == "exists":
        output.parent.mkdir(parents=True)
        output.write_text("preserve")
    elif kind == "extension":
        output = output.with_name("public.json")
    elif kind == "outside":
        output = output.parents[2] / "public.tfvars.json"
    else:
        output = frozen.path / "private.tfvars.json"
    with pytest.raises(ValueError):
        reader.prepare(frozen.path, frozen.pin, f"arn:aws:iam::{OWNER}:role/login", output)
    if kind == "exists":
        assert output.read_text() == "preserve"
    else:
        assert not output.exists()


def test_cli_output_omits_private_account_and_principal(case, monkeypatch, capsys):
    frozen, output = case
    principal = f"arn:aws:iam::{OWNER}:role/login"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare-reader",
            "--release",
            str(frozen.path),
            "--release-sha256",
            frozen.pin,
            "--source-role-arn",
            principal,
            "--output",
            str(output),
        ],
    )
    reader.main()
    text = capsys.readouterr().out
    assert OWNER not in text and principal not in text
    assert json.loads(text) == {
        "status": "prepared_local_reader_role_inputs",
        "region": "eu-north-1",
        "aws_calls": 0,
        "execution_authorized": False,
    }
