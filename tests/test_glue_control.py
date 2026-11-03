"""Read-only saved-job/release checks with no credentials, network or Spark startup."""

import json
import runpy
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from glue_control_fixtures import CONTROL as control
from glue_control_fixtures import ROOT


def test_preflight_checks_the_exact_job_and_closes_the_control_body(control_case):
    case = control_case
    result = control.preflight(case.glue, case.s3, case.plan)
    assert result["release_manifest_version"] == "version-1" and case.body.closed
    case.glue.get_job.assert_called_once_with(JobName="urban-geo-test-pilot-writer")
    case.s3.get_object.assert_called_once_with(
        Bucket=case.plan["bucket"],
        ExpectedBucketOwner="123456789012",
        Key=case.plan["completion_key"],
        Range="bytes=0-1048576",
    )
    assert case.plan["start_request"] == {
        "JobName": "urban-geo-test-pilot-writer",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 10,
        "ExecutionClass": "STANDARD",
        "JobRunQueuingEnabled": False,
        "Arguments": {},
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("Role", "wrong-role"),
        ("Timeout", 480),
        ("NumberOfWorkers", True),
        ("MaxRetries", 1),
        ("JobRunQueuingEnabled", True),
        ("DefaultArguments", {"--extra-py-files": "unreviewed"}),
        ("NonOverridableArguments", {}),
        ("SecurityConfiguration", "unexpected"),
        ("Connections", {"Connections": ["unreviewed-vpc"]}),
    ],
)
def test_job_drift_stops_before_reading_s3(control_case, field, value):
    case = control_case
    case.glue.get_job.return_value["Job"][field] = value
    with pytest.raises(ValueError):
        control.preflight(case.glue, case.s3, case.plan)
    case.s3.get_object.assert_not_called()


def test_wrong_release_pin_rejects_after_bounded_read_and_closes_body(control_case):
    case = control_case
    with pytest.raises(ValueError, match="explicit pin"):
        control.preflight(case.glue, case.s3, case.plan | {"release_sha256": "0" * 64})
    assert case.body.closed


def test_documented_absent_defaults_are_accepted(control_case):
    for field in (
        "JobMode",
        "ExecutionClass",
        "MaxRetries",
        "JobRunQueuingEnabled",
        "DefaultArguments",
    ):
        control_case.glue.get_job.return_value["Job"].pop(field)
    control.preflight(control_case.glue, control_case.s3, control_case.plan)


def test_plan_cli_does_not_construct_sdk_clients(frozen, monkeypatch, capsys):
    monkeypatch.setattr(
        control, "create_glue_client", lambda **kwargs: pytest.fail("No SDK client in plan mode")
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_glue_pilot.py", "--release", str(frozen.path), "--release-sha256", frozen.pin],
    )
    runpy.run_path(str(ROOT / "scripts/run_glue_pilot.py"), run_name="__main__")
    assert json.loads(capsys.readouterr().out)["mode"] == "plan_only"


@pytest.mark.parametrize("package", ["boto3", "botocore"])
def test_control_sdk_version_drift_prevents_client_creation(monkeypatch, package):
    monkeypatch.setattr(
        control, "version", lambda name: "wrong" if name == package else control.SDK_PINS[name]
    )
    monkeypatch.setattr(
        control, "import_module", lambda name: pytest.fail("No optional import after version drift")
    )
    with pytest.raises(ValueError, match=package):
        control.create_glue_client()


def test_glue_client_uses_explicit_tls_endpoint_and_single_attempt_policy(monkeypatch):
    session = SimpleNamespace(client=Mock(return_value="client"))
    config_class = Mock(side_effect=lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(control, "version", lambda name: control.SDK_PINS[name])
    monkeypatch.setattr(
        control,
        "import_module",
        lambda name: SimpleNamespace(Config=config_class)
        if name == "botocore.config"
        else SimpleNamespace(),
    )
    assert control.create_glue_client(session=session) == "client"
    args, options = session.client.call_args
    assert args == ("glue",) and options["region_name"] == "eu-north-1"
    assert options["verify"] and options["use_ssl"]
    config = options["config"]
    assert config.signature_version == "v4" and config.max_pool_connections == 1
    assert config.connect_timeout == 5 and config.read_timeout == 10
    assert config.retries == {"mode": "standard", "total_max_attempts": 1}
    assert (
        config.ignore_configured_endpoint_urls
        and not config.use_fips_endpoint
        and not config.use_dualstack_endpoint
    )


@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_bad_control_region_is_rejected_before_sdk_resolution(monkeypatch, region):
    monkeypatch.setattr(control, "version", lambda name: pytest.fail("No SDK resolution"))
    with pytest.raises(ValueError, match="scoped"):
        control.create_glue_client(region=region)
