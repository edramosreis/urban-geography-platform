"""Fixed controller transport, durable evidence and fail-closed result checks without AWS."""

import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
for name in (
    "aws_login_bridge",
    "glue_control",
    "aws_login_probe",
    "prepare_glue_iam",
    "glue_operator_session",
    "run_glue_pilot",
    "run_glue_operator",
    "glue_host_launcher",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
bridge = sys.modules["aws_login_bridge"]
control = sys.modules["glue_control"]
worker = sys.modules["run_glue_operator"]
host = sys.modules["glue_host_launcher"]
OWNER = "123456789012"
PRINCIPAL = f"arn:aws:iam::{OWNER}:role/managed/project/login"
KEY = "ASIA" + "A" * 16
SECRET = "s" * 40
TOKEN = "dummySessionToken"


def credentials(seconds=900):
    return bridge.temporary(
        {
            "Version": 1,
            "AccessKeyId": KEY,
            "SecretAccessKey": SECRET,
            "SessionToken": TOKEN,
            "Expiration": (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(),
            "AccountId": OWNER,
        }
    )


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, "ROOT", tmp_path)
    monkeypatch.setattr(control, "ROOT", tmp_path)
    sdk = tmp_path / "artifacts/sdk"
    sdk.mkdir(parents=True)
    output = tmp_path / "artifacts/operation"
    source = credentials()
    export = Mock(return_value=source)
    cleanup = Mock()
    monkeypatch.setattr(bridge, "export_login", export)
    monkeypatch.setattr(bridge, "cleanup", cleanup)
    monkeypatch.setattr(bridge, "local_docker", lambda docker: ([docker, "--context", "local"], {}))
    plan = control.load_plan(frozen.path, frozen.pin)
    receipt = {
        "outcome": "job_succeeded",
        "terminal_confirmed": True,
        "job_name": plan["job"]["Name"],
        "run_id": "jr_synthetic",
        "release_id": plan["release_id"],
        "release_sha256": frozen.pin,
    }
    reply = worker.operation_reply(frozen.pin, receipt)

    def capture(command, **kwargs):
        if "inspect" in command:
            return ("sha256:" + "a" * 64).encode()
        if "ls" in command:
            return b""
        assert "run" in command
        attempt = output / "attempt"
        attempt.mkdir()
        (attempt / "receipt.json").write_text(json.dumps(receipt))
        return json.dumps(reply).encode()

    capture_mock = Mock(side_effect=capture)
    monkeypatch.setattr(bridge, "capture", capture_mock)
    return SimpleNamespace(
        frozen=frozen,
        sdk=sdk,
        output=output,
        source=source,
        export=export,
        cleanup=cleanup,
        capture=capture_mock,
        receipt=receipt,
        reply=reply,
        plan=plan,
    )


def launch(case):
    return host.launch(
        case.frozen.path,
        case.frozen.pin,
        PRINCIPAL,
        case.sdk,
        case.output,
        "urban-geo",
        "aws",
        "docker",
    )


def test_success_uses_private_pipe_and_binds_receipt_before_claiming_success(case):
    result = launch(case)
    assert result == {
        "status": "controller_completed",
        "outcome": "job_succeeded",
        "terminal_confirmed": True,
        "controller_receipt_verified": True,
        "output_readback_verified": False,
    }
    call = next(item for item in case.capture.call_args_list if "run" in item.args[0])
    command = call.args[0]
    assert "--context" in command and command[command.index("--pull") + 1] == "never"
    assert KEY.encode() in call.kwargs["content"]
    assert call.kwargs["seconds"] == host.SUPERVISOR_SECONDS == 1800
    assert call.kwargs["maximum"] == 4096
    assert not any(value in str(command) + json.dumps(result) for value in (KEY, SECRET, TOKEN))
    assert case.source.token == ""
    case.export.assert_called_once_with("aws", "urban-geo", OWNER)
    case.cleanup.assert_called_once()
    assert (case.output / "launch.json").is_file()
    assert not any(
        value.encode() in path.read_bytes()
        for path in case.output.rglob("*")
        if path.is_file()
        for value in (KEY, SECRET, TOKEN)
    )


def test_mounts_only_one_fresh_writable_directory_and_fixed_receiver(case):
    command = host.docker_command(
        ["docker"],
        case.sdk,
        "sha256:" + "a" * 64,
        "urban-geo-login-" + "b" * 16,
        case.frozen.path,
        case.frozen.pin,
        PRINCIPAL,
        case.output,
        offline=True,
    )
    mounts = [command[i + 1] for i, value in enumerate(command) if value == "--mount"]
    assert len(mounts) == 6
    writable = [value for value in mounts if not value.endswith(",readonly")]
    assert writable == [
        f"type=bind,source={case.output.resolve()},target=/workspace/artifacts/glue-operation"
    ]
    assert any("target=/workspace/artifacts/frozen-release,readonly" in value for value in mounts)
    assert not any(".aws" in value or "target=/workspace," in value for value in mounts)
    assert "--read-only" in command and command[command.index("--network") + 1] == "none"
    assert (
        command[command.index("--cpus") + 1] == "2"
        and command[command.index("--memory") + 1] == "4g"
    )
    assert "/workspace/scripts/run_glue_operator.py" in command and "--credentials-stdin" in command
    assert "/workspace/scripts/aws_login_probe.py" not in command
    assert not case.output.exists()
    case.export.assert_not_called()


@pytest.mark.parametrize("kind", ["exists", "outside", "release", "sdk"])
def test_bad_output_never_exports_or_launches(case, kind):
    if kind == "exists":
        case.output.mkdir()
    elif kind == "outside":
        case.output = case.output.parent.parent / "data/operation"
    elif kind == "release":
        case.output = case.frozen.path / "child"
    else:
        case.output = case.sdk / "child"
    with pytest.raises(ValueError):
        launch(case)
    case.capture.assert_not_called()
    case.export.assert_not_called()
    case.cleanup.assert_not_called()


@pytest.mark.parametrize(
    "defect",
    [
        "transport",
        "invalid_json",
        "wrong_pin",
        "integer_boolean",
        "boolean_version",
        "unknown_outcome",
        "missing_receipt",
        "wrong_receipt_job",
        "wrong_receipt_release",
        "integer_receipt_boolean",
    ],
)
def test_uncertain_or_unbound_completion_preserves_evidence_and_clears_credentials(case, defect):
    original = case.capture.side_effect

    def capture(command, **kwargs):
        if "run" not in command:
            return original(command, **kwargs)
        if defect == "transport":
            raise RuntimeError(SECRET)
        if defect == "wrong_pin":
            case.reply["release_sha256"] = "0" * 64
        elif defect == "integer_boolean":
            case.reply["terminal_confirmed"] = 1
        elif defect == "boolean_version":
            case.reply["version"] = True
        elif defect == "unknown_outcome":
            case.reply["outcome"] = "everything_ok"
        elif defect == "wrong_receipt_job":
            case.receipt["job_name"] = "another-job"
        elif defect == "wrong_receipt_release":
            case.receipt["release_id"] = "0" * 64
        elif defect == "integer_receipt_boolean":
            case.receipt["terminal_confirmed"] = 1
        content = original(command, **kwargs)
        if defect == "missing_receipt":
            (case.output / "attempt/receipt.json").unlink()
        return b"not json" if defect == "invalid_json" else content

    case.capture.side_effect = capture
    with pytest.raises(bridge.BridgeError, match="reconcile_before_retry") as error:
        launch(case)
    assert SECRET not in str(error.value)
    assert case.source.token == "" and (case.output / "launch.json").is_file()
    case.cleanup.assert_called_once()
    assert sum("run" in item.args[0] for item in case.capture.call_args_list) == 1
    with pytest.raises(ValueError):
        launch(case)
    assert case.export.call_count == 1


def test_preexisting_container_is_not_used_or_removed(case):
    original = case.capture.side_effect
    case.capture.side_effect = (
        lambda command, **kwargs: b"existing" if "ls" in command else original(command, **kwargs)
    )
    with pytest.raises(bridge.BridgeError, match="already_exists"):
        launch(case)
    case.export.assert_not_called()
    case.cleanup.assert_not_called()
    assert not case.output.exists()


def test_short_source_snapshot_never_launches_container(case):
    case.source.expiration = datetime.now(UTC) + timedelta(seconds=119)
    with pytest.raises(bridge.BridgeError):
        launch(case)
    assert case.source.token == ""
    assert not any("run" in item.args[0] for item in case.capture.call_args_list)
    case.cleanup.assert_not_called()


def test_structured_worker_failure_is_not_success(case):
    original = case.capture.side_effect
    case.capture.side_effect = (
        lambda command, **kwargs: json.dumps(worker.operation_reply(case.frozen.pin)).encode()
        if "run" in command
        else original(command, **kwargs)
    )
    result = launch(case)
    assert result["status"] == "operation_failed" and not result["controller_receipt_verified"]
    assert not result["terminal_confirmed"] and case.source.token == ""
    case.cleanup.assert_called_once()


def test_stop_unconfirmed_is_preserved_as_uncertain(case):
    case.reply.update(outcome="stop_unconfirmed", terminal_confirmed=False)
    case.receipt.update(outcome="stop_unconfirmed", terminal_confirmed=False)
    result = launch(case)
    assert result["outcome"] == "stop_unconfirmed" and not result["terminal_confirmed"]
    assert result["controller_receipt_verified"]


def test_cleanup_failure_names_only_the_owned_container(case):
    name = "urban-geo-login-" + "c" * 16
    case.cleanup.side_effect = bridge.BridgeError("owned_container_cleanup_unconfirmed", name)
    with pytest.raises(bridge.BridgeError) as error:
        launch(case)
    assert error.value.args == ("owned_container_cleanup_unconfirmed", name)
    assert case.source.token == "" and (case.output / "attempt/receipt.json").exists()


def test_host_default_cli_plans_without_tools_or_credentials(case, monkeypatch, capsys):
    run = Mock(side_effect=AssertionError("must not execute"))
    monkeypatch.setattr(host, "launch", run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "launcher",
            "--release",
            str(case.frozen.path),
            "--release-sha256",
            case.frozen.pin,
            "--source-role-arn",
            PRINCIPAL,
            "--sdk-root",
            str(case.sdk),
            "--output",
            str(case.output),
        ],
    )
    host.main()
    text = capsys.readouterr().out
    assert json.loads(text)["credential_exported"] is False and OWNER not in text
    run.assert_not_called()
    case.capture.assert_not_called()
    case.export.assert_not_called()
    assert not case.output.exists()


@pytest.mark.parametrize("mode", ["success", "job_failed", "error", "cleanup_error"])
def test_host_cli_owns_exit_status_and_never_echoes_provider_details(
    case, monkeypatch, capsys, mode
):
    result = {
        "status": "controller_completed",
        "outcome": "job_succeeded" if mode == "success" else "job_failed",
        "terminal_confirmed": True,
    }
    run = Mock(return_value=result)
    if mode == "error":
        run.side_effect = RuntimeError(KEY + SECRET + TOKEN)
    elif mode == "cleanup_error":
        run.side_effect = bridge.BridgeError(
            "owned_container_cleanup_unconfirmed", "urban-geo-login-" + "c" * 16
        )
    monkeypatch.setattr(host, "launch", run)
    monkeypatch.setattr(host.shutil, "which", lambda name: "docker")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "launcher",
            "--release",
            str(case.frozen.path),
            "--release-sha256",
            case.frozen.pin,
            "--source-role-arn",
            PRINCIPAL,
            "--sdk-root",
            str(case.sdk),
            "--output",
            str(case.output),
            "--aws-cli",
            "aws",
            "--execute",
        ],
    )
    if mode == "success":
        host.main()
    else:
        with pytest.raises(SystemExit) as error:
            host.main()
        assert error.value.code == 2
    text = capsys.readouterr()
    assert not any(value in text.out + text.err for value in (KEY, SECRET, TOKEN, OWNER))
    report = json.loads(text.out)
    assert (
        report["status"]
        == {
            "success": "controller_completed",
            "job_failed": "controller_completed",
            "error": "handoff_failed",
            "cleanup_error": "owned_container_cleanup_unconfirmed",
        }[mode]
    )
    run.assert_called_once()
