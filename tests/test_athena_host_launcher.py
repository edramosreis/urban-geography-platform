"""Local-only Docker handoff and independent saved-evidence checks without AWS."""

import hashlib
import importlib.util
import json
import shutil
import sys
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from athena_host_fixtures import BRIDGE as bridge
from athena_host_fixtures import CONTROL, PRINCIPAL, ROOT, prepared_case

if "athena_host_launcher" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "athena_host_launcher", ROOT / "scripts/athena_host_launcher.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["athena_host_launcher"] = module
    spec.loader.exec_module(module)
host = sys.modules["athena_host_launcher"]


@pytest.fixture
def case(tmp_path, monkeypatch):
    case = prepared_case(tmp_path, monkeypatch)
    monkeypatch.setattr(
        host,
        "JARS",
        [
            (
                "https://example.invalid/" + host.BUNDLE_NAME,
                hashlib.sha256(case.bundle.read_bytes()).hexdigest(),
            )
        ],
    )
    case.credentials = bridge.TemporaryCredentials(
        "ASIA" + "A" * 16,
        "s" * 40,
        "dummyLoginToken",
        datetime.now(UTC) + timedelta(seconds=900),
        "123456789012",
    )
    case.export = Mock(return_value=case.credentials)
    case.cleanup = Mock()
    monkeypatch.setattr(bridge, "export_login", case.export)
    monkeypatch.setattr(bridge, "cleanup", case.cleanup)
    monkeypatch.setattr(bridge, "local_docker", lambda path: ([path, "--context", "local"], {}))

    def capture(command, **kwargs):
        if "inspect" in command:
            return ("sha256:" + "a" * 64).encode()
        if "ls" in command:
            return b""
        assert "run" in command and "dummyLoginToken" not in " ".join(command)
        assert bridge.receive(kwargs["content"])[1] == "123456789012"
        shutil.copytree(case.evidence, case.output / "attempt")
        return CONTROL.canonical(case.reply)

    case.capture = Mock(side_effect=capture)
    monkeypatch.setattr(bridge, "capture", case.capture)
    return case


def paths(case):
    return (
        case.plan,
        case.pin,
        PRINCIPAL,
        case.sdk,
        case.input,
        case.writer,
        case.writer_pin,
        case.bundle,
        case.output,
    )


def launch(case):
    return host.launch(*paths(case), "urban-geo", "aws", "docker")


def test_command_has_fixed_runtime_mounts_resources_and_private_pipe(case):
    command = host.docker_command(
        ["docker", "--context", "local"],
        case.sdk,
        "sha256:" + "a" * 64,
        "urban-geo-login-" + "a" * 16,
        case.plan,
        case.pin,
        PRINCIPAL,
        case.input,
        case.writer,
        case.writer_pin,
        case.bundle,
        case.output,
        offline=True,
    )
    assert command[command.index("--network") + 1] == "none"
    assert (
        command[command.index("--cpus") + 1] == "2"
        and command[command.index("--memory") + 1] == "4g"
    )
    assert "--read-only" in command and "--publish" not in command and "--env-file" not in command
    assert command[command.index("--tmpfs") + 1] == "/tmp:rw,nosuid,nodev,size=536870912"
    assert command[-2:] == ["--execute", "--credentials-stdin"]
    mounts = [command[i + 1] for i, x in enumerate(command) if x == "--mount"]
    assert all(
        "readonly" in m for m in mounts if "target=/workspace/artifacts/reader-operation" not in m
    )
    assert not any(".aws" in m for m in mounts)


def test_success_checks_pinned_evidence_clears_login_snapshot_and_cleans_container(case):
    result = launch(case)
    assert (
        result["status"] == "verified_trial"
        and result["result_verified"]
        and not result["reconcile_before_retry"]
    )
    assert (
        case.credentials.token == case.credentials.access_key == case.credentials.secret_key == ""
    )
    case.cleanup.assert_called_once()
    case.export.assert_called_once_with("aws", "urban-geo", "123456789012")
    stored = json.loads((case.output / "launch.json").read_text())
    assert "dummyLoginToken" not in json.dumps(stored)


@pytest.mark.parametrize("defect", ["pin", "bundle", "output", "sdk_overlap"])
def test_invalid_local_input_stops_before_login_export(case, defect):
    if defect == "pin":
        case.pin = "0" * 64
    elif defect == "bundle":
        case.bundle.write_bytes(b"changed")
    elif defect == "output":
        case.output.mkdir()
    else:
        case.output = case.sdk / "overlap"
    with pytest.raises(ValueError):
        launch(case)
    case.export.assert_not_called()
    case.capture.assert_not_called()


@pytest.mark.parametrize("defect", ["reply", "saved_record", "timeout", "short_login"])
def test_handoff_failure_clears_credentials_and_never_retries(case, defect):
    original = case.capture.side_effect

    def capture(command, **kwargs):
        result = original(command, **kwargs)
        if "run" in command:
            if defect == "reply":
                return b"untrusted non-JSON text"
            if defect == "saved_record":
                (case.output / "attempt/receipt.json").write_text("{}")
            if defect == "timeout":
                raise TimeoutError("dummyLoginToken")
        return result

    case.capture.side_effect = capture
    if defect == "short_login":
        case.credentials.expiration = datetime.now(UTC) + timedelta(seconds=119)
    with pytest.raises(bridge.BridgeError, match="reader_handoff_failed_reconcile_before_retry"):
        launch(case)
    assert case.credentials.token == ""
    assert sum("run" in call.args[0] for call in case.capture.call_args_list) == (
        0 if defect == "short_login" else 1
    )
    assert case.cleanup.call_count == (0 if defect == "short_login" else 1)


def test_container_cleanup_failure_is_reported_after_keys_are_cleared(case):
    case.cleanup.side_effect = bridge.BridgeError(
        "owned_container_cleanup_unconfirmed", "urban-geo-login-" + "a" * 16
    )
    with pytest.raises(bridge.BridgeError, match="owned_container_cleanup_unconfirmed"):
        launch(case)
    assert case.credentials.token == ""


def test_plan_cli_never_exports_credentials_or_starts_a_container(case, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "launcher",
            "--plan",
            str(case.plan),
            "--plan-sha256",
            case.pin,
            "--source-role-arn",
            PRINCIPAL,
            "--sdk-root",
            str(case.sdk),
            "--input",
            str(case.input),
            "--writer-receipt",
            str(case.writer),
            "--writer-receipt-sha256",
            case.writer_pin,
            "--bundle",
            str(case.bundle),
            "--output",
            str(case.output),
        ],
    )
    host.main()
    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "plan_only" and not result["credential_exported"]
    case.export.assert_not_called()
    case.capture.assert_not_called()
