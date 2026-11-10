"""Local Docker command construction, private credential handoff and fixed evidence readback."""

import importlib.util
import io
import json
import shutil
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from trial_cleanup_command_fixtures import ROOT, command, connect_infrastructure, make_command_case
from trial_cleanup_fixtures import bridge, make_case, packet

for name in (
    "trial_data_cleanup",
    "trial_cleanup_receipt",
    "run_trial_cleanup",
    "trial_cleanup_host_launcher",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
runner = sys.modules["run_trial_cleanup"]
receipts = sys.modules["trial_cleanup_receipt"]
host = sys.modules["trial_cleanup_host_launcher"]


@pytest.fixture(params=command.PHASES)
def case(frozen, tmp_path, monkeypatch, request, capsys):
    phase = request.param[0] if isinstance(request.param, tuple) else request.param
    case = make_command_case(frozen, tmp_path, monkeypatch, phase)
    if isinstance(request.param, tuple):
        from trial_infrastructure_fixtures import ServiceError

        case.infrastructure.clients["iam"].get_policy.side_effect = ServiceError(
            "AccessDenied", 403
        )
    case.prepared = command.prepare(case.operation, case.output)
    connect_infrastructure(case, monkeypatch)
    auth = tmp_path / "auth"
    auth.mkdir()
    case.auth = make_case(frozen, auth, monkeypatch)
    for name in ("trial_cleanup_operation", "trial_quiescence", "trial_data_cleanup"):
        module = sys.modules[name]
        if hasattr(module, "create_s3_client"):
            monkeypatch.setattr(module, "create_s3_client", lambda snapshot: case.s3)
        monkeypatch.setattr(
            module,
            "create_service_client",
            lambda snapshot, service: case.glue if service == "glue" else case.athena,
        )
    monkeypatch.setattr(
        sys, "stdin", SimpleNamespace(isatty=lambda: False, buffer=io.BytesIO(packet()))
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_trial_cleanup.py",
            *command.arguments(case.operation, case.output),
            "--execute",
            "--credentials-stdin",
            "--request-sha256",
            case.prepared["request_sha256"],
        ],
    )
    runner.main()
    case.reply = receipts.verify_reply(
        capsys.readouterr().out.encode(), case.prepared["request_sha256"], case.operation.phase
    )
    assert case.reply["status"] == "phase_completed"
    case.saved = case.output
    case.output = tmp_path / "artifacts/host-operation"
    case.sdk = tmp_path / "artifacts/sdk"
    case.sdk.mkdir()
    monkeypatch.setattr(bridge, "ROOT", tmp_path)
    case.credentials = bridge.TemporaryCredentials(
        "ASIA" + "C" * 16,
        "s" * 40,
        "dummyHostToken",
        datetime.now(UTC) + timedelta(seconds=900),
        "123456789012",
    )
    case.export = Mock(return_value=case.credentials)
    case.cleanup = Mock()
    monkeypatch.setattr(bridge, "export_login", case.export)
    monkeypatch.setattr(bridge, "cleanup", case.cleanup)
    monkeypatch.setattr(bridge, "local_docker", lambda path: ([path, "--context", "local"], {}))

    def capture(cmd, **kw):
        if "inspect" in cmd:
            return ("sha256:" + "a" * 64).encode()
        if "ls" in cmd:
            return b""
        assert "run" in cmd and bridge.receive(kw["content"])[1] == "123456789012"
        assert "dummyHostToken" not in " ".join(cmd)
        shutil.copytree(case.saved, case.output / "attempt")
        return json.dumps(case.reply).encode()

    case.capture = Mock(side_effect=capture)
    monkeypatch.setattr(bridge, "capture", case.capture)
    return case


def launch(case):
    return host.launch(
        case.operation,
        case.sdk,
        case.output,
        case.prepared["request_sha256"],
        "urban-geo",
        "aws",
        "docker",
    )


def test_command_uses_only_readonly_input_mounts_and_one_private_pipe(case):
    cmd = host.docker_command(
        ["docker", "--context", "local"],
        case.sdk,
        "sha256:" + "a" * 64,
        "urban-geo-login-" + "a" * 16,
        case.operation,
        case.output,
        case.prepared["request_sha256"],
        offline=True,
    )
    assert cmd[cmd.index("--network") + 1] == "none"
    assert cmd[cmd.index("--cpus") + 1] == "2" and cmd[cmd.index("--memory") + 1] == "4g"
    assert "--read-only" in cmd and "--publish" not in cmd and "--env-file" not in cmd
    assert cmd[cmd.index("sha256:" + "a" * 64) + 1] == "/opt/venv/bin/python"
    assert (
        "PYTHONPATH=/tooling/lib/python3.11/site-packages:/workspace/src:/workspace/scripts" in cmd
    )
    assert cmd[-2:] == ["--execute", "--credentials-stdin"]
    mounts = [cmd[i + 1] for i, v in enumerate(cmd) if v == "--mount"]
    writable = [m for m in mounts if not m.endswith(",readonly")]
    assert len(writable) == 1 and "target=/workspace/artifacts/cleanup-operation" in writable[0]
    assert not any(".aws" in m for m in mounts)
    assert any("target=/workspace/analytics/athena,readonly" in m for m in mounts)
    assert not any("target=/workspace/analytics/athena/uv.lock," in m for m in mounts)
    mapped, _ = host._remap(case.operation)
    assert all(
        str(r.path).startswith("/workspace/artifacts/cleanup-inputs/") for r in mapped.records()
    )
    assert (
        mapped.queries == ()
        if case.operation.phase in {"inventory", command.INFRA_PHASE}
        else len(mapped.queries) == 3
    )


def test_success_verifies_saved_records_clears_snapshot_and_cleans_owned_container(case):
    result = launch(case)
    assert result["status"] == "phase_completed" and not result["requires_review"]
    assert result["data_cleanup_observed"] == (case.operation.phase == "delete-data")
    assert not any(case.credentials.session_options().values())
    case.export.assert_called_once_with("aws", "urban-geo", "123456789012")
    case.cleanup.assert_called_once()
    assert "dummyHostToken" not in (case.output / "launch.json").read_text()
    expected_seconds = 900 if case.operation.phase == command.INFRA_PHASE else 2100
    assert (
        json.loads((case.output / "launch.json").read_bytes())["supervisor_seconds"]
        == expected_seconds
    )
    assert (
        next(c for c in case.capture.call_args_list if "run" in c.args[0]).kwargs["seconds"]
        == expected_seconds
    )
    if case.operation.phase == command.INFRA_PHASE:
        assert result["named_infrastructure_absence_observed"]
        case.auth.factory.assert_not_called()


@pytest.mark.parametrize("defect", ["pin", "output", "sdk_overlap", "record"])
def test_invalid_local_preflight_never_exports_or_launches(case, defect):
    if defect == "pin":
        case.prepared["request_sha256"] = "0" * 64
    elif defect == "output":
        case.output.mkdir()
    elif defect == "sdk_overlap":
        case.output = case.sdk / "overlap"
    else:
        case.operation.inventory.path.write_bytes(b"changed")
    with pytest.raises(ValueError):
        launch(case)
    case.export.assert_not_called()
    case.capture.assert_not_called()


@pytest.mark.parametrize("defect", ["timeout", "reply", "saved", "short_login"])
def test_failed_handoff_never_retries_and_always_clears_owned_credentials(case, defect):
    original = case.capture.side_effect

    def capture(cmd, **kw):
        result = original(cmd, **kw)
        if "run" in cmd:
            if defect == "timeout":
                raise TimeoutError("dummyHostToken")
            if defect == "reply":
                return b"dummyHostToken"
            if defect == "saved":
                (case.output / "attempt/session-complete.json").write_text("{}")
        return result

    case.capture.side_effect = capture
    if defect == "short_login":
        case.credentials.expiration = datetime.now(UTC) + timedelta(seconds=119)
    with pytest.raises(bridge.BridgeError, match="reconcile_before_retry"):
        launch(case)
    assert not any(case.credentials.session_options().values())
    assert sum("run" in c.args[0] for c in case.capture.call_args_list) == (
        0 if defect == "short_login" else 1
    )
    assert case.cleanup.call_count == (0 if defect == "short_login" else 1)


def test_cleanup_failure_names_only_owned_container_after_clearing_keys(case):
    case.cleanup.side_effect = bridge.BridgeError(
        "owned_container_cleanup_unconfirmed", "urban-geo-login-" + "a" * 16
    )
    with pytest.raises(bridge.BridgeError, match="owned_container_cleanup_unconfirmed"):
        launch(case)
    assert not any(case.credentials.session_options().values())


def test_plan_cli_does_not_find_executables_or_export_keys(case, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "trial_cleanup_host_launcher.py",
            *command.arguments(case.operation, case.output),
            "--sdk-root",
            str(case.sdk),
        ],
    )
    finder = Mock(side_effect=AssertionError("plan must stay local"))
    monkeypatch.setattr(host.shutil, "which", finder)
    host.main()
    value = json.loads(capsys.readouterr().out)
    assert value["mode"] == "plan_only" and not value["credential_exported"]
    assert not case.output.exists() and not case.export.called and not case.capture.called


@pytest.mark.parametrize("case", [command.INFRA_PHASE], indirect=True)
def test_infrastructure_source_needs_600_seconds_before_container_handoff(case):
    case.credentials.expiration = datetime.now(UTC) + timedelta(seconds=599)
    with pytest.raises(bridge.BridgeError, match="reconcile_before_retry"):
        launch(case)
    assert not any(case.credentials.session_options().values())
    assert not any("run" in c.args[0] for c in case.capture.call_args_list)
    case.cleanup.assert_not_called()


@pytest.mark.parametrize("case", [command.INFRA_PHASE], indirect=True)
def test_infrastructure_target_record_tampering_is_rejected_on_host(case):
    original = case.capture.side_effect

    def tamper(cmd, **kw):
        value = original(cmd, **kw)
        if "run" in cmd:
            (case.output / "attempt/inspection/target-00.json").write_text("{}")
        return value

    case.capture.side_effect = tamper
    with pytest.raises(bridge.BridgeError, match="reconcile_before_retry"):
        launch(case)
    assert not any(case.credentials.session_options().values())
    case.cleanup.assert_called_once()


@pytest.mark.parametrize("case", [(command.INFRA_PHASE, "denied")], indirect=True)
def test_infrastructure_review_required_reply_is_preserved_and_cli_exits_unsuccessfully(
    case, monkeypatch, capsys
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "trial_cleanup_host_launcher.py",
            *command.arguments(case.operation, case.output),
            "--sdk-root",
            str(case.sdk),
            "--request-sha256",
            case.prepared["request_sha256"],
            "--execute",
        ],
    )
    monkeypatch.setattr(host.shutil, "which", lambda name: name)
    with pytest.raises(SystemExit) as stopped:
        host.main()
    assert stopped.value.code == 2
    value = json.loads(capsys.readouterr().out)
    assert value["status"] == "phase_completed" and value["requires_review"]
    assert not value["named_infrastructure_absence_observed"] and not value["data_cleanup_observed"]
    assert value["reconcile_before_retry"] and not any(case.credentials.session_options().values())
    case.cleanup.assert_called_once()
