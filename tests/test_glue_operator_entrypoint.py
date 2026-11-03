"""Join the operator credential context to one controller without AWS or subprocesses."""

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
for name in (
    "aws_login_bridge",
    "aws_role_session",
    "glue_control",
    "aws_login_probe",
    "prepare_glue_iam",
    "glue_operator_session",
    "run_glue_pilot",
    "prepare_dbt_project",
    "writer_readback",
    "glue_writer_receipt",
    "run_glue_operator",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
bridge = sys.modules["aws_login_bridge"]
control = sys.modules["glue_control"]
worker = sys.modules["run_glue_operator"]
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
    monkeypatch.setattr(control, "ROOT", tmp_path)
    monkeypatch.setattr(worker, "collect_writer", Mock(return_value=None))
    output = tmp_path / "artifacts/operation"
    plan = control.load_plan(frozen.path, frozen.pin)
    receipt = {
        "outcome": "job_succeeded",
        "terminal_confirmed": True,
        "job_name": plan["job"]["Name"],
        "run_id": "jr_synthetic",
        "release_id": plan["release_id"],
        "release_sha256": frozen.pin,
    }
    return SimpleNamespace(
        frozen=frozen,
        output=output,
        plan=plan,
        receipt=receipt,
        reply=worker.operation_reply(frozen.pin, receipt),
    )


def test_receiver_connects_acquired_credentials_to_controller_and_clears_them(case, monkeypatch):
    assumed = credentials(2400)
    acquired = Mock()

    @contextmanager
    def owned(*args):
        acquired(*args)
        try:
            yield assumed
        finally:
            assumed.clear()

    def execute(plan, release, output, content):
        restored, owner = bridge.receive(content)
        try:
            assert owner == OWNER and restored.token == TOKEN
            assert plan == case.plan and output == case.output
        finally:
            restored.clear()
        return case.receipt

    run = Mock(side_effect=execute)
    monkeypatch.setattr(worker, "operator_credentials", owned)
    monkeypatch.setattr(worker, "run_with_credentials", run)
    assert (
        worker.run_operation(
            case.frozen.path, case.frozen.pin, PRINCIPAL, case.output, b"private source"
        )
        == case.reply
    )
    assert (
        run.call_count == 1
        and acquired.call_args.args[-1] == b"private source"
        and assumed.token == ""
    )


@pytest.mark.parametrize(
    "mode",
    [
        "plan",
        "success",
        "error",
        "interactive",
        "missing_stdin",
        "existing_output",
        "missing_output",
    ],
)
def test_receiver_cli_bounds_input_and_suppresses_provider_details(case, monkeypatch, capsys, mode):
    stream = Mock()
    stream.read.return_value = b"private source"
    monkeypatch.setattr(
        sys, "stdin", SimpleNamespace(buffer=stream, isatty=lambda: mode == "interactive")
    )
    run = Mock(return_value=case.reply)
    if mode == "error":
        run.side_effect = RuntimeError(KEY + SECRET + TOKEN)
    monkeypatch.setattr(worker, "run_operation", run)
    argv = [
        "receiver",
        "--release",
        str(case.frozen.path),
        "--release-sha256",
        case.frozen.pin,
        "--source-role-arn",
        PRINCIPAL,
        "--output",
        str(case.output),
    ]
    if mode == "existing_output":
        case.output.mkdir(parents=True)
    elif mode == "missing_output":
        index = argv.index("--output")
        del argv[index : index + 2]
    if mode != "plan":
        argv.append("--execute")
    if mode != "missing_stdin":
        argv.append("--credentials-stdin")
    monkeypatch.setattr(sys, "argv", argv)
    if mode in {"interactive", "missing_stdin", "missing_output"}:
        with pytest.raises(SystemExit):
            worker.main()
    else:
        worker.main()
    text = capsys.readouterr()
    assert not any(value in text.out + text.err for value in (KEY, SECRET, TOKEN))
    if mode in {"plan", "interactive", "missing_stdin", "missing_output", "existing_output"}:
        stream.read.assert_not_called()
        run.assert_not_called()
    else:
        stream.read.assert_called_once_with(32769)
        assert json.loads(text.out)["status"] == (
            "operation_failed" if mode == "error" else "controller_completed"
        )


def test_existing_receiver_attempt_is_preserved_before_acquisition(case, monkeypatch):
    case.output.mkdir(parents=True)
    record = case.output / "receipt.json"
    record.write_text("existing evidence")
    acquire = Mock(side_effect=AssertionError("must not acquire"))
    run = Mock(side_effect=AssertionError("must not run"))
    monkeypatch.setattr(worker, "operator_credentials", acquire)
    monkeypatch.setattr(worker, "run_with_credentials", run)
    with pytest.raises(ValueError):
        worker.run_operation(case.frozen.path, case.frozen.pin, PRINCIPAL, case.output, b"unused")
    acquire.assert_not_called()
    run.assert_not_called()
    assert record.read_text() == "existing evidence"


def test_controller_error_unwinds_operator_context_without_changing_error(case, monkeypatch):
    assumed = credentials(2400)

    @contextmanager
    def owned(*args):
        try:
            yield assumed
        finally:
            assumed.clear()

    failure = RuntimeError("controller needs recovery")
    monkeypatch.setattr(worker, "operator_credentials", owned)
    monkeypatch.setattr(worker, "run_with_credentials", Mock(side_effect=failure))
    with pytest.raises(RuntimeError) as error:
        worker.run_operation(case.frozen.path, case.frozen.pin, PRINCIPAL, case.output, b"unused")
    assert error.value is failure and assumed.token == ""


def test_failed_acquisition_cannot_reach_the_controller(case, monkeypatch):
    run = Mock(side_effect=AssertionError("must not run"))
    monkeypatch.setattr(
        worker, "operator_credentials", Mock(side_effect=bridge.BridgeError("denied"))
    )
    monkeypatch.setattr(worker, "run_with_credentials", run)
    with pytest.raises(bridge.BridgeError):
        worker.run_operation(case.frozen.path, case.frozen.pin, PRINCIPAL, case.output, b"unused")
    run.assert_not_called()
    assert not case.output.exists()


@pytest.mark.parametrize(
    "outcome,terminal,called",
    [
        ("job_succeeded", True, True),
        ("job_failed", True, False),
        ("stop_unconfirmed", False, False),
    ],
)
def test_capture_runs_only_inside_the_live_successful_operator_context(
    case, monkeypatch, outcome, terminal, called
):
    assumed = credentials(2400)

    @contextmanager
    def owned(*args):
        try:
            yield assumed
        finally:
            assumed.clear()

    hashes = {"writer_receipt_sha256": "a" * 64, "capture_receipt_sha256": "b" * 64}

    def collect(release, pin, output, snapshot):
        assert snapshot is assumed and snapshot.token == TOKEN
        return hashes

    captured = Mock(side_effect=collect)
    monkeypatch.setattr(worker, "operator_credentials", owned)
    monkeypatch.setattr(
        worker,
        "run_with_credentials",
        Mock(return_value={**case.receipt, "outcome": outcome, "terminal_confirmed": terminal}),
    )
    monkeypatch.setattr(worker, "collect_writer", captured)
    result = worker.run_operation(
        case.frozen.path, case.frozen.pin, PRINCIPAL, case.output, b"unused"
    )
    assert result["writer_receipt_captured"] is called
    assert result["outcome"] == outcome and assumed.token == ""
    assert captured.call_count == int(called)


@pytest.mark.parametrize(
    "mode",
    ["success", "short", "expires_between_pages", "sdk_create", "api", "close", "status_write"],
)
def test_capture_connection_admits_requests_and_closes_client_without_losing_controller_result(
    case, monkeypatch, mode
):
    # Exercise the real connection function, which the ordinary entrypoint fixture isolates.
    from importlib import util

    spec = util.spec_from_file_location(
        "capture_worker_test", ROOT / "scripts/run_glue_operator.py"
    )
    bound = util.module_from_spec(spec)
    spec.loader.exec_module(bound)
    case.output.mkdir(parents=True)
    controller = case.output / "receipt.json"
    controller.write_text(json.dumps(case.receipt))
    original = controller.read_bytes()
    assumed = credentials(89 if mode == "short" else 2400)
    client = SimpleNamespace(filter_log_events=Mock(return_value={"events": []}), close=Mock())
    factory = Mock(return_value=client)
    if mode == "sdk_create":
        factory.side_effect = RuntimeError(SECRET)
    if mode == "api":
        client.filter_log_events.side_effect = RuntimeError(SECRET)
    if mode == "close":
        client.close.side_effect = RuntimeError(SECRET)

    def capture(logs, release, pin, path, controller_pin, output):
        logs.filter_log_events()
        if mode == "expires_between_pages":
            assumed.expiration = datetime.now(UTC) + timedelta(seconds=89)
            logs.filter_log_events()
        output.mkdir()
        (output / "capture.json").write_text("proof")
        return {"writer_receipt_sha256": "a" * 64}

    monkeypatch.setattr(bound, "create_logs_client", factory)
    monkeypatch.setattr(bound, "capture_writer_receipt", capture)
    monkeypatch.setattr(bound, "load_capture", Mock())
    if mode == "status_write":
        original_open = Path.open

        def open_path(path, *args, **kwargs):
            if path.name == "writer-capture-status.json":
                raise OSError("synthetic full disk")
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(Path, "open", open_path)
    result = bound.collect_writer(case.frozen.path, case.frozen.pin, case.output, assumed)
    assert (result is not None) == (mode == "success")
    assert controller.read_bytes() == original
    if mode != "status_write":
        assert SECRET not in (case.output / "writer-capture-status.json").read_text()
    assert client.close.call_count == (0 if mode in {"short", "sdk_create"} else 1)
    assert client.filter_log_events.call_count == (0 if mode in {"short", "sdk_create"} else 1)
    if mode == "short":
        factory.assert_not_called()
    assert assumed.token == TOKEN  # The surrounding operator context owns this snapshot.
    assumed.clear()
