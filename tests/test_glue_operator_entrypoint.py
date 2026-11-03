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
    "glue_control",
    "aws_login_probe",
    "prepare_glue_iam",
    "glue_operator_session",
    "run_glue_pilot",
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
