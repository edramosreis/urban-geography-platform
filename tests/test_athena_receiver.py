"""Plan-only receiver, bounded stdin and fixed secret-free failure replies."""

import io
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from athena_host_fixtures import PRINCIPAL, RECEIPT, prepared_case
from athena_host_fixtures import RECEIVER as receiver


@pytest.fixture
def case(tmp_path, monkeypatch):
    case = prepared_case(tmp_path, monkeypatch)
    argv = [
        "receiver",
        "--plan",
        str(case.plan),
        "--plan-sha256",
        case.pin,
        "--source-role-arn",
        PRINCIPAL,
        "--input",
        str(case.input),
        "--writer-receipt",
        str(case.writer),
        "--writer-receipt-sha256",
        case.writer_pin,
        "--output",
        str(case.evidence),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(receiver, "prepare_operation", Mock(return_value=(case.case.plan, {})))
    case.run = Mock(return_value=case.result)
    monkeypatch.setattr(receiver, "run_operation", case.run)
    return case


def test_plan_mode_does_not_read_credentials_or_call_runtime(case, monkeypatch, capsys):
    monkeypatch.setattr(
        sys, "stdin", SimpleNamespace(buffer=Mock(side_effect=AssertionError("no read")))
    )
    receiver.main()
    reply = json.loads(capsys.readouterr().out)
    assert reply["mode"] == "plan_only" and not reply["credential_exported"]
    case.run.assert_not_called()


def test_private_pipe_failure_reply_omits_provider_secrets(case, monkeypatch, capsys):
    sys.argv.extend(["--execute", "--credentials-stdin"])
    monkeypatch.setattr(
        sys, "stdin", SimpleNamespace(isatty=lambda: False, buffer=io.BytesIO(b"synthetic packet"))
    )
    case.run.side_effect = RuntimeError("sentinel-secret-do-not-emit")
    receiver.main()
    text = capsys.readouterr().out
    assert "sentinel-secret" not in text
    assert json.loads(text) == RECEIPT.operation_reply(case.pin)
    assert case.run.call_args.args[3] == b"synthetic packet"


def test_execution_requires_private_stdin(case, monkeypatch):
    sys.argv.append("--execute")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(isatty=lambda: True))
    with pytest.raises(SystemExit):
        receiver.main()
    case.run.assert_not_called()
