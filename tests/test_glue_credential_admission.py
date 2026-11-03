"""Whole-operation credential admission without starting a real Glue job."""

import json
import runpy
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from aws_login_bridge import BridgeError, TemporaryCredentials, envelope, receive, temporary
from glue_control_fixtures import CONTROL as control
from glue_control_fixtures import ROOT, make_control_case

OWNER = "123456789012"
KEY = "ASIA" + "A" * 16
SECRET = "s" * 40
TOKEN = "dummySessionToken"


def packet(seconds=2400, owner=OWNER):
    value = {
        "Version": 1,
        "AccessKeyId": KEY,
        "SecretAccessKey": SECRET,
        "SessionToken": TOKEN,
        "Expiration": (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(),
        "AccountId": owner,
    }
    return envelope(temporary(value), owner)


def test_horizon_includes_observation_stop_and_reserve_without_extending_job(frozen):
    policy = control.credential_admission()
    assert policy == {
        "watch_seconds": 900,
        "stop_seconds": 120,
        "reserve_seconds": 180,
        "required_lifetime_seconds": 1200,
    }
    assert (
        policy["required_lifetime_seconds"]
        == control.WATCH_SECONDS + control.STOP_SECONDS + control.CREDENTIAL_RESERVE_SECONDS
    )
    assert control.load_plan(frozen.path, frozen.pin)["start_request"]["Timeout"] == 10


@pytest.mark.parametrize("remaining,accepted", [(1199.999, False), (1200, True), (1200.001, True)])
def test_operation_boundary_is_exact(remaining, accepted):
    now = datetime(2026, 11, 3, tzinfo=UTC)
    credentials = TemporaryCredentials(
        KEY, SECRET, TOKEN, now + timedelta(seconds=remaining), OWNER
    )
    if accepted:
        credentials.require_operation(1200, now)
    else:
        with pytest.raises(BridgeError, match="too_short_for_operation"):
            credentials.require_operation(1200, now)


@pytest.mark.parametrize("seconds", [True, 59, 43201, 1200.0, "1200"])
def test_invalid_or_relaxed_operation_policy_is_rejected(seconds):
    now = datetime(2026, 11, 3, tzinfo=UTC)
    credentials = TemporaryCredentials(KEY, SECRET, TOKEN, now + timedelta(hours=1), OWNER)
    with pytest.raises(BridgeError, match="invalid_operation_lifetime"):
        credentials.require_operation(seconds, now)


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(control, "ROOT", tmp_path)
    namespace = runpy.run_path(str(ROOT / "scripts/run_glue_pilot.py"))
    run = namespace["run_with_credentials"]
    output = tmp_path / "artifacts/trial"
    plan = control.load_plan(frozen.path, frozen.pin)
    creds = []
    opened = []
    clients = {name: SimpleNamespace(close=Mock()) for name in ("glue", "s3", "sts")}
    clients["sts"].get_caller_identity = Mock(
        return_value={
            "Account": OWNER,
            "Arn": f"arn:aws:sts::{OWNER}:assumed-role/project-operator/session",
        }
    )

    def parse(content):
        value, owner = receive(content)
        creds.append(value)
        return value, owner

    @contextmanager
    def owned(credentials, sdk):
        assert credentials is creds[-1] and sdk == "glue"
        opened.append(True)
        try:
            yield clients
        finally:
            for client in clients.values():
                client.close()

    def execute(*args, **kwargs):
        kwargs["before_start"]()
        return {"outcome": "job_succeeded"}

    execute_mock = Mock(side_effect=execute)
    monkeypatch.setitem(run.__globals__, "receive", parse)
    monkeypatch.setitem(run.__globals__, "owned_clients", owned)
    monkeypatch.setitem(run.__globals__, "execute_trial", execute_mock)
    return SimpleNamespace(
        run=run,
        plan=plan,
        frozen=frozen,
        output=output,
        creds=creds,
        opened=opened,
        clients=clients,
        execute=execute_mock,
    )


def run(case, content=None):
    return case.run(
        case.plan, case.frozen.path, case.output, packet() if content is None else content
    )


def test_sufficient_snapshot_is_explicit_rechecked_closed_and_not_journaled(case):
    assert run(case)["outcome"] == "job_succeeded"
    case.clients["sts"].get_caller_identity.assert_called_once_with()
    case.execute.assert_called_once()
    for client in case.clients.values():
        client.close.assert_called_once()
    assert case.creds[0].token == ""
    events = [json.loads(line) for line in (case.output / "events.jsonl").read_text().splitlines()]
    assert [item["event"] for item in events] == [
        "attempt_created",
        "credential_admission",
        "credential_lifetime_rechecked_before_start",
    ]
    content = b"".join(path.read_bytes() for path in case.output.iterdir())
    assert not any(value.encode() in content for value in (KEY, SECRET, TOKEN))


@pytest.mark.parametrize("reason", ["short", "wrong_account", "malformed", "oversized"])
def test_ineligible_record_stops_before_clients_journal_or_submission(case, reason):
    content = {
        "short": lambda: packet(600),
        "wrong_account": lambda: packet(owner="999999999999"),
        "malformed": lambda: b"invalid",
        "oversized": lambda: b"x" * (32768 + 1),
    }[reason]()
    with pytest.raises(BridgeError):
        run(case, content)
    assert case.opened == [] and not case.output.exists()
    case.execute.assert_not_called()
    assert not case.creds or case.creds[0].token == ""


def test_preexisting_output_stops_before_reading_credentials_or_creating_clients(case):
    case.output.mkdir(parents=True)
    with pytest.raises(ValueError, match="fresh"):
        run(case)
    assert not case.creds and not case.opened
    case.execute.assert_not_called()


@pytest.mark.parametrize("mode", ["identity_ttl", "wrong_identity", "sdk_failure"])
def test_identity_phase_failure_closes_clients_without_creating_attempt(case, mode):
    def identity():
        if mode == "identity_ttl":
            case.creds[0].expiration = datetime.now(UTC) + timedelta(seconds=1199)
            return {"Account": OWNER, "Arn": f"arn:aws:sts::{OWNER}:assumed-role/operator/session"}
        if mode == "wrong_identity":
            return {"Account": "999999999999", "Arn": "wrong"}
        raise RuntimeError("controlled SDK failure")

    case.clients["sts"].get_caller_identity.side_effect = identity
    with pytest.raises((BridgeError, RuntimeError)):
        run(case)
    case.execute.assert_not_called()
    assert not case.output.exists() and case.creds[0].token == ""
    for client in case.clients.values():
        client.close.assert_called_once()


def test_preflight_time_cannot_consume_submission_reserve_silently(case):
    def preflight_then_submit(*args, **kwargs):
        case.creds[0].expiration = datetime.now(UTC) + timedelta(seconds=1199)
        kwargs["before_start"]()
        pytest.fail("No submission after rejection")

    case.execute.side_effect = preflight_then_submit
    with pytest.raises(BridgeError, match="too_short_for_operation"):
        run(case)
    events = (case.output / "events.jsonl").read_text()
    assert "credential_admission_denied_before_start" in events and "start_intent" not in events
    assert not (case.output / "receipt.json").exists() and case.creds[0].token == ""
    for client in case.clients.values():
        client.close.assert_called_once()


def test_controller_failure_closes_and_clears_without_success_receipt(case):
    case.execute.side_effect = RuntimeError("controlled failure")
    with pytest.raises(RuntimeError):
        run(case)
    assert not (case.output / "receipt.json").exists() and case.creds[0].token == ""
    for client in case.clients.values():
        client.close.assert_called_once()


def test_control_hook_runs_after_preflight_and_before_start_intent(frozen):
    case = make_control_case(frozen)
    events = []
    case.glue.start_job_run = Mock()

    def denied():
        assert events[-1]["event"] == "preflight_complete"
        raise BridgeError("credential_lifetime_too_short_for_operation")

    with pytest.raises(BridgeError):
        control.execute_trial(case.glue, case.s3, case.plan, events.append, before_start=denied)
    case.glue.get_job.assert_called_once()
    case.glue.start_job_run.assert_not_called()
    assert not any(item["event"] in {"start_intent", "submission_unknown"} for item in events)
    assert case.body.closed


@pytest.mark.parametrize("explicit_stdin", [False, True])
def test_plan_only_does_not_read_stdin_or_open_clients(frozen, monkeypatch, capsys, explicit_stdin):
    namespace = runpy.run_path(str(ROOT / "scripts/run_glue_pilot.py"))
    main = namespace["main"]
    stream = Mock(side_effect=AssertionError("No stdin discovery"))
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=SimpleNamespace(read=stream)))
    command = ["run_glue_pilot.py", "--release", str(frozen.path), "--release-sha256", frozen.pin]
    if explicit_stdin:
        command.append("--credentials-stdin")
    monkeypatch.setattr(sys, "argv", command)
    operation = Mock(side_effect=AssertionError("No run"))
    monkeypatch.setitem(main.__globals__, "run_with_credentials", operation)
    main()
    result = json.loads(capsys.readouterr().out)
    assert (
        result["mode"] == "plan_only"
        and result["credential_admission"]["required_lifetime_seconds"] == 1200
    )
    stream.assert_not_called()
    operation.assert_not_called()
