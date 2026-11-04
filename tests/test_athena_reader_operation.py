"""One scoped operation keeps source, query/result gates and owned cleanup together."""

import importlib.util
import json
import sys
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from athena_control_fixtures import CONTROL, execution
from athena_runner_fixtures import ROOT, trial_case

for name in (
    "aws_login_bridge",
    "aws_role_session",
    "athena_reader_session",
    "athena_reader_operation",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
operation = sys.modules["athena_reader_operation"]
bridge = sys.modules["aws_login_bridge"]
PRINCIPAL = "arn:aws:iam::123456789012:role/managed/project/login"
CALLER = {
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/urban-geo-test-athena-reader/athena-reader-"
    + "a" * 32,
    "UserId": "AROA" + "B" * 17 + ":athena-reader-" + "a" * 32,
}


@pytest.fixture
def case(tmp_path, monkeypatch):
    trial = trial_case(tmp_path, monkeypatch)
    monkeypatch.setattr(operation, "ARTIFACTS", tmp_path)
    plans = tmp_path / "plans"
    plans.mkdir()
    path = plans / "plan.json"
    path.write_bytes(CONTROL.canonical(trial.plan))
    pin = CONTROL.hashlib.sha256(path.read_bytes()).hexdigest()
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    writer = receipts / "writer.json"
    writer.write_bytes(trial.options["writer_receipt"].read_bytes())
    trial.clients["sts"].get_caller_identity.return_value = dict(CALLER)
    snapshot = bridge.TemporaryCredentials(
        "ASIA" + "A" * 16,
        "s" * 40,
        "dummyReaderToken",
        datetime.now(UTC) + timedelta(seconds=3500),
        "123456789012",
    )
    events = []
    environment = {
        "AWS_ACCESS_KEY_ID": snapshot.access_key,
        "AWS_SECRET_ACCESS_KEY": snapshot.secret_key,
        "AWS_SESSION_TOKEN": snapshot.token,
    }

    @contextmanager
    def acquire(*args):
        events.append("assumed")
        try:
            yield snapshot
        finally:
            snapshot.clear()
            events.append("keys_cleared")

    @contextmanager
    def resources(**kwargs):
        assert kwargs == {"snapshot": snapshot, "include_results": True}
        events.append("clients_opened")
        try:
            yield SimpleNamespace(clients=trial.clients, reader_environment=environment)
        finally:
            try:
                assert snapshot.token == "dummyReaderToken"
                with ExitStack() as cleanup:
                    for client in trial.clients.values():
                        cleanup.callback(client.close)
            finally:
                environment.clear()
                events.append("clients_closed")

    acquisition = Mock(side_effect=acquire)
    owner = Mock(side_effect=resources)
    factory = Mock(return_value=trial.reader)
    monkeypatch.setattr(operation, "reader_credentials", acquisition)
    monkeypatch.setattr(operation, "query_resources", owner)
    monkeypatch.setattr(operation, "ReaderProcess", factory)
    jars = Mock()
    monkeypatch.setattr(operation, "verified_jars", jars)
    return SimpleNamespace(
        trial=trial,
        plan=path,
        pin=pin,
        writer=writer,
        output=tmp_path / "operation",
        snapshot=snapshot,
        events=events,
        environment=environment,
        acquisition=acquisition,
        owner=owner,
        factory=factory,
        jars=jars,
    )


def run(case, **overrides):
    options = {
        "input_path": case.trial.options["input_path"],
        "writer_receipt": case.writer,
        "writer_receipt_sha256": case.trial.options["writer_receipt_sha256"],
        "output": case.output,
        "now": case.trial.options["now"],
        "pause": case.trial.options["pause"],
    }
    return operation.run_operation(
        case.plan, case.pin, PRINCIPAL, b"synthetic-source-envelope", **(options | overrides)
    )


def assert_closed(case):
    assert case.snapshot.session_options() == {
        "aws_access_key_id": "",
        "aws_secret_access_key": "",
        "aws_session_token": "",
    }
    assert case.environment == {}
    for client in case.trial.clients.values():
        client.close.assert_called_once()


def test_verified_flow_shares_snapshot_and_keeps_clients_inside_role_ownership(case, capsys):
    report = run(case)
    assert report["status"] == "verified_trial" and report["result_verified"]
    assert report["row"] == case.trial.expected
    assert case.events == ["assumed", "clients_opened", "clients_closed", "keys_cleared"]
    case.acquisition.assert_called_once_with(
        case.plan, case.pin, PRINCIPAL, b"synthetic-source-envelope"
    )
    assert case.factory.call_args.args == (
        case.output / "reader",
        case.environment,
        case.trial.plan["target"],
    )
    assert case.trial.body.closed
    case.trial.clients["athena"].start_query_execution.assert_called_once()
    assert (case.output / "receipt.json").is_file()
    assert_closed(case)
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "defect", ["plan", "receipt", "existing", "outside", "input", "receipts", "plans", "release"]
)
def test_local_rejections_happen_before_credential_acquisition(case, tmp_path, defect):
    options = {}
    if defect == "plan":
        case.pin = "0" * 64
    elif defect == "receipt":
        options["writer_receipt_sha256"] = "0" * 64
    elif defect == "existing":
        case.output.mkdir()
    elif defect == "release":
        release = case.trial.options["input_path"].parent
        (release / "release.json").write_text("{}")
        options["output"] = release / "new-output"
    else:
        options["output"] = {
            "outside": tmp_path.parent / "outside-operation",
            "input": case.trial.options["input_path"] / "operation",
            "receipts": case.writer.parent / "operation",
            "plans": case.plan.parent / "operation",
        }[defect]
    with pytest.raises(ValueError):
        run(case, **options)
    case.acquisition.assert_not_called()
    case.owner.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("Account", "999999999999"),
        ("Arn", CALLER["Arn"].replace("-athena-reader/", "-glue-operator/")),
        ("Arn", CALLER["Arn"].replace("/athena-reader-", "/unapproved-")),
        ("UserId", "unapproved"),
    ],
)
def test_wrong_query_client_identity_closes_everything_without_creating_attempt(case, field, value):
    case.trial.clients["sts"].get_caller_identity.return_value[field] = value
    with pytest.raises(bridge.BridgeError, match="reader_query_identity_differs"):
        run(case)
    assert not case.output.exists()
    case.factory.assert_not_called()
    case.trial.clients["athena"].start_query_execution.assert_not_called()
    assert_closed(case)


def test_short_snapshot_never_opens_query_clients(case):
    case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=1199)
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        run(case)
    case.owner.assert_not_called()
    assert case.snapshot.token == "" and not case.output.exists()


def test_plan_drift_during_acquisition_is_rejected_before_attempt_creation(case):
    original = case.acquisition.side_effect

    @contextmanager
    def change(*args):
        with original(*args) as snapshot:
            case.plan.write_bytes(b"changed")
            yield snapshot

    case.acquisition.side_effect = change
    with pytest.raises(ValueError, match="checksum"):
        run(case)
    assert not case.output.exists()
    case.trial.clients["athena"].start_query_execution.assert_not_called()
    assert_closed(case)


def test_insufficient_readback_budget_never_launches_reader(case):
    table = case.trial.clients["glue"].get_table.return_value

    def shrink(**kwargs):
        case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=719)
        return table

    case.trial.clients["glue"].get_table.side_effect = shrink
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        run(case)
    case.trial.reader.assert_not_called()
    case.trial.clients["athena"].start_query_execution.assert_not_called()
    assert json.loads((case.output / "failure.json").read_text())["stage"] == "source_preflight"
    assert_closed(case)


def test_start_lifetime_rejection_preserves_conservative_submission_recovery(case):
    group = case.trial.clients["athena"].get_work_group.return_value

    def shrink(**kwargs):
        case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=539)
        return group

    case.trial.clients["athena"].get_work_group.side_effect = shrink
    with pytest.raises(CONTROL.SubmissionUnknown):
        run(case)
    case.trial.clients["athena"].start_query_execution.assert_not_called()
    assert (case.output / "submission.json").exists()
    assert json.loads((case.output / "failure.json").read_text())["recovery_required"]
    assert_closed(case)


def test_stop_and_confirmation_can_use_the_shorter_reserve(case):
    def failed_observation(**kwargs):
        case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=75)
        raise RuntimeError("synthetic observation failed")

    terminal = execution(case.trial.attempt, state="CANCELLED")
    observations = 0

    def observe(**kwargs):
        nonlocal observations
        observations += 1
        return failed_observation(**kwargs) if observations == 1 else terminal

    case.trial.clients["athena"].get_query_execution.side_effect = observe
    report = run(case)
    assert report["status"] == "query_not_verified" and report["terminal_confirmed"]
    case.trial.clients["athena"].stop_query_execution.assert_called_once_with(
        QueryExecutionId="query-001"
    )
    case.trial.clients["athena"].start_query_execution.assert_called_once()
    assert_closed(case)


def test_unknown_submission_keeps_one_request_and_cleans_up(case):
    case.trial.clients["athena"].start_query_execution.side_effect = TimeoutError()
    with pytest.raises(CONTROL.SubmissionUnknown):
        run(case)
    case.trial.clients["athena"].start_query_execution.assert_called_once()
    assert (case.output / "submission.json").is_file()
    assert not (case.output / "receipt.json").exists()
    assert_closed(case)


def test_expiry_before_result_read_preserves_successful_execution_without_verification(case):
    started = case.trial.clients["athena"].start_query_execution.return_value

    def shrink(**kwargs):
        case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=75)
        return started

    case.trial.clients["athena"].start_query_execution.side_effect = shrink
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        run(case)
    assert (case.output / "execution.json").exists()
    assert not (case.output / "receipt.json").exists()
    case.trial.clients["s3"].head_object.assert_not_called()
    case.trial.clients["athena"].stop_query_execution.assert_not_called()
    assert_closed(case)


def test_query_client_startup_failure_still_clears_the_scoped_snapshot(case):
    case.owner.side_effect = RuntimeError("client construction failed")
    with pytest.raises(RuntimeError, match="construction"):
        run(case)
    assert case.snapshot.token == "" and case.events == ["assumed", "keys_cleared"]
    assert not case.output.exists()


def test_cached_call_checks_lifetime_when_invoked_and_unlisted_calls_are_unavailable(case):
    raw = Mock()
    guarded = operation.LifetimeClient(raw, case.snapshot, operation.CALL_SECONDS["athena"])
    submit = guarded.start_query_execution
    case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=539)
    with pytest.raises(bridge.BridgeError):
        submit(QueryString="synthetic")
    raw.start_query_execution.assert_not_called()
    with pytest.raises(AttributeError):
        _ = guarded.delete_work_group
    raw.delete_work_group.assert_not_called()
    case.snapshot.clear()


def test_prepare_operation_needs_no_credentials_sdk_or_linux_runtime(case):
    plan, identity = operation.prepare_operation(
        case.plan,
        case.pin,
        PRINCIPAL,
        input_path=case.trial.options["input_path"],
        writer_receipt=case.writer,
        writer_receipt_sha256=case.trial.options["writer_receipt_sha256"],
        output=case.output,
    )
    assert plan == case.trial.plan and identity["query_plan_sha256"] == case.pin
    assert identity["role_name"] == "urban-geo-test-athena-reader"
    case.acquisition.assert_not_called()
    case.owner.assert_not_called()
    case.jars.assert_not_called()
    assert not case.output.exists()
