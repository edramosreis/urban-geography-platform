"""Scoped builder ownership, source gates, controlled DDL and conservative failure behavior."""

import hashlib
import importlib.util
import json
import sys
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt_runner_fixtures import ROOT, RUNNER, runner_case

for name in (
    "aws_login_bridge",
    "aws_role_session",
    "athena_reader_session",
    "dbt_builder_session",
    "dbt_rpc",
    "dbt_metadata",
    "dbt_adapter_binding",
    "dbt_model_worker",
    "dbt_model_executor",
    "dbt_view_capture",
    "dbt_builder_operation",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
operation = sys.modules["dbt_builder_operation"]
bridge = sys.modules["aws_login_bridge"]
PRINCIPAL = "arn:aws:iam::123456789012:role/managed/project/login"
CALLER = {
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/urban-geo-test-dbt-view-builder/dbt-builder-"
    + "a" * 32,
    "UserId": "AROA" + "B" * 17 + ":dbt-builder-" + "a" * 32,
}


@pytest.fixture
def case(tmp_path, monkeypatch):
    base = runner_case(tmp_path, monkeypatch)
    monkeypatch.setattr(operation, "ARTIFACTS", tmp_path)
    plans = tmp_path / "plans"
    plans.mkdir()
    path = plans / "views.json"
    plan = RUNNER.view_build_plan(base.project, **base.options["target"])
    path.write_bytes(RUNNER.canonical(plan))
    pin = hashlib.sha256(path.read_bytes()).hexdigest()
    base.clients["sts"].get_caller_identity.return_value = dict(CALLER)
    snapshot = bridge.TemporaryCredentials(
        "ASIA" + "A" * 16,
        "s" * 40,
        "dummyBuilderToken",
        datetime.now(UTC) + timedelta(seconds=3500),
        "123456789012",
    )
    environment = {
        "AWS_ACCESS_KEY_ID": snapshot.access_key,
        "AWS_SECRET_ACCESS_KEY": snapshot.secret_key,
        "AWS_SESSION_TOKEN": snapshot.token,
    }
    events = []

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
        assert kwargs == {"snapshot": snapshot}
        events.append("clients_opened")
        try:
            yield SimpleNamespace(clients=base.clients, reader_environment=environment)
        finally:
            try:
                assert snapshot.token == "dummyBuilderToken"
                with ExitStack() as cleanup:
                    for client in base.clients.values():
                        cleanup.callback(client.close)
            finally:
                environment.clear()
                events.append("clients_closed")

    acquisition = Mock(side_effect=acquire)
    owner = Mock(side_effect=resources)
    factory = Mock(return_value=base.reader)
    executor_factory = Mock(return_value=base.executor)
    jars, tooling = Mock(), Mock()
    for name, value in {
        "builder_credentials": acquisition,
        "query_resources": owner,
        "ReaderProcess": factory,
        "ModelExecutor": executor_factory,
        "verified_jars": jars,
        "tooling_ready": tooling,
    }.items():
        monkeypatch.setattr(operation, name, value)
    return SimpleNamespace(
        base=base,
        project=base.project,
        path=path,
        pin=pin,
        plan=plan,
        output=base.output,
        snapshot=snapshot,
        environment=environment,
        events=events,
        acquisition=acquisition,
        owner=owner,
        factory=factory,
        executor_factory=executor_factory,
        jars=jars,
        tooling=tooling,
    )


def run(case, **overrides):
    options = {
        key: case.base.options[key]
        for key in ("input_path", "writer_receipt", "writer_receipt_sha256", "now", "pause")
    }
    options["output"] = case.output
    return operation.run_operation(
        case.project,
        case.path,
        case.pin,
        PRINCIPAL,
        b"synthetic-source-envelope",
        **(options | overrides),
    )


def assert_closed(case):
    assert case.environment == {} and case.snapshot.token == ""
    assert case.snapshot.access_key == case.snapshot.secret_key == ""
    for client in case.base.clients.values():
        client.close.assert_called_once()


def test_one_snapshot_runs_both_controlled_views_and_completes_only_after_cleanup(case, capsys):
    report = run(case)
    assert report["status"] == "source_checked_view_build"
    assert report["models_succeeded"] and report["source_readback_verified"]
    assert not report["data_tests_passed"] and not report["view_catalog_verified"]
    assert not report["result_verified"] and not report["writer_job_identity_verified"]
    assert len(case.base.requests) == 2
    assert case.events == ["assumed", "clients_opened", "clients_closed", "keys_cleared"]
    case.acquisition.assert_called_once_with(
        case.project, case.path, case.pin, PRINCIPAL, b"synthetic-source-envelope"
    )
    guarded = case.executor_factory.call_args.args[0]
    assert all(isinstance(client, operation.LifetimeClient) for client in guarded.values())
    assert case.executor_factory.call_args.kwargs == {"max_seconds": 720}
    completed = json.loads((case.output / "builder-operation.json").read_text())
    assert completed["view_plan_sha256"] == case.pin
    assert completed["caller"] == CALLER
    assert completed["run_intent_sha256"] == report["run_intent_sha256"]
    assert (
        completed["receipt_sha256"]
        == hashlib.sha256((case.output / "receipt.json").read_bytes()).hexdigest()
    )
    contents = b"".join(p.read_bytes() for p in case.output.rglob("*") if p.is_file())
    assert b"dummyBuilderToken" not in contents and (b"s" * 40) not in contents
    assert_closed(case)
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "defect",
    [
        "plan",
        "project",
        "receipt",
        "existing",
        "outside",
        "input",
        "receipts",
        "plans",
        "project_output",
        "release",
    ],
)
def test_bad_local_inputs_or_output_never_acquire_credentials(case, tmp_path, defect):
    options = {}
    if defect == "plan":
        case.pin = "0" * 64
    elif defect == "project":
        (case.project / "models/grid_proximity.sql").write_text("SELECT 1")
    elif defect == "receipt":
        options["writer_receipt_sha256"] = "0" * 64
    elif defect == "existing":
        case.output.mkdir()
    elif defect == "release":
        release = case.base.options["input_path"].parent
        (release / "release.json").write_text("{}")
        options["output"] = release / "new-output"
    else:
        options["output"] = {
            "outside": tmp_path.parent / "outside-operation",
            "input": case.base.options["input_path"] / "operation",
            "receipts": case.base.options["writer_receipt"].parent / "operation",
            "plans": case.path.parent / "operation",
            "project_output": case.project / "operation",
        }[defect]
    with pytest.raises(ValueError):
        run(case, **options)
    case.acquisition.assert_not_called()
    case.owner.assert_not_called()


@pytest.mark.parametrize("name", ["jars", "tooling"])
def test_stale_local_runtime_never_acquires_credentials(case, name):
    getattr(case, name).side_effect = ValueError("stale local runtime")
    with pytest.raises(ValueError, match="stale local runtime"):
        run(case)
    case.acquisition.assert_not_called()
    assert not case.output.exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("Account", "999999999999"),
        ("Arn", CALLER["Arn"].replace("-dbt-view-builder/", "-athena-reader/")),
        ("Arn", CALLER["Arn"].replace("/dbt-builder-", "/unapproved-")),
        ("UserId", "unapproved"),
    ],
)
def test_wrong_query_client_identity_never_starts_reader_or_dbt(case, field, value):
    case.base.clients["sts"].get_caller_identity.return_value[field] = value
    with pytest.raises(bridge.BridgeError, match="builder_query_identity_differs"):
        run(case)
    case.factory.assert_not_called()
    case.executor_factory.assert_not_called()
    assert not case.output.exists()
    assert_closed(case)


def test_short_snapshot_never_opens_query_clients(case):
    case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=1799)
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        run(case)
    case.owner.assert_not_called()
    assert case.snapshot.token == "" and not case.output.exists()


def test_plan_drift_during_acquisition_never_opens_query_clients(case):
    original = case.acquisition.side_effect

    @contextmanager
    def change(*args):
        with original(*args) as snapshot:
            case.path.write_bytes(b"changed")
            yield snapshot

    case.acquisition.side_effect = change
    with pytest.raises(ValueError, match="checksum"):
        run(case)
    case.owner.assert_not_called()
    assert case.snapshot.token == "" and not case.output.exists()


def test_client_startup_failure_still_clears_the_builder_snapshot(case):
    case.owner.side_effect = RuntimeError("client construction failed")
    with pytest.raises(RuntimeError, match="construction"):
        run(case)
    assert case.snapshot.token == "" and case.events == ["assumed", "keys_cleared"]


@pytest.mark.parametrize("phase", ["reader", "models"])
def test_insufficient_child_budget_stops_before_launching_that_child(case, phase):
    if phase == "reader":
        original = case.base.clients["glue"].get_table.return_value

        def shrink(**kwargs):
            case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=1079)
            return original

        case.base.clients["glue"].get_table.side_effect = shrink
    else:
        original_reader = case.base.reader.return_value

        def shrink(*args, **kwargs):
            case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=899)
            return original_reader

        case.base.reader.side_effect = shrink
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        run(case)
    if phase == "reader":
        case.base.reader.assert_not_called()
    case.base.executor.assert_not_called()
    assert not case.base.requests and not (case.output / "builder-operation.json").exists()
    assert_closed(case)


@pytest.mark.parametrize("phase", ["reader", "models"])
def test_child_failure_preserves_failure_evidence_without_outer_completion(case, phase):
    getattr(case.base, "reader" if phase == "reader" else "executor").side_effect = RuntimeError(
        "synthetic child failure"
    )
    with pytest.raises(RuntimeError, match="child failure"):
        run(case)
    failure = json.loads((case.output / "failure.json").read_text())
    assert failure["stage"] == ("source_preflight" if phase == "reader" else "view_execution")
    assert not (case.output / "builder-operation.json").exists()
    assert_closed(case)


@pytest.mark.parametrize("kind", ["timeout", "lifetime"])
def test_unknown_submission_is_preserved_without_a_replacement(case, kind):
    from athena_control import SubmissionUnknown

    client = case.base.clients["athena"]
    if kind == "timeout":
        client.start_query_execution.side_effect = TimeoutError("synthetic lost response")
    else:
        group = client.get_work_group.return_value

        def shrink(**kwargs):
            case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=539)
            return group

        client.get_work_group.side_effect = shrink
    with pytest.raises(SubmissionUnknown):
        run(case)
    assert client.start_query_execution.call_count == (1 if kind == "timeout" else 0)
    assert (case.output / "queries/query-01/submission.json").is_file()
    assert not (case.output / "queries/query-02").exists()
    assert not (case.output / "builder-operation.json").exists()
    assert_closed(case)


def test_client_close_failure_keeps_runner_receipt_without_outer_completion(case):
    case.base.clients["glue"].close.side_effect = RuntimeError("synthetic close failure")
    with pytest.raises(RuntimeError, match="close failure"):
        run(case)
    assert (case.output / "receipt.json").exists()
    assert not (case.output / "builder-operation.json").exists()
    assert_closed(case)


def test_changed_receipt_during_cleanup_cannot_be_published_as_outer_completion(case):
    case.base.clients["glue"].close.side_effect = lambda: (case.output / "receipt.json").write_text(
        "{}"
    )
    with pytest.raises(ValueError, match="checksum|Saved view receipt differs"):
        run(case)
    assert not (case.output / "builder-operation.json").exists()
    assert_closed(case)


def test_prepare_operation_works_without_credentials_sdk_or_worker_runtime(case):
    options = {
        key: case.base.options[key]
        for key in ("input_path", "writer_receipt", "writer_receipt_sha256")
    }
    plan, identity = operation.prepare_operation(
        case.project, case.path, case.pin, PRINCIPAL, output=case.output, **options
    )
    assert plan == case.plan and identity["view_plan_sha256"] == case.pin
    assert identity["role_name"] == "urban-geo-test-dbt-view-builder"
    case.acquisition.assert_not_called()
    case.jars.assert_not_called()
    case.tooling.assert_not_called()
    assert not case.output.exists()


def test_cancellation_can_use_the_shorter_credential_reserve(case):
    from dbt_control_fixtures import metadata

    client = case.base.clients["athena"]
    observations = 0

    def observe(**kwargs):
        nonlocal observations
        observations += 1
        if observations == 1:
            case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=75)
            raise RuntimeError("synthetic observation failure")
        identity = kwargs["QueryExecutionId"]
        return metadata(case.base.requests[identity], identity, state="CANCELLED")

    client.get_query_execution.side_effect = observe
    with pytest.raises(RuntimeError, match="not verified"):
        run(case)
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="view-1")
    saved = json.loads((case.output / "queries/query-01/receipt.json").read_text())
    assert saved["terminal_confirmed"] and saved["stop_acknowledged"]
    assert len(case.base.requests) == 1
    assert not (case.output / "builder-operation.json").exists()
    assert_closed(case)


def test_expiry_before_ddl_result_read_preserves_terminal_query_without_completion(case):
    from dbt_control_fixtures import metadata

    client = case.base.clients["athena"]

    def observe(**kwargs):
        case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=75)
        identity = kwargs["QueryExecutionId"]
        return metadata(case.base.requests[identity], identity)

    client.get_query_execution.side_effect = observe
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        run(case)
    client.get_query_results.assert_not_called()
    client.stop_query_execution.assert_not_called()
    assert (case.output / "queries/query-01/receipt.json").exists()
    assert len(case.base.requests) == 1
    assert not (case.output / "builder-operation.json").exists()
    assert_closed(case)
