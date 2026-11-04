"""Bounded owned stopping, uncertain acknowledgements and fresh post-stop observations."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from trial_cleanup_fixtures import PRINCIPAL, bridge, make_case, packet
from trial_quiescence_fixtures import execute, make_controller_case, plans


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    return make_controller_case(frozen, tmp_path, monkeypatch)


def no_deletion(case):
    assert {c[0] for c in case.glue.method_calls} <= set(case.controller.GLUE_CALLS)
    assert {c[0] for c in case.athena.method_calls} <= set(case.controller.ATHENA_CALLS)


def test_owned_stops_are_durable_exact_once_and_confirmed_independently(case):
    for client, name in [
        (case.athena, "update_work_group"),
        (case.glue, "batch_stop_job_run"),
        (case.athena, "stop_query_execution"),
    ]:
        original = getattr(client, name).side_effect

        def change(original=original, **params):
            intents = sorted(case.output.glob("action-*-intent.json"))
            assert intents and json.loads(intents[-1].read_bytes())["request"] == params
            return original(**params)

        getattr(client, name).side_effect = change
    report = execute(case)
    assert report["status"] == "quiescence_observed" and report["quiescence_observed"]
    assert report["owned_terminal_observed"] and report["quiescence_and_catalog_observed"]
    assert not report["deletion_authorized"] and not report["future_job_submissions_fenced"]
    case.athena.update_work_group.assert_called_once_with(
        WorkGroup=case.group["Name"], State="DISABLED"
    )
    case.glue.batch_stop_job_run.assert_called_once_with(
        JobName=case.job["Name"], JobRunIds=["jr_owned"]
    )
    assert [
        c.kwargs["QueryExecutionId"] for c in case.athena.stop_query_execution.call_args_list
    ] == ["query-0", "query-1", "query-2"]
    assert len(report["changes"]) == 5 and report["poll_rounds"] == 1
    assert report["api_calls"] <= 107
    case.glue.close.assert_not_called()
    case.athena.close.assert_not_called()
    no_deletion(case)


def test_terminal_work_and_disabled_workgroup_need_no_changes(case):
    case.group["State"] = "DISABLED"
    case.run["JobRunState"] = "SUCCEEDED"
    for row in case.query_rows.values():
        row["Status"]["State"] = "SUCCEEDED"
    report = execute(case)
    assert report["quiescence_observed"] and report["changes"] == []
    case.glue.batch_stop_job_run.assert_not_called()
    case.athena.stop_query_execution.assert_not_called()
    case.athena.update_work_group.assert_not_called()


@pytest.mark.parametrize("which", ["disable", "job", "query"])
def test_lost_change_response_is_not_retried_but_terminal_observation_can_confirm(case, which):
    method = {
        "disable": case.athena.update_work_group,
        "job": case.glue.batch_stop_job_run,
        "query": case.athena.stop_query_execution,
    }[which]
    original = method.side_effect

    def lost(**kw):
        original(**kw)
        raise RuntimeError("sentinel-private-provider-error")

    method.side_effect = lost
    report = execute(case)
    assert report["quiescence_observed"] and not report["deletion_authorized"]
    assert method.call_count == (3 if which == "query" else 1)
    assert any(change["error_type"] == "RuntimeError" for change in report["changes"])
    assert all("sentinel" not in p.read_text() for p in case.output.rglob("*.json"))


@pytest.mark.parametrize("which", ["job", "query", "both"])
def test_unaccepted_stops_and_active_work_never_count_as_quiet(case, which):
    if which in {"job", "both"}:
        case.glue.batch_stop_job_run.side_effect = RuntimeError("denied")
    if which in {"query", "both"}:
        case.athena.stop_query_execution.side_effect = RuntimeError("denied")
    report = execute(case)
    assert report["requires_review"] and not report["quiescence_observed"]
    assert not report["deletion_authorized"] and report["poll_rounds"] == 8
    assert (
        case.glue.batch_stop_job_run.call_count == 1
        and case.athena.stop_query_execution.call_count == 3
    )
    assert report["api_calls"] <= 107
    if which == "both":
        assert report["api_calls"] == 67
    no_deletion(case)


def test_stop_acknowledgement_without_terminal_state_is_insufficient(case):
    case.glue.batch_stop_job_run.side_effect = lambda **kw: {
        "SuccessfulSubmissions": [{"JobName": case.job["Name"], "JobRunId": "jr_owned"}]
    }
    report = execute(case)
    assert not report["quiescence_observed"] and report["changes"][1]["acknowledged"] is True


def test_shared_deadline_ends_polling_without_automatic_new_requests(case):
    case.glue.batch_stop_job_run.side_effect = RuntimeError("denied")
    case.pause = lambda seconds: case.clock.__setitem__(0, case.clock[0] + 200)
    report = execute(case)
    assert report["poll_rounds"] == 1 and not report["quiescence_observed"]
    case.glue.batch_stop_job_run.assert_called_once()


def test_unbound_active_query_is_never_stopped_or_adopted(case):
    case.query_rows["unbound"] = {
        **case.query_rows["query-0"],
        "QueryExecutionId": "unbound",
        "Status": {"State": "RUNNING"},
    }
    report = execute(case)
    assert report["unbound_queries"] == ["unbound"] and not report["quiescence_observed"]
    assert {
        c.kwargs["QueryExecutionId"] for c in case.athena.stop_query_execution.call_args_list
    } == {"query-0", "query-1", "query-2"}


def test_no_receipts_never_infers_stop_targets_from_inventory(case):
    case.job_receipt, case.evidence = None, ()
    case.pin = plans.plan_sha256(plans.quiescence_plan(case.frozen.path, case.frozen.pin, None, ()))
    report = execute(case)
    assert report["unbound_job_runs"] == ["jr_owned"] and len(report["unbound_queries"]) == 3
    assert report["requires_review"] and not report["quiescence_observed"]
    case.glue.batch_stop_job_run.assert_not_called()
    case.athena.stop_query_execution.assert_not_called()


@pytest.mark.parametrize("defect", ["pin", "existing", "outside", "live_sql", "live_context"])
def test_bad_scope_output_or_live_identity_is_rejected_before_any_change(case, defect):
    if defect == "pin":
        case.pin = "0" * 64
    elif defect == "existing":
        case.output.mkdir(parents=True)
    elif defect == "outside":
        case.output = case.output.parents[1] / "outside"
    elif defect == "live_sql":
        case.query_rows["query-0"]["Query"] = "SELECT unrelated"
    else:
        case.query_rows["query-0"]["QueryExecutionContext"] = {
            "Catalog": "AwsDataCatalog",
            "Database": "other",
        }
    with pytest.raises(ValueError):
        execute(case)
    case.athena.update_work_group.assert_not_called()
    case.glue.batch_stop_job_run.assert_not_called()
    case.athena.stop_query_execution.assert_not_called()


def test_changed_receipt_after_disable_prevents_further_changes(case):
    original = case.athena.update_work_group.side_effect

    def drift(**kw):
        response = original(**kw)
        case.job_receipt.path.write_bytes(b"changed")
        return response

    case.athena.update_work_group.side_effect = drift
    with pytest.raises(ValueError):
        execute(case)
    case.glue.batch_stop_job_run.assert_not_called()
    case.athena.stop_query_execution.assert_not_called()
    assert (case.output / "failure.json").exists() and not (case.output / "receipt.json").exists()


def test_already_stopping_job_is_observed_without_duplicate_stop(case):
    case.run["JobRunState"] = "STOPPING"

    def observe(**kw):
        if case.glue.get_job_run.call_count > 1:
            case.run["JobRunState"] = "STOPPED"
        return {"JobRun": case.run}

    case.glue.get_job_run.side_effect = observe
    report = execute(case)
    assert report["quiescence_observed"]
    case.glue.batch_stop_job_run.assert_not_called()


def test_reenabled_workgroup_in_post_inventory_prevents_quiescence_claim(case):
    def observe(**kw):
        if case.athena.get_work_group.call_count == 3:
            case.group["State"] = "ENABLED"
        return {"WorkGroup": case.group}

    case.athena.get_work_group.side_effect = observe
    report = execute(case)
    assert not report["quiescence_observed"] and report["requires_review"]
    case.athena.update_work_group.assert_called_once()


@pytest.fixture
def connected(case, tmp_path, monkeypatch):
    auth = make_case(case.frozen, tmp_path, monkeypatch)
    case.auth = auth
    case.snapshots = []

    def create(snapshot, service):
        snapshot.require_operation(1800)
        case.snapshots.append(snapshot)
        return case.glue if service == "glue" else case.athena

    case.factory = Mock(side_effect=create)
    monkeypatch.setattr(case.controller, "create_service_client", case.factory)
    return case


def run_connected(case):
    return case.controller.run_quiescence(
        case.frozen.path,
        case.frozen.pin,
        case.auth.path,
        case.auth.pin,
        PRINCIPAL,
        packet(),
        job=case.job_receipt,
        queries=case.evidence,
        expected_plan_sha256=case.pin,
        output=case.output,
        now=case.now,
        pause=case.pause,
    )


def test_scoped_completion_is_written_only_after_four_clients_close_and_owned_keys_clear(
    connected, monkeypatch
):
    case = connected
    original = case.controller.write_once

    def write(path, content):
        assert all(not any(c.session_options().values()) for c in case.auth.credentials)
        for client in [*case.auth.clients, case.glue, case.athena]:
            client.close.assert_called_once_with()
        return original(path, content)

    monkeypatch.setattr(case.controller, "write_once", write)
    result = run_connected(case)
    assert result["quiescence_observed"] and not result["deletion_authorized"]
    assert result["status"] == "scoped_trial_quiescence_complete"
    assert case.factory.call_count == 2


def test_scoped_short_snapshot_never_constructs_service_clients(connected):
    case = connected
    case.auth.raw["Expiration"] = datetime.now(UTC) + timedelta(seconds=1700)
    with pytest.raises(bridge.BridgeError):
        run_connected(case)
    case.factory.assert_not_called()
    assert all(not any(c.session_options().values()) for c in case.auth.credentials)
    assert not case.output.exists()


def test_partial_scoped_client_startup_closes_prior_client_and_clears_keys(connected):
    case = connected
    original = case.factory.side_effect

    def fail(snapshot, service):
        if service == "athena":
            raise RuntimeError("client failed")
        return original(snapshot, service)

    case.factory.side_effect = fail
    with pytest.raises(RuntimeError):
        run_connected(case)
    case.glue.close.assert_called_once_with()
    case.athena.close.assert_not_called()
    assert all(not any(c.session_options().values()) for c in case.auth.credentials)
    assert not (case.output / "session-complete.json").exists()


def test_scoped_client_close_failure_prevents_outer_completion_and_clears_keys(connected):
    case = connected
    case.athena.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError):
        run_connected(case)
    assert all(not any(c.session_options().values()) for c in case.auth.credentials)
    case.glue.close.assert_called_once_with()
    assert (case.output / "attempt/receipt.json").exists() and not (
        case.output / "session-complete.json"
    ).exists()
