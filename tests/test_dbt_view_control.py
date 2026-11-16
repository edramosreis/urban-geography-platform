"""Durable view ownership, failure blocking, deadlines and strict adapter response bounds."""

import copy
import json

import pytest
from dbt_control_fixtures import VIEWS, case, metadata, params


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    return case(tmp_path, monkeypatch)


def test_two_ordered_view_queries_keep_tokens_and_reuse_verified_terminal_metadata(fixture):
    f = fixture
    with f.control.session():
        for index in range(2):
            identity = f.control.start_query_execution(**params(f.control, index))[
                "QueryExecutionId"
            ]
            assert (
                f.control.get_query_execution(QueryExecutionId=identity)["QueryExecution"][
                    "Status"
                ]["State"]
                == "SUCCEEDED"
            )
            assert f.control.get_query_results(QueryExecutionId=identity)["UpdateCount"] == 0
    assert f.client.get_query_execution.call_count == 2
    assert f.client.get_query_results.call_args.kwargs == {
        "QueryExecutionId": "view-2",
        "MaxResults": 1,
    }
    tokens = [r["ClientRequestToken"] for r in f.requests.values()]
    assert len(set(tokens)) == 2 and all(len(t) == 64 for t in tokens)
    assert all(
        r["ResultConfiguration"]["ExpectedBucketOwner"] == "123456789012"
        for r in f.requests.values()
    )
    report = json.loads((f.control.output / "receipt.json").read_text())
    assert report["status"] == "view_queries_completed" and report["queries_reserved"] == 2
    assert not report["result_verified"] and not report["caller_identity_verified"]
    f.client.stop_query_execution.assert_not_called()


@pytest.mark.parametrize(
    "change",
    ["order", "sql", "workgroup", "results", "token", "extra", "oversized", "catalog", "database"],
)
def test_changed_adapter_request_blocks_before_submission(fixture, change):
    f = fixture
    request = copy.deepcopy(params(f.control, 1 if change == "order" else 0))
    if change == "sql":
        request["QueryString"] = request["QueryString"].replace("AS OF 123", "AS OF 456")
    elif change == "oversized":
        request["QueryString"] += " " * 16384
    elif change == "workgroup":
        request["WorkGroup"] = "primary"
    elif change == "results":
        request["ResultConfiguration"]["OutputLocation"] = "s3://elsewhere/"
    elif change == "catalog":
        request["QueryExecutionContext"]["Catalog"] = "other"
    elif change == "database":
        request["QueryExecutionContext"]["Database"] = "other"
    elif change == "token":
        request["ClientRequestToken"] = "replacement"
    elif change == "extra":
        request["ExecutionParameters"] = ["unexpected"]
    with f.control.session():
        with pytest.raises(ValueError, match="fixed view recipe"):
            f.control.start_query_execution(**request)
        with pytest.raises(RuntimeError, match="blocked"):
            f.control.start_query_execution(**params(f.control))
    f.client.start_query_execution.assert_not_called()
    f.client.get_work_group.assert_not_called()


def test_uncertain_response_preserves_original_request_and_blocks_all_retries(fixture):
    f = fixture
    f.client.start_query_execution.side_effect = TimeoutError("synthetic")
    with f.control.session():
        with pytest.raises(VIEWS.SubmissionUnknown):
            f.control.start_query_execution(**params(f.control))
        with pytest.raises(RuntimeError, match="blocked"):
            f.control.start_query_execution(**params(f.control))
    saved = json.loads((f.control.output / "query-01/intent.json").read_text())
    assert saved["request"]["ClientRequestToken"]
    assert not (f.control.output / "query-01/query.json").exists()
    f.client.start_query_execution.assert_called_once()
    f.client.stop_query_execution.assert_not_called()


@pytest.mark.parametrize("confirms", [True, False])
def test_watchdog_cancels_once_and_blocks_following_model(fixture, confirms):
    f = fixture

    def observed(**kw):
        state = "CANCELLED" if confirms and f.client.stop_query_execution.called else "RUNNING"
        return metadata(f.requests[kw["QueryExecutionId"]], kw["QueryExecutionId"], state)

    f.client.get_query_execution.side_effect = observed
    with f.control.session():
        with pytest.raises(RuntimeError, match="not verified"):
            f.control.start_query_execution(**params(f.control))
    assert f.clock.value == (300 if confirms else 360)
    assert f.control.report()["queries"][0]["terminal_confirmed"] is confirms
    assert f.client.get_query_execution.call_count <= 72
    f.client.stop_query_execution.assert_called_once()


def test_second_query_cannot_extend_the_total_run_deadline(fixture):
    f = fixture
    with f.control.session():
        f.control.start_query_execution(**params(f.control))
        f.clock.value = 599
        f.client.get_query_execution.side_effect = lambda **kw: metadata(
            f.requests[kw["QueryExecutionId"]],
            kw["QueryExecutionId"],
            "CANCELLED" if f.client.stop_query_execution.called else "RUNNING",
        )
        with pytest.raises(RuntimeError, match="not verified"):
            f.control.start_query_execution(**params(f.control, 1))
    assert f.clock.value == 600
    f.client.stop_query_execution.assert_called_once_with(QueryExecutionId="view-2")


def test_positive_first_scan_refuses_second_full_query_allowance(fixture):
    f = fixture
    f.client.get_query_execution.side_effect = lambda **kw: metadata(
        f.requests[kw["QueryExecutionId"]], kw["QueryExecutionId"], scanned=1
    )
    with f.control.session():
        f.control.start_query_execution(**params(f.control))
        with pytest.raises(RuntimeError, match="cumulative"):
            f.control.start_query_execution(**params(f.control, 1))
    f.client.start_query_execution.assert_called_once()
    assert f.control.report()["data_scanned_bytes"] == 1


def test_overscan_cancels_and_accounts_for_the_highest_observation(fixture):
    f = fixture

    def observed(**kw):
        return metadata(
            f.requests[kw["QueryExecutionId"]],
            kw["QueryExecutionId"],
            "CANCELLED" if f.client.stop_query_execution.called else "RUNNING",
            scanned=104857601,
        )

    f.client.get_query_execution.side_effect = observed
    with f.control.session():
        with pytest.raises(RuntimeError, match="not verified"):
            f.control.start_query_execution(**params(f.control))
    assert f.control.report()["data_scanned_bytes"] == 104857601
    f.client.stop_query_execution.assert_called_once()


@pytest.mark.parametrize("tamper", ["project", "child_intent"])
def test_disk_changes_during_api_call_block_completion_without_losing_query_ownership(
    fixture, tamper
):
    f = fixture
    original = f.client.start_query_execution.side_effect

    def changed(**request):
        result = original(**request)
        target = (
            f.project / "macros/require_view.sql"
            if tamper == "project"
            else f.control.output / "query-01/intent.json"
        )
        target.write_text("changed")
        return result

    f.client.start_query_execution.side_effect = changed
    with f.control.session():
        with pytest.raises(ValueError):
            f.control.start_query_execution(**params(f.control))
    assert (f.control.output / "query-01/query.json").exists()
    assert f.control.report()["blocked"]
    f.client.get_query_execution.assert_called_once()


@pytest.mark.parametrize("bad", ["NextToken", "Rows", "repeat", "other_id"])
def test_adapter_results_cannot_escape_the_owned_bounded_response(fixture, bad):
    f = fixture
    with f.control.session():
        f.control.start_query_execution(**params(f.control))
        if bad == "NextToken":
            f.client.get_query_results.return_value = {"NextToken": "more"}
        elif bad == "Rows":
            f.client.get_query_results.return_value = {
                "ResultSet": {"Rows": [{"Data": [{"VarCharValue": "unexpected"}]}]}
            }
        elif bad == "repeat":
            f.control.get_query_results(QueryExecutionId="view-1")
        with pytest.raises(ValueError):
            f.control.get_query_results(
                QueryExecutionId="someone-else" if bad == "other_id" else "view-1"
            )
    assert f.client.get_query_results.call_count <= 1
    assert f.control.report()["blocked"]


def test_context_is_required_and_cannot_be_reentered(fixture):
    f = fixture
    with pytest.raises(RuntimeError, match="inactive"):
        f.control.start_query_execution(**params(f.control))
    f.client.start_query_execution.assert_not_called()
    with f.control.session():
        pass
    with pytest.raises(RuntimeError, match="once"):
        with f.control.session():
            pass


@pytest.mark.parametrize(
    "field", ["WorkGroup", "StatementType", "EngineVersion", "ResultConfiguration"]
)
def test_execution_metadata_drift_cancels_owned_query_before_following_model(fixture, field):
    f = fixture

    def observed(**kw):
        response = metadata(
            f.requests[kw["QueryExecutionId"]],
            kw["QueryExecutionId"],
            "CANCELLED" if f.client.stop_query_execution.called else "RUNNING",
        )
        if not f.client.stop_query_execution.called:
            response["QueryExecution"][field] = (
                "wrong" if field in {"WorkGroup", "StatementType"} else {}
            )
        return response

    f.client.get_query_execution.side_effect = observed
    with f.control.session():
        with pytest.raises(RuntimeError, match="execution_contract_unverified"):
            f.control.start_query_execution(**params(f.control))
    f.client.stop_query_execution.assert_called_once()
    assert f.control.report()["blocked"]


def test_third_submission_is_refused_even_after_two_successful_queries(fixture):
    f = fixture
    with f.control.session():
        for index in range(2):
            f.control.start_query_execution(**params(f.control, index))
        with pytest.raises(ValueError, match="ceiling"):
            f.control.start_query_execution(**params(f.control))
    assert f.client.start_query_execution.call_count == 2


def test_interrupt_after_known_identity_keeps_owned_cleanup(fixture):
    f = fixture

    def observed(**kw):
        if not f.client.stop_query_execution.called:
            raise KeyboardInterrupt()
        return metadata(f.requests[kw["QueryExecutionId"]], kw["QueryExecutionId"], "CANCELLED")

    f.client.get_query_execution.side_effect = observed
    with f.control.session():
        with pytest.raises(KeyboardInterrupt):
            f.control.start_query_execution(**params(f.control))
    f.client.stop_query_execution.assert_called_once()
    assert (f.control.output / "query-01/query.json").exists()
    assert "stop_observation" in (f.control.output / "query-01/events.jsonl").read_text()
    assert f.control.report()["blocked"]


def test_external_source_gate_runs_before_each_submission_and_blocks_without_a_token(
    tmp_path, monkeypatch
):
    calls = []

    def source_gate():
        calls.append(len(f.requests))
        if len(calls) == 2:
            raise ValueError("Source catalog changed")

    f = case(tmp_path, monkeypatch, before_submit=source_gate)
    with f.control.session():
        f.control.start_query_execution(**params(f.control))
        with pytest.raises(ValueError, match="Source catalog changed"):
            f.control.start_query_execution(**params(f.control, 1))
    assert calls == [0, 1]
    f.client.start_query_execution.assert_called_once()
    assert not (f.control.output / "query-02").exists()
    f.client.stop_query_execution.assert_not_called()


def test_project_and_clock_are_rechecked_after_the_external_gate(tmp_path, monkeypatch):
    def delayed_gate():
        f.clock.value = 601

    f = case(tmp_path, monkeypatch, before_submit=delayed_gate)
    with f.control.session():
        with pytest.raises(RuntimeError, match="deadline"):
            f.control.start_query_execution(**params(f.control))
    f.client.start_query_execution.assert_not_called()
    assert not (f.control.output / "query-01").exists()


def test_profile_catalog_and_outer_whitespace_are_rendered_before_durable_submission(fixture):
    f = fixture
    incoming = copy.deepcopy(params(f.control))
    incoming["QueryExecutionContext"]["Catalog"] = "AwsDataCatalog"
    incoming["QueryString"] = "\n" + incoming["QueryString"] + "\n"
    with f.control.session():
        f.control.start_query_execution(**incoming)
    sent = f.requests["view-1"]
    assert sent["QueryExecutionContext"] == {
        "Catalog": "awsdatacatalog",
        "Database": "urban_geo_test_m4",
    }
    assert sent["QueryString"] == incoming["QueryString"].strip()
    saved = json.loads((f.control.output / "query-01/intent.json").read_text())
    assert saved["request"] == sent
    assert (
        saved["plan"]["query_sha256"]
        == __import__("hashlib").sha256(sent["QueryString"].encode()).hexdigest()
    )
    assert f.control.report()["queries"][0]["outcome"] == "query_succeeded"
    f.client.stop_query_execution.assert_not_called()
