"""Optional release-result acceptance joins tests, view observations and source gates."""

import copy
import hashlib
import json
from unittest.mock import Mock

import pytest
from dbt_catalog_fixtures import fake_view
from dbt_control_fixtures import metadata, params
from dbt_data_test_fixtures import summary
from dbt_gold_fixtures import GOLD, result_rows
from dbt_runner_fixtures import RUNNER, runner_case
from dbt_test_plan import TEST_NAMES
from dbt_view_plan import normalized_sql


@pytest.fixture
def case(tmp_path, monkeypatch):
    return runner_case(tmp_path, monkeypatch)


def configure(case, mode):
    source_table = case.clients["glue"].get_table.return_value
    plan = RUNNER.view_build_plan(case.project, **case.options["target"])
    expected = GOLD.expected_gold(
        case.options["input_path"] / "summary.json",
        plan["source_plan"]["binding"]["source_run_sha256"],
    )
    stopped = set()

    def table(**kw):
        if kw["Name"] != "building_proximity":
            return {"Table": fake_view(plan, kw["Name"])[0]}
        if len(case.requests) == 10:
            if mode == "final_source":
                value = copy.deepcopy(source_table)
                value["Table"]["VersionId"] = "2"
                return value
            if mode == "proof":
                (case.output / "gold/query-01/results.json").write_text("changed")
        return source_table

    def observed(**kw):
        identity = kw["QueryExecutionId"]
        request = case.requests[identity]
        sql = normalized_sql(request["QueryString"])
        dml = not sql.startswith("create or replace")
        state = (
            "CANCELLED"
            if identity in stopped
            else "RUNNING"
            if mode == "watchdog" and sql.startswith("SELECT source_release,")
            else "SUCCEEDED"
        )
        value = metadata(request, identity, state=state, scanned=4096 if dml else 0)
        if dml:
            query = value["QueryExecution"]
            query["StatementType"], query["SubstatementType"] = "DML", "SELECT"
            query["ResultConfiguration"]["OutputLocation"] = (
                request["ResultConfiguration"]["OutputLocation"] + identity + ".csv"
            )
        return value

    def results(**kw):
        sql = normalized_sql(case.requests[kw["QueryExecutionId"]]["QueryString"])
        if sql.startswith("select count(*) as failures"):
            return summary()
        if sql.startswith("SELECT source_release,"):
            value = result_rows(expected)
            if mode == "count":
                value["ResultSet"]["Rows"][1]["Data"][3]["VarCharValue"] = "7"
            if mode == "extra":
                value["ResultSet"]["Rows"].append(value["ResultSet"]["Rows"][1])
            return value
        return {
            "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
            "UpdateCount": 0,
        }

    def execute_tests(control, path):
        path.mkdir()
        for i in range(7):
            identity = control.start_query_execution(**params(control, i))["QueryExecutionId"]
            control.get_query_execution(QueryExecutionId=identity)
            control.get_query_results(QueryExecutionId=identity)
        return {
            "success": True,
            "test_statuses": {n: "pass" for n in TEST_NAMES},
            "failure_counts": {n: 0 for n in TEST_NAMES},
        }

    case.clients["glue"].get_table.side_effect = table
    case.clients["athena"].get_query_execution.side_effect = observed
    case.clients["athena"].get_query_results.side_effect = results
    case.clients["athena"].stop_query_execution.side_effect = (
        lambda **kw: stopped.add(kw["QueryExecutionId"]) or {}
    )
    case.options.update(execute_tests=execute_tests, view_readback=True, gold_results=True)


@pytest.mark.parametrize("mode", ["pass", "count", "extra", "watchdog", "final_source", "proof"])
def test_joined_receipt_requires_gold_match_and_all_final_gates(case, mode):
    configure(case, mode)
    if mode == "pass":
        report = RUNNER.run_view_build(case.clients, case.project, case.output, **case.options)
        assert (
            report["result_verified"]
            and report["view_catalog_verified"]
            and report["data_tests_passed"]
        )
        assert not report["writer_job_identity_verified"]
        assert len(case.requests) == 10 and (case.output / "submission-gate-10.json").exists()
        content = (case.output / "gold-result-verification.json").read_bytes()
        assert hashlib.sha256(content).hexdigest() == report["gold_result_verification_sha256"]
    else:
        with pytest.raises((ValueError, RuntimeError)):
            RUNNER.run_view_build(case.clients, case.project, case.output, **case.options)
        assert not (case.output / "receipt.json").exists()
        failure = json.loads((case.output / "failure.json").read_text())
        assert not failure["result_verified"]
        assert failure["stage"] == (
            "final_source_gate" if mode in {"final_source", "proof"} else "gold_result"
        )
        assert failure["gold_control"]["result_verified"] is (mode in {"final_source", "proof"})
        assert len(case.requests) == 10
    assert case.clients["athena"].stop_query_execution.call_count == (
        1 if mode == "watchdog" else 0
    )


@pytest.mark.parametrize("entry", ["injected", "owned"])
@pytest.mark.parametrize("mode", ["type", "views", "tests"])
def test_invalid_gold_options_fail_before_credentials_or_output(case, monkeypatch, entry, mode):
    resources = Mock(side_effect=AssertionError("No credentials"))
    monkeypatch.setattr(RUNNER, "query_resources", resources)
    options = dict(
        case.options, view_readback=mode != "views", gold_results="yes" if mode == "type" else True
    )
    if mode != "tests":
        options["execute_tests"] = Mock()
    if entry == "owned":
        del options["snapshot_reader"]
        del options["execute_models"]
        options.pop("execute_tests", None)
        options["data_tests"] = mode != "tests"
    with pytest.raises(ValueError, match="requires data tests and view readback"):
        if entry == "owned":
            RUNNER.run_owned_views(case.project, case.output, **options)
        else:
            RUNNER.run_view_build(case.clients, case.project, case.output, **options)
    resources.assert_not_called()
    assert not case.output.exists()
