"""Bound failure summaries and seven distinct queries; quality failure is not execution failure."""

import copy
import sqlite3

import pytest
from dbt_control_fixtures import params
from dbt_data_test_fixtures import CONTROL, PLAN, summary
from dbt_data_test_fixtures import test_case as make_case


@pytest.mark.parametrize("failures", [0, 1, 123])
def test_bounded_summary_distinguishes_pass_from_quality_failure(failures):
    assert CONTROL.failure_summary(summary(failures)) == failures


@pytest.mark.parametrize(
    "bad", ["page", "extra_row", "column", "header", "negative", "flags", "null", "oversized"]
)
def test_invalid_summary_cannot_be_accepted(bad):
    response = copy.deepcopy(summary())
    result = response["ResultSet"]
    if bad == "page":
        response["NextToken"] = "more"
    elif bad == "extra_row":
        result["Rows"].append(result["Rows"][1])
    elif bad == "column":
        result["ResultSetMetadata"]["ColumnInfo"][0]["Type"] = "varchar"
    elif bad == "header":
        result["Rows"][0]["Data"][0]["VarCharValue"] = "unexpected"
    elif bad == "negative":
        result["Rows"][1]["Data"][0]["VarCharValue"] = "-1"
    elif bad == "flags":
        result["Rows"][1]["Data"][1]["VarCharValue"] = "true"
    elif bad == "null":
        result["Rows"][1]["Data"][0] = {}
    else:
        result["Rows"][1]["Data"][0]["VarCharValue"] = "1" * 65536
    with pytest.raises(ValueError):
        CONTROL.failure_summary(response)


@pytest.mark.parametrize("quality_failure", [False, True])
def test_all_seven_queries_run_once_with_bounded_results(tmp_path, monkeypatch, quality_failure):
    f = make_case(tmp_path, monkeypatch)
    with f.control.session():
        for index in reversed(range(7)):
            f.client.get_query_results.return_value = summary(
                1 if quality_failure and index == 0 else 0
            )
            identity = f.control.start_query_execution(**params(f.control, index))[
                "QueryExecutionId"
            ]
            f.control.get_query_execution(QueryExecutionId=identity)
            f.control.get_query_results(QueryExecutionId=identity)
    report = f.control.report()
    assert report["data_tests_executed"] == 7 and report["data_tests_passed"] is not quality_failure
    assert report["status"] == ("data_tests_failed" if quality_failure else "data_tests_passed")
    assert not report["blocked"] and not report["result_verified"]
    assert report["data_scanned_bytes"] == 7 * 4096
    assert all(call.kwargs["MaxResults"] == 3 for call in f.client.get_query_results.call_args_list)


def test_a_test_cannot_be_replayed_under_a_new_token(tmp_path, monkeypatch):
    f = make_case(tmp_path, monkeypatch)
    with f.control.session():
        f.control.start_query_execution(**params(f.control))
        with pytest.raises(ValueError, match="repeated"):
            f.control.start_query_execution(**params(f.control))
    f.client.start_query_execution.assert_called_once()


@pytest.mark.parametrize("bad", [None, "null", "duplicate", "aggregate"])
def test_reference_sql_detects_bad_rows_and_aggregates(tmp_path, monkeypatch, bad):
    f = make_case(tmp_path, monkeypatch)
    binding = f.control.plan()["source_plan"]["binding"]
    statements = PLAN.compiled_tests(binding)
    with sqlite3.connect(":memory:") as db:
        db.execute(
            "create table facts (building_id,park_id,distance_m,within_threshold,"
            "grid_id,source_release,area_id)"
        )
        db.execute("insert into facts values ('b1',null,null,0,'g1','r1','pilot')")
        if bad == "null":
            db.execute("update facts set building_id=null")
        if bad == "duplicate":
            db.execute("insert into facts select * from facts")
        for name in ("grid_proximity", "release_proximity"):
            db.execute(f"create table {name} (mapped_buildings,buildings_within,share_within)")
            db.execute(f"insert into {name} values (1,0,0)")
        if bad == "aggregate":
            db.execute("update grid_proximity set buildings_within=2")
        counts = {}
        for name, sql in statements.items():
            sql = (
                PLAN.summary_sql(sql)
                .replace(
                    f'"awsdatacatalog"."{binding["source_schema"]}"."building_proximity"', "facts"
                )
                .replace("FOR VERSION AS OF 123", "")
            )
            for view in ("grid_proximity", "release_proximity"):
                sql = sql.replace(f'"AwsDataCatalog"."{binding["source_schema"]}"."{view}"', view)
            counts[name] = db.execute(sql).fetchone()[0]
        assert sum(counts.values()) == (0 if bad is None else 1)
