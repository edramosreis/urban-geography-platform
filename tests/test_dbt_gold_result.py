"""Release-view result acceptance requires the pinned row, not merely a successful query."""

import copy
import json

import pytest
from dbt_control_fixtures import params
from dbt_gold_fixtures import GOLD, gold_case, result_rows


@pytest.fixture
def case(tmp_path, monkeypatch):
    return gold_case(tmp_path, monkeypatch)


@pytest.mark.parametrize("share", ["0.3333333333333333", "0.3333333333334"])
def test_valid_row_uses_existing_exact_counts_and_share_tolerance(case, share):
    response = result_rows(case.expected)
    response["ResultSet"]["Rows"][1]["Data"][5]["VarCharValue"] = share
    assert GOLD.compare_result(response, case.expected)["share_within"] == float(share)


@pytest.mark.parametrize(
    "bad",
    [
        "columns",
        "type",
        "null",
        "cell_type",
        "extra_cell",
        "header",
        "missing",
        "extra_row",
        "page",
        "updated",
        "bool_update",
        "oversized",
        "count",
        "hash",
        "nonfinite",
        "negative",
    ],
)
def test_malformed_or_different_results_are_not_accepted(case, bad):
    response = copy.deepcopy(case.client.get_query_results.return_value)
    rs = response["ResultSet"]
    row = rs["Rows"][1]["Data"]
    if bad == "columns":
        rs["ResultSetMetadata"]["ColumnInfo"].reverse()
    elif bad == "type":
        rs["ResultSetMetadata"]["ColumnInfo"][3]["Type"] = "double"
    elif bad == "null":
        row[0] = {}
    elif bad == "cell_type":
        row[0]["VarCharValue"] = 123
    elif bad == "extra_cell":
        row.append({"VarCharValue": "extra"})
    elif bad == "header":
        rs["Rows"][0]["Data"][0]["VarCharValue"] = "other"
    elif bad == "missing":
        rs["Rows"] = rs["Rows"][:1]
    elif bad == "extra_row":
        rs["Rows"].append(rs["Rows"][1])
    elif bad == "page":
        response["NextToken"] = "more"
    elif bad == "updated":
        response["UpdateCount"] = 1
    elif bad == "bool_update":
        response["UpdateCount"] = True
    elif bad == "oversized":
        row[0]["VarCharValue"] = "x" * 65536
    elif bad == "count":
        row[3]["VarCharValue"] = "7"
    elif bad == "hash":
        row[2]["VarCharValue"] = "b" * 64
    elif bad == "nonfinite":
        row[5]["VarCharValue"] = "NaN"
    elif bad == "negative":
        row[4]["VarCharValue"] = "-1"
    with pytest.raises(ValueError):
        GOLD.compare_result(response, case.expected)


def test_one_owned_view_query_compares_and_saves_the_reference_proof(case):
    control = case.control
    with control.session():
        identity = control.start_query_execution(**params(control))["QueryExecutionId"]
        proof = control.verify_result(identity, summary_path=case.summary)
    report = control.report()
    assert report["result_verified"] and report["status"] == "gold_result_matches"
    assert proof["row"] == case.expected and proof["data_scanned_bytes"] == 4096
    assert control.plan()["limits"]["max_queries"] == 1
    sql = control.plan()["queries"][0]["sql"]
    assert (
        '"release_proximity"' in sql and "building_proximity" not in sql and sql.endswith("LIMIT 2")
    )
    case.client.get_query_results.assert_called_once_with(QueryExecutionId=identity, MaxResults=3)
    assert case.client.get_query_execution.call_count == 3
    saved = json.loads((control.output / "query-01/results.json").read_text())
    assert GOLD.sha256(GOLD.canonical(saved)) == proof["response_sha256"]


@pytest.mark.parametrize("changed", ["summary", "request", "metadata"])
def test_reference_request_or_metadata_drift_prevents_verification(case, changed):
    control = case.control
    response = case.client.get_query_results.return_value
    observe = case.client.get_query_execution.side_effect

    def results(**kwargs):
        if changed == "summary":
            case.summary.write_text("changed")
        elif changed == "request":
            (control.output / "query-01/intent.json").write_text("changed")
        elif changed == "metadata":

            def drift(**kw):
                value = observe(**kw)
                value["QueryExecution"]["Statistics"]["DataScannedInBytes"] += 1
                return value

            case.client.get_query_execution.side_effect = drift
        return response

    case.client.get_query_results.side_effect = results
    with pytest.raises(ValueError), control.session():
        identity = control.start_query_execution(**params(control))["QueryExecutionId"]
        control.verify_result(identity, summary_path=case.summary)
    assert control.report()["blocked"] and not control.report()["result_verified"]
    assert not (control.output / "query-01/gold-result.json").exists()


@pytest.mark.parametrize("foreign", [False, True])
def test_result_read_cannot_use_a_foreign_id_or_be_replayed(case, foreign):
    control = case.control
    with control.session():
        identity = control.start_query_execution(**params(control))["QueryExecutionId"]
        if not foreign:
            control.verify_result(identity, summary_path=case.summary)
        with pytest.raises(ValueError):
            control.verify_result("foreign" if foreign else identity, summary_path=case.summary)
    assert not control.report()["result_verified"]
    assert case.client.get_query_results.call_count == (0 if foreign else 1)
