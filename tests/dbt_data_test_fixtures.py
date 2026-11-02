"""Data-test plans and source-free DML response fixtures."""

import importlib.util
import sys

from athena_plan_fixtures import ROOT
from dbt_control_fixtures import case, metadata

for name in ("dbt_test_plan", "dbt_test_control"):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
PLAN = sys.modules["dbt_test_plan"]
CONTROL = sys.modules["dbt_test_control"]


def summary(failures=0):
    names = list(CONTROL.COLUMNS)
    values = [str(failures), "true" if failures else "false", "true" if failures else "false"]
    return {
        "ResultSet": {
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Name": name, "Type": kind}
                    for name, kind in zip(names, ["bigint", "boolean", "boolean"], strict=True)
                ]
            },
            "Rows": [{"Data": [{"VarCharValue": v} for v in row]} for row in (names, values)],
        }
    }


def test_case(tmp_path, monkeypatch):
    f = case(tmp_path, monkeypatch, controller_class=CONTROL.TestController)

    def observed(**kwargs):
        identity = kwargs["QueryExecutionId"]
        response = metadata(f.requests[identity], identity, scanned=4096)
        query = response["QueryExecution"]
        query["StatementType"], query["SubstatementType"] = "DML", "SELECT"
        query["ResultConfiguration"]["OutputLocation"] = (
            query["ResultConfiguration"]["OutputLocation"].removesuffix(".txt") + ".csv"
        )
        return response

    f.client.get_query_execution.side_effect = observed
    f.client.get_query_results.return_value = summary()
    return f
