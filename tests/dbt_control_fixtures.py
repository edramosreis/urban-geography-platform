"""Synthetic view control and SDK metadata fixtures; no optional dbt/SDK imports."""

import importlib.util
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from athena_control_fixtures import CONTROL, Clock
from athena_plan_fixtures import ATHENA, OPTIONS, ROOT, workgroup

for name in ("dbt_view_plan", "dbt_view_control"):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
VIEWS = sys.modules["dbt_view_control"]
PREPARE = sys.modules["prepare_dbt_project"]


def metadata(request, identity, state="SUCCEEDED", scanned=0):
    return {
        "QueryExecution": {
            "QueryExecutionId": identity,
            "Query": request["QueryString"],
            "WorkGroup": request["WorkGroup"],
            "QueryExecutionContext": request["QueryExecutionContext"],
            "StatementType": "DDL",
            "Status": {"State": state},
            "EngineVersion": {
                "SelectedEngineVersion": "Athena engine version 3",
                "EffectiveEngineVersion": "Athena engine version 3",
            },
            "ResultConfiguration": request["ResultConfiguration"]
            | {
                "OutputLocation": request["ResultConfiguration"]["OutputLocation"]
                + identity
                + ".txt"
            },
            "ResultReuseConfiguration": request["ResultReuseConfiguration"],
            "Statistics": {
                "DataScannedInBytes": scanned,
                "ResultReuseInformation": {"ReusedPreviousResult": False},
            },
        }
    }


def params(controller, index=0):
    plan = controller.plan()
    source = plan["source_plan"]
    return {
        "QueryString": plan["queries"][index]["sql"],
        "WorkGroup": source["workgroup"]["Name"],
        "QueryExecutionContext": source["request_template"]["QueryExecutionContext"],
        "ResultConfiguration": {
            "OutputLocation": source["request_template"]["ResultConfiguration"]["OutputLocation"]
        },
    }


def case(tmp_path, monkeypatch, *, before_submit=None):
    for module in (CONTROL, VIEWS, PREPARE):
        monkeypatch.setattr(module, "ARTIFACTS", tmp_path)
    project = tmp_path / "project"
    PREPARE.prepare_project(
        project, source_schema="urban_geo_test_m4", snapshot_id=123, source_run_sha256="a" * 64
    )
    client = Mock()
    client.get_work_group.return_value = {"WorkGroup": workgroup(ATHENA.read_plan(**OPTIONS))}
    requests = {}
    clock = Clock()
    control = VIEWS.ViewController(
        client,
        project,
        tmp_path / "run",
        deployment_name=OPTIONS["deployment_name"],
        account_id=OPTIONS["account_id"],
        now=clock.now,
        pause=clock.pause,
        before_submit=before_submit,
    )

    def start(**request):
        identity = "view-" + str(len(requests) + 1)
        saved = __import__("json").loads(
            (control.output / f"query-{len(requests) + 1:02}" / "intent.json").read_text()
        )
        assert saved["request"] == request
        assert (control.output / f"query-{len(requests) + 1:02}" / "submission.json").exists()
        requests[identity] = request
        return {"QueryExecutionId": identity}

    client.start_query_execution.side_effect = start
    client.get_query_execution.side_effect = lambda **kw: metadata(
        requests[kw["QueryExecutionId"]], kw["QueryExecutionId"]
    )
    client.get_query_results.return_value = {
        "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
        "UpdateCount": 0,
    }
    return SimpleNamespace(
        control=control, client=client, requests=requests, clock=clock, project=project
    )
