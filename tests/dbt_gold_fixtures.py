"""Small retained-reference and DML fixtures; no Spark or service clients."""

import importlib.util
import json
import sys
from types import SimpleNamespace

from athena_runner_fixtures import ROOT, trial_case
from dbt_control_fixtures import PREPARE, VIEWS, metadata

SPEC = importlib.util.spec_from_file_location(
    "dbt_gold_result", ROOT / "scripts/dbt_gold_result.py"
)
assert SPEC is not None and SPEC.loader is not None
GOLD = importlib.util.module_from_spec(SPEC)
sys.modules["dbt_gold_result"] = GOLD
SPEC.loader.exec_module(GOLD)


def result_rows(expected):
    names = list(GOLD.COLUMNS)
    return {
        "ResultSet": {
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Name": n, "Type": t} for n, t in zip(names, GOLD.TYPES, strict=True)
                ]
            },
            "Rows": [
                {"Data": [{"VarCharValue": v} for v in row]}
                for row in (names, [str(expected[n]) for n in names])
            ],
        },
        "UpdateCount": 0,
    }


def gold_case(tmp_path, monkeypatch):
    base = trial_case(tmp_path, monkeypatch)
    for module in (VIEWS, PREPARE):
        monkeypatch.setattr(module, "ARTIFACTS", tmp_path)
    project = tmp_path / "project"
    PREPARE.prepare_project(project, **base.plan["binding"])
    client = base.clients["athena"]
    control = GOLD.GoldController(
        client,
        project,
        tmp_path / "gold",
        **base.plan["target"],
        now=base.options["now"],
        pause=base.options["pause"],
    )
    requests = {}

    def start(**params):
        requests["gold-1"] = params
        assert (
            json.loads((control.output / "query-01/intent.json").read_text())["request"] == params
        )
        return {"QueryExecutionId": "gold-1"}

    def observed(**kwargs):
        identity = kwargs["QueryExecutionId"]
        value = metadata(requests[identity], identity, scanned=4096)
        query = value["QueryExecution"]
        query["StatementType"], query["SubstatementType"] = "DML", "SELECT"
        query["ResultConfiguration"]["OutputLocation"] = (
            requests[identity]["ResultConfiguration"]["OutputLocation"] + identity + ".csv"
        )
        return value

    client.start_query_execution.side_effect = start
    client.get_query_execution.side_effect = observed
    client.get_query_results.return_value = result_rows(base.expected)
    return SimpleNamespace(
        control=control,
        client=client,
        expected=base.expected,
        summary=base.options["input_path"] / "summary.json",
    )
