"""Synthetic source/reader plus a model-executor double for the joined view runner."""

import importlib.util
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from athena_runner_fixtures import CALLER as CALLER
from athena_runner_fixtures import ROOT, trial_case
from dbt_control_fixtures import PREPARE, VIEWS, metadata, params

for name in ("dbt_test_plan", "dbt_test_control", "dbt_view_catalog", "dbt_gold_result"):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

SPEC = importlib.util.spec_from_file_location(
    "dbt_view_runner", ROOT / "scripts/dbt_view_runner.py"
)
assert SPEC is not None and SPEC.loader is not None
RUNNER = importlib.util.module_from_spec(SPEC)
sys.modules["dbt_view_runner"] = RUNNER
SPEC.loader.exec_module(RUNNER)


def runner_case(tmp_path, monkeypatch):
    base = trial_case(tmp_path, monkeypatch)
    for module in (RUNNER, VIEWS, PREPARE):
        monkeypatch.setattr(module, "ARTIFACTS", tmp_path)
    project = tmp_path / "project"
    PREPARE.prepare_project(project, **base.plan["binding"])
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    writer = receipts / "writer.json"
    writer.write_bytes(base.options["writer_receipt"].read_bytes())
    output = tmp_path / "view-run"
    requests = {}
    clients = {key: base.clients[key] for key in ("athena", "sts", "glue")}

    def start(**request):
        number = len(requests) + 1
        assert base.reader.called
        assert (output / f"submission-gate-{number:02}.json").exists()

        def phase(sql):
            if VIEWS.normalized_sql(sql).startswith("select count(*) as failures"):
                return "tests"
            if VIEWS.normalized_sql(sql).startswith("SELECT source_release,"):
                return "gold"
            return "queries"

        selected = phase(request["QueryString"])
        ordinal = 1 + sum(phase(r["QueryString"]) == selected for r in requests.values())
        phase_name = selected
        saved = json.loads((output / f"{phase_name}/query-{ordinal:02}/intent.json").read_text())
        assert saved["request"] == request
        identity = "view-" + str(number)
        requests[identity] = request
        return {"QueryExecutionId": identity}

    client = clients["athena"]
    client.start_query_execution.side_effect = start
    client.get_query_execution.side_effect = lambda **kw: metadata(
        requests[kw["QueryExecutionId"]], kw["QueryExecutionId"]
    )
    client.get_query_results.return_value = {
        "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
        "UpdateCount": 0,
    }

    def execute(control, path):
        path.mkdir()
        for index in range(2):
            identity = control.start_query_execution(**params(control, index))["QueryExecutionId"]
            control.get_query_execution(QueryExecutionId=identity)
            control.get_query_results(QueryExecutionId=identity)
        return {
            "success": True,
            "model_statuses": {name: "success" for name in ("grid_proximity", "release_proximity")},
        }

    executor = Mock(side_effect=execute)
    options = {key: value for key, value in base.options.items() if key != "writer_receipt"}
    options.update(target=base.plan["target"], writer_receipt=writer, execute_models=executor)
    return SimpleNamespace(
        project=project,
        output=output,
        options=options,
        clients=clients,
        requests=requests,
        reader=base.reader,
        executor=executor,
    )
