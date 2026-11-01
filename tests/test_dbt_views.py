"""Offline checker rejects unexpected API scope, SQL drift and hidden adapter errors."""

import copy
import importlib.util
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from athena_plan_fixtures import ATHENA, OPTIONS

ROOT = Path(__file__).resolve().parents[1]
for name in ("check_dbt_project", "check_dbt_views"):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
VIEWS = sys.modules["check_dbt_views"]


@pytest.mark.parametrize(
    "service,operation,params",
    [
        ("s3", "DeleteObject", {"Bucket": "pilot", "Key": "facts"}),
        ("glue", "GetTables", {"CatalogId": "999999999999", "DatabaseName": "urban_geo_test_m4"}),
        ("glue", "GetTables", {"CatalogId": "123456789012", "DatabaseName": "other"}),
        (
            "glue",
            "GetTables",
            {
                "CatalogId": "123456789012",
                "DatabaseName": "urban_geo_test_m4",
                "NextToken": "extra",
            },
        ),
        (
            "glue",
            "DeleteTableVersion",
            {
                "DatabaseName": "urban_geo_test_m4",
                "TableName": "building_proximity",
                "VersionId": "1",
            },
        ),
        ("glue", "DeleteTable", {"DatabaseName": "urban_geo_test_m4", "Name": "grid_proximity"}),
        ("athena", "GetQueryExecution", {"QueryExecutionId": "unowned-query"}),
    ],
)
def test_fake_boundary_records_and_rejects_out_of_scope_operations(service, operation, params):
    case = VIEWS.ViewScenario(ATHENA.read_plan(**OPTIONS), "create")
    with pytest.raises(ValueError):
        case.reply(service, operation, params)
    assert case.violations and len(case.calls) == 1


def request(plan, name="grid_proximity"):
    return {
        "QueryString": (
            f'create or replace view "AwsDataCatalog"."urban_geo_test_m4"."{name}" as SELECT 1'
        ),
        "QueryExecutionContext": plan["request_template"]["QueryExecutionContext"],
        "WorkGroup": plan["workgroup"]["Name"],
        "ResultConfiguration": {
            "OutputLocation": plan["request_template"]["ResultConfiguration"]["OutputLocation"]
        },
    }


@pytest.mark.parametrize("change", ["workgroup", "results", "catalog", "name", "extra"])
def test_fake_boundary_rejects_changed_query_target(change):
    plan = ATHENA.read_plan(**OPTIONS)
    case = VIEWS.ViewScenario(plan, "create")
    params = copy.deepcopy(request(plan))
    if change == "workgroup":
        params["WorkGroup"] = "primary"
    elif change == "results":
        params["ResultConfiguration"]["OutputLocation"] = "s3://other/results"
    elif change == "catalog":
        params["QueryExecutionContext"]["Catalog"] = "other"
    elif change == "name":
        params["QueryString"] = params["QueryString"].replace(
            "grid_proximity", "building_proximity"
        )
    else:
        params["ResultReuseConfiguration"] = {"ResultReuseByAgeConfiguration": {"Enabled": True}}
    with pytest.raises(ValueError, match="Unexpected view"):
        case.reply("athena", "StartQueryExecution", params)
    assert case.queries == []


def test_trace_comparison_detects_changed_sql_and_swallowed_version_errors():
    plan = ATHENA.read_plan(**OPTIONS)
    case = VIEWS.ViewScenario(plan, "create")
    manifest = {
        "nodes": {
            "model.urban_geo_athena." + name: {"compiled_code": "SELECT 1"} for name in VIEWS.VIEWS
        }
    }
    results = {
        "results": [
            {"unique_id": "model.urban_geo_athena." + name, "status": "success"}
            for name in VIEWS.VIEWS
        ]
    }
    for name in VIEWS.VIEWS:
        case.reply("athena", "StartQueryExecution", request(plan, name))
    case.verify(manifest, results)
    case.queries[0] += "; DROP TABLE facts"
    with pytest.raises(ValueError, match="differs"):
        case.verify(manifest, results)
    case.queries[0] = request(plan)["QueryString"]
    case.violations.append("Adapter swallowed a version-maintenance exception")
    with pytest.raises(ValueError, match="Unexpected simulated API"):
        case.verify(manifest, results)


def test_call_count_ceiling_stops_a_simulated_loop():
    case = VIEWS.ViewScenario(ATHENA.read_plan(**OPTIONS), "create")
    for _ in range(40):
        case.reply("sts", "GetCallerIdentity", {})
    with pytest.raises(ValueError, match="excessive"):
        case.reply("sts", "GetCallerIdentity", {})


@pytest.mark.parametrize("unsafe", ["existing", "outside", "inside_project", "contains_project"])
def test_bad_output_fails_before_optional_sdk_imports(tmp_path, monkeypatch, unsafe):
    artifacts = tmp_path / "artifacts"
    project = artifacts / "project"
    project.mkdir(parents=True)
    monkeypatch.setattr(VIEWS, "ARTIFACTS", artifacts)
    monkeypatch.setattr(VIEWS, "verify_project", Mock(return_value={}))
    pins = Mock(side_effect=AssertionError("Do not inspect optional tooling"))
    monkeypatch.setattr(VIEWS, "tooling_sdk_pins", pins)
    output = {
        "existing": project,
        "outside": tmp_path / "outside",
        "inside_project": project / "check",
        "contains_project": artifacts,
    }[unsafe]
    with pytest.raises(ValueError, match="fresh"):
        VIEWS.check_views(project, output, "create")
    pins.assert_not_called()
