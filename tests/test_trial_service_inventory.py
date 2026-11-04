"""Bounded service observation with explicit target identity and no mutation or ownership claim."""

import hashlib
import json

import pytest
from trial_cleanup_fixtures import inventory
from trial_service_fixtures import SENTINEL, make_services, services


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(inventory, "ARTIFACTS", tmp_path / "artifacts")
    value = make_services(frozen)
    value.frozen, value.output = frozen, tmp_path / "artifacts/services"
    return value


def collect(case):
    return services.inventory_services(
        case.glue, case.athena, case.frozen.path, case.frozen.pin, case.output
    )


def assert_read_only(case):
    assert {c[0] for c in case.glue.method_calls} <= set(services.GLUE_READ_CALLS)
    assert {c[0] for c in case.athena.method_calls} <= set(services.ATHENA_READ_CALLS)
    case.glue.close.assert_not_called()
    case.athena.close.assert_not_called()


def test_complete_inventory_binds_records_and_omits_sql_arguments_and_provider_details(case):
    result = collect(case)
    assert result["status"] == "service_inventory_complete" and result["api_calls"] == 7
    assert result["catalog_observed"] and result["enumeration_complete"]
    assert not result["quiescence_and_catalog_observed"] and not result["deletion_authorized"]
    assert not result["query_ownership_verified"] and not result["absent_expected_relations"]
    for name, pin in result["records"].items():
        assert hashlib.sha256((case.output / name).read_bytes()).hexdigest() == pin
    assert all(SENTINEL not in p.read_text() for p in case.output.iterdir())
    expected = case.scope["expected_services"]
    case.glue.get_database.assert_called_once_with(
        CatalogId="123456789012", Name=expected["database"]
    )
    case.glue.get_tables.assert_called_once_with(
        CatalogId="123456789012",
        DatabaseName=expected["database"],
        AttributesToGet=["NAME", "TABLE_TYPE"],
        MaxResults=10,
    )
    case.glue.get_job_runs.assert_called_once_with(JobName=expected["job"], MaxResults=10)
    case.athena.list_query_executions.assert_called_once_with(
        WorkGroup=expected["workgroup"], MaxResults=10
    )
    assert_read_only(case)


@pytest.mark.parametrize(
    "what", ["relation", "missing_type", "job_active", "query_active", "context", "enabled"]
)
def test_unexpected_relations_active_work_or_context_are_preserved_for_review(case, what):
    reason = {
        "relation": "unexpected_relations",
        "missing_type": "unexpected_relations",
        "job_active": "active_glue_runs",
        "query_active": "active_athena_queries",
        "context": "query_context_differs",
        "enabled": "workgroup_enabled",
    }[what]
    if what == "relation":
        case.tables.append({"Name": "keep_other_table", "TableType": "EXTERNAL_TABLE"})
    elif what == "missing_type":
        case.tables[0].pop("TableType")
    elif what == "job_active":
        case.run["JobRunState"] = "STOPPING"
    elif what == "query_active":
        case.query["Status"]["State"] = "QUEUED"
    elif what == "context":
        case.query["QueryExecutionContext"]["Database"] = "other"
    else:
        case.group["State"] = "ENABLED"
    result = collect(case)
    assert (
        result["status"] == "service_inventory_requires_review"
        and reason in result["review_reasons"]
    )
    assert not result["query_ownership_verified"] and not result["deletion_authorized"]
    assert_read_only(case)


def test_absent_relations_and_empty_available_history_are_reported_without_ownership_claim(case):
    case.glue.get_tables.return_value = {}
    case.glue.get_job_runs.return_value = {"JobRuns": []}
    case.athena.list_query_executions.return_value = {"QueryExecutionIds": []}
    result = collect(case)
    assert result["absent_expected_relations"] == sorted(case.scope["expected_services"]["tables"])
    assert result["api_calls"] == 6 and not result["job_runs"] and not result["queries"]
    assert not result["deletion_authorized"] and not result["quiescence_and_catalog_observed"]
    case.athena.get_query_execution.assert_not_called()
    assert_read_only(case)


@pytest.mark.parametrize("stream", ["tables", "runs", "queries"])
def test_each_listing_preserves_opaque_pagination_and_explicit_target(case, stream):
    method, field = {
        "tables": (case.glue.get_tables, "TableList"),
        "runs": (case.glue.get_job_runs, "JobRuns"),
        "queries": (case.athena.list_query_executions, "QueryExecutionIds"),
    }[stream]
    first = method.return_value
    method.side_effect = [first | {"NextToken": "opaque+token/%"}, {field: []}]
    result = collect(case)
    assert result["api_calls"] == 8 and method.call_args.kwargs["NextToken"] == "opaque+token/%"
    assert method.call_args.kwargs["MaxResults"] == 10
    assert_read_only(case)


@pytest.mark.parametrize(
    "defect",
    [
        "database",
        "location",
        "owner",
        "job",
        "role",
        "script",
        "workgroup",
        "result_owner",
        "result_location",
        "enforce",
        "query_id",
        "query_group",
        "query_state",
        "run_state",
        "run_job",
        "duplicate",
    ],
)
def test_wrong_identity_ambiguous_rows_or_unknown_state_never_complete(case, defect):
    if defect in {"database", "location", "owner"}:
        case.database[
            {"database": "Name", "location": "LocationUri", "owner": "CatalogId"}[defect]
        ] = "wrong"
    elif defect in {"job", "role"}:
        case.job["Name" if defect == "job" else "Role"] = "wrong"
    elif defect == "script":
        case.job["Command"]["ScriptLocation"] = "s3://other/script.py"
    elif defect == "workgroup":
        case.group["Name"] = "other"
    elif defect in {"result_owner", "result_location"}:
        case.group["Configuration"]["ResultConfiguration"][
            "ExpectedBucketOwner" if defect == "result_owner" else "OutputLocation"
        ] = "wrong"
    elif defect == "enforce":
        case.group["Configuration"]["EnforceWorkGroupConfiguration"] = 1
    elif defect in {"query_id", "query_group"}:
        case.query["QueryExecutionId" if defect == "query_id" else "WorkGroup"] = "wrong"
    elif defect == "query_state":
        case.query["Status"]["State"] = "UNKNOWN"
    elif defect == "run_state":
        case.run["JobRunState"] = "UNKNOWN"
    elif defect == "run_job":
        case.run["JobName"] = "other"
    else:
        case.tables.append(case.tables[0])
    with pytest.raises(ValueError):
        collect(case)
    assert not (case.output / "inventory.json").exists()
    assert json.loads((case.output / "failure.json").read_bytes())["deletion_authorized"] is False
    assert_read_only(case)


@pytest.mark.parametrize(
    "defect", ["cycle", "pages", "rows", "items", "record_bytes", "report_bytes"]
)
def test_pagination_row_api_and_storage_bounds_retain_failure_without_complete_report(
    case, monkeypatch, defect
):
    if defect == "cycle":
        case.glue.get_tables.side_effect = lambda **kw: {"TableList": [], "NextToken": "same"}
    elif defect == "pages":
        number = 0

        def next_page(**kw):
            nonlocal number
            number += 1
            return {"TableList": [], "NextToken": str(number)}

        case.glue.get_tables.side_effect = next_page
    elif defect == "rows":
        case.tables[:] = [{"Name": str(i), "TableType": "EXTERNAL_TABLE"} for i in range(11)]
    elif defect == "items":
        monkeypatch.setattr(services, "MAX_ITEMS", 1)
    elif defect == "record_bytes":
        monkeypatch.setattr(services, "MAX_RECORD_BYTES", 8000)
        case.tables[:] = [
            {"Name": str(i) + "x" * 1000, "TableType": "EXTERNAL_TABLE"} for i in range(10)
        ]
    else:
        monkeypatch.setattr(services, "MAX_REPORT_BYTES", 16)
    with pytest.raises(ValueError):
        collect(case)
    assert not (case.output / "inventory.json").exists()
    assert (case.output / "failure.json").exists()
    assert case.glue.get_tables.call_count <= 2
    assert_read_only(case)


def test_service_error_preserves_prior_projected_records_without_provider_error_text(case):
    case.athena.get_work_group.side_effect = RuntimeError(SENTINEL)
    with pytest.raises(RuntimeError):
        collect(case)
    assert (case.output / "job-runs-01.json").exists()
    assert all(SENTINEL not in p.read_text() for p in case.output.iterdir())
    assert json.loads((case.output / "failure.json").read_bytes())["stage"] == "workgroup"
    assert not (case.output / "inventory.json").exists()
    assert_read_only(case)


@pytest.mark.parametrize("defect", ["pin", "existing", "outside", "release"])
def test_bad_local_scope_is_rejected_before_any_service_read(case, defect):
    if defect == "pin":
        case.frozen.pin = "0" * 64
    elif defect == "existing":
        case.output.mkdir(parents=True)
    elif defect == "outside":
        case.output = case.output.parents[1] / "outside"
    else:
        case.output = case.frozen.path / "inventory"
    with pytest.raises(ValueError):
        collect(case)
    assert not case.glue.method_calls and not case.athena.method_calls
