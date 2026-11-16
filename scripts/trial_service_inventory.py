"""Bounded read-only observations of the dedicated catalog, writer job and workgroup."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from athena_control import ACTIVE as QUERY_ACTIVE
from athena_control import TERMINAL as QUERY_TERMINAL
from athena_read_plan import EXECUTION_CATALOG
from glue_control import ACTIVE as JOB_ACTIVE
from glue_control import TERMINAL as JOB_TERMINAL
from trial_cleanup_inventory import _save, _text, cleanup_scope, validate_inventory_output

PAGE_SIZE = 10
MAX_PAGES = 2
MAX_ITEMS = 20
MAX_CALLS = 3 + 3 * MAX_PAGES + MAX_ITEMS
MAX_RECORD_BYTES = 64 * 1024
MAX_REPORT_BYTES = 512 * 1024
GLUE_READ_CALLS = ("get_database", "get_tables", "get_job", "get_job_runs")
ATHENA_READ_CALLS = ("get_work_group", "list_query_executions", "get_query_execution")


class GlueClient(Protocol):
    def get_database(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_tables(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_job(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_job_runs(self, **kwargs: Any) -> dict[str, Any]: ...


class AthenaClient(Protocol):
    def get_work_group(self, **kwargs: Any) -> dict[str, Any]: ...
    def list_query_executions(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]: ...


def workgroup_observation(response: dict[str, Any], scope: dict[str, Any]) -> dict[str, Any]:
    owner = scope["target"]["account_id"]
    expected = scope["expected_services"]["workgroup"]
    results = "s3://" + scope["buckets"]["results"]["name"] + "/queries/"
    group = response["WorkGroup"]
    config = group.get("Configuration", {})
    result = config.get("ResultConfiguration", {})
    if (
        group.get("Name") != expected
        or group.get("State") not in {"ENABLED", "DISABLED"}
        or config.get("EnforceWorkGroupConfiguration") is not True
        or result.get("OutputLocation") != results
        or result.get("ExpectedBucketOwner") != owner
    ):
        raise ValueError("Dedicated Athena workgroup identity/configuration differs")
    return {
        "name": group["Name"],
        "state": group["State"],
        "results_location": results,
        "expected_bucket_owner": owner,
        "enforce_workgroup_configuration": True,
    }


def inventory_services(
    glue: GlueClient, athena: AthenaClient, release: Path, pin: str, output: Path
) -> dict[str, Any]:
    """Observe only; clients belong to the caller and no observed ID authorizes a stop."""
    scope = cleanup_scope(release, pin)
    validate_inventory_output(release, output)
    expected = scope["expected_services"]
    owner = scope["target"]["account_id"]
    deployment = scope["target"]["deployment_name"]
    warehouse = "s3://" + scope["buckets"]["lake"]["name"] + "/warehouse/"
    script = (
        "s3://"
        + scope["buckets"]["lake"]["name"]
        + "/runtime/releases/"
        + scope["release_id"]
        + "/run_glue_s3_writer.py"
    )
    plan = {
        "version": 1,
        "release_manifest_sha256": pin,
        "target": scope["target"],
        "expected_services": expected,
        "read_only": True,
        "deletion_authorized": False,
        "limits": {
            "page_size": PAGE_SIZE,
            "max_pages_per_listing": MAX_PAGES,
            "max_items_per_listing": MAX_ITEMS,
            "max_service_calls": MAX_CALLS,
            "max_record_bytes": MAX_RECORD_BYTES,
            "max_report_bytes": MAX_REPORT_BYTES,
        },
    }
    output.mkdir(parents=True)
    plan_pin = _save(output / "plan.json", plan, MAX_RECORD_BYTES)
    started = datetime.now(UTC).isoformat()
    records: dict[str, str] = {}
    calls = 0
    stage = "database"

    def request(method: Callable[..., dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        if calls >= MAX_CALLS:
            raise ValueError("Service inventory API ceiling reached")
        calls += 1
        return method(**params)

    def save(name: str, value: dict[str, Any]) -> None:
        records[name] = _save(output / name, {"plan_sha256": plan_pin, **value}, MAX_RECORD_BYTES)

    def listing(
        label: str,
        method: Callable[..., dict[str, Any]],
        params: dict[str, Any],
        field: str,
        project: Callable[[Any], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        identities: set[str] = set()
        cursors: set[str] = set()
        cursor: dict[str, str] = {}
        for number in range(1, MAX_PAGES + 1):
            response = request(method, params | {"MaxResults": PAGE_SIZE} | cursor)
            rows = response.get(field, [])
            if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
                raise ValueError("Invalid or oversized service inventory page")
            page = []
            for row in rows:
                item = project(row)
                if item["id"] in identities:
                    raise ValueError("Duplicate service inventory identity")
                identities.add(item["id"])
                page.append(item)
            if len(items) + len(page) > MAX_ITEMS:
                raise ValueError("Service inventory item ceiling reached")
            token = response.get("NextToken")
            if token is not None:
                token = _text(token)
            save(
                f"{label}-{number:02}.json",
                {
                    "request": params | {"MaxResults": PAGE_SIZE} | cursor,
                    "items": page,
                    "next_token": token,
                },
            )
            items.extend(page)
            if token is None:
                return items
            if token in cursors:
                raise ValueError("Service inventory pagination made no progress")
            cursors.add(token)
            cursor = {"NextToken": token}
        raise ValueError("Service inventory page ceiling reached before enumeration completed")

    def relation(row: Any) -> dict[str, Any]:
        name = _text(row.get("Name"), 1024)
        kind = None if row.get("TableType") is None else _text(row["TableType"], 255)
        if (
            row.get("CatalogId", owner) != owner
            or row.get("DatabaseName", expected["database"]) != expected["database"]
        ):
            raise ValueError("Catalog relation identity differs")
        wanted = "EXTERNAL_TABLE" if name == "building_proximity" else "VIRTUAL_VIEW"
        return {
            "id": name,
            "table_type": kind,
            "in_cleanup_scope": name in expected["tables"],
            "expected_type_matches": name in expected["tables"] and kind == wanted,
        }

    def job_run(row: Any) -> dict[str, Any]:
        state = row.get("JobRunState")
        if row.get("JobName") != expected["job"] or state not in JOB_ACTIVE | JOB_TERMINAL:
            raise ValueError("Job run identity or state differs")
        return {"id": _text(row.get("Id"), 255), "state": state, "active": state in JOB_ACTIVE}

    try:
        database = request(glue.get_database, {"CatalogId": owner, "Name": expected["database"]})[
            "Database"
        ]
        if (
            database.get("Name") != expected["database"]
            or database.get("CatalogId", owner) != owner
            or database.get("LocationUri") != warehouse
            or database.get("TargetDatabase")
            or database.get("FederatedDatabase")
        ):
            raise ValueError("Dedicated catalog database identity/location differs")
        save(
            "database.json", {"name": database["Name"], "catalog_id": owner, "location": warehouse}
        )
        stage = "relations"
        relations = listing(
            stage,
            glue.get_tables,
            {
                "CatalogId": owner,
                "DatabaseName": expected["database"],
                "AttributesToGet": ["NAME", "TABLE_TYPE"],
            },
            "TableList",
            relation,
        )
        stage = "job"
        job = request(glue.get_job, {"JobName": expected["job"]})["Job"]
        if (
            job.get("Name") != expected["job"]
            or job.get("Role") != f"arn:aws:iam::{owner}:role/service-role/{deployment}-glue-writer"
            or job.get("Command", {}).get("ScriptLocation") != script
        ):
            raise ValueError("Dedicated Glue job identity/script differs")
        save("job.json", {"name": job["Name"], "role": job["Role"], "script_location": script})
        stage = "job-runs"
        runs = listing(stage, glue.get_job_runs, {"JobName": expected["job"]}, "JobRuns", job_run)
        stage = "workgroup"
        group = workgroup_observation(
            request(athena.get_work_group, {"WorkGroup": expected["workgroup"]}), scope
        )
        save("workgroup.json", group)
        stage = "query-ids"
        ids = listing(
            stage,
            athena.list_query_executions,
            {"WorkGroup": expected["workgroup"]},
            "QueryExecutionIds",
            lambda row: {"id": _text(row, 128)},
        )
        queries = []
        for number, row in enumerate(ids, 1):
            stage = "query"
            query = request(athena.get_query_execution, {"QueryExecutionId": row["id"]})[
                "QueryExecution"
            ]
            state = query.get("Status", {}).get("State")
            if (
                query.get("QueryExecutionId") != row["id"]
                or query.get("WorkGroup") != expected["workgroup"]
                or state not in QUERY_ACTIVE | QUERY_TERMINAL
            ):
                raise ValueError("Query identity, workgroup or state differs")
            context = query.get("QueryExecutionContext", {})
            matches = (
                context.get("Catalog") == EXECUTION_CATALOG
                and context.get("Database") == expected["database"]
            )
            value = {
                "id": row["id"],
                "state": state,
                "active": state in QUERY_ACTIVE,
                "matches_trial_context": matches,
            }
            save(f"query-{number:02}.json", value)
            queries.append(value)
        reasons = []
        if any(not row["expected_type_matches"] for row in relations):
            reasons.append("unexpected_relations")
        if any(row["active"] for row in runs):
            reasons.append("active_glue_runs")
        if any(row["active"] for row in queries):
            reasons.append("active_athena_queries")
        if any(not row["matches_trial_context"] for row in queries):
            reasons.append("query_context_differs")
        if group["state"] != "DISABLED":
            reasons.append("workgroup_enabled")
        report = {
            "version": 1,
            "status": "service_inventory_requires_review"
            if reasons
            else "service_inventory_complete",
            "plan_sha256": plan_pin,
            "release_manifest_sha256": pin,
            "records": records,
            "started_at_utc": started,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "api_calls": calls,
            "enumeration_complete": True,
            "relations": relations,
            "absent_expected_relations": sorted(
                set(expected["tables"]) - {r["id"] for r in relations}
            ),
            "job_runs": runs,
            "queries": queries,
            "workgroup_state": group["state"],
            "review_reasons": reasons,
            "catalog_observed": True,
            "query_ownership_verified": False,
            "quiescence_and_catalog_observed": False,
            "deletion_authorized": False,
        }
        _save(output / "inventory.json", report, MAX_REPORT_BYTES)
        return report
    except BaseException as error:
        try:
            _save(
                output / "failure.json",
                {
                    "status": "service_inventory_failed",
                    "plan_sha256": plan_pin,
                    "stage": stage,
                    "api_calls": calls,
                    "records_retained": len(records),
                    "error_type": type(error).__name__,
                    "enumeration_complete": False,
                    "deletion_authorized": False,
                },
                MAX_RECORD_BYTES,
            )
        except Exception:
            pass
        raise
