"""Disable the dedicated workgroup and stop only receipt-bound executions; no deletion CLI."""

import hashlib
import time
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Protocol

from athena_control import ACTIVE as QUERY_ACTIVE
from athena_control import TERMINAL as QUERY_TERMINAL
from athena_control import canonical, write_once
from athena_read_plan import EXECUTION_CATALOG, same
from aws_login_bridge import BridgeError
from aws_role_session import LifetimeClient
from glue_control import ACTIVE as JOB_ACTIVE
from glue_control import TERMINAL as JOB_TERMINAL
from trial_cleanup_inventory import _save, validate_inventory_output
from trial_cleanup_operation import _saved_pin, create_service_client, prepare_operation
from trial_cleanup_session import cleanup_credentials, load_cleanup_plan
from trial_quiescence_plan import (
    MAX_OWN_CALLS,
    MAX_ROUNDS,
    POLL_SECONDS,
    STOP_SECONDS,
    QueryEvidence,
    Record,
    evidence_records,
    plan_sha256,
    quiescence_plan,
    recheck_evidence,
)
from trial_service_inventory import (
    ATHENA_READ_CALLS,
    GLUE_READ_CALLS,
    AthenaClient,
    GlueClient,
    inventory_services,
    workgroup_observation,
)

MAX_RECORD_BYTES = 64 * 1024
REQUIRED_SECONDS = 1800
READ_SECONDS = 90
GLUE_CALLS = (*GLUE_READ_CALLS, "get_job_run", "batch_stop_job_run")
ATHENA_CALLS = (*ATHENA_READ_CALLS, "update_work_group", "stop_query_execution")


class GlueStopClient(GlueClient, Protocol):
    def get_job_run(self, **kwargs: Any) -> dict[str, Any]: ...
    def batch_stop_job_run(self, **kwargs: Any) -> dict[str, Any]: ...


class AthenaStopClient(AthenaClient, Protocol):
    def update_work_group(self, **kwargs: Any) -> dict[str, Any]: ...
    def stop_query_execution(self, **kwargs: Any) -> dict[str, Any]: ...


def checked_plan(
    release: Path, pin: str, job: Record | None, queries: tuple[QueryEvidence, ...], expected: str
) -> dict[str, Any]:
    plan = quiescence_plan(release, pin, job, queries)
    if plan_sha256(plan) != expected:
        raise ValueError("Quiescence scope differs from its explicit plan pin")
    return plan


def check_output(
    release: Path, job: Record | None, queries: tuple[QueryEvidence, ...], output: Path
) -> None:
    validate_inventory_output(release, output)
    for record in evidence_records(job, queries):
        protected = record.path.parent.resolve()
        if output.resolve().is_relative_to(protected) or protected.is_relative_to(output.resolve()):
            raise ValueError("Keep fresh quiescence output separate from saved receipt directories")


def quiesce_services(
    glue: GlueStopClient,
    athena: AthenaStopClient,
    release: Path,
    pin: str,
    job: Record | None,
    queries: tuple[QueryEvidence, ...],
    expected_plan_sha256: str,
    output: Path,
    *,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Borrow clients; issue each change at most once and independently observe terminal states."""
    plan = checked_plan(release, pin, job, queries, expected_plan_sha256)
    check_output(release, job, queries, output)
    output.mkdir(parents=True)
    _save(output / "intent.json", plan, MAX_RECORD_BYTES)
    scope = plan["scope"]
    expected = scope["expected_services"]
    own_calls = 0
    changes: list[dict[str, Any]] = []
    observations: dict[str, str] = {}
    stage = "pre_inventory"
    deadline: float | None = None

    def unchanged() -> None:
        recheck_evidence(release, pin, job, queries)
        _saved_pin(output / "intent.json", plan, MAX_RECORD_BYTES)

    def request(method: Callable[..., dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
        nonlocal own_calls
        if own_calls >= MAX_OWN_CALLS or (deadline is not None and now() >= deadline):
            raise TimeoutError("Quiescence observation/call ceiling reached")
        own_calls += 1
        return method(**params)

    def observe_job() -> str:
        row = request(
            glue.get_job_run,
            {
                "JobName": expected["job"],
                "RunId": plan["job_run"]["id"],
                "PredecessorsIncluded": False,
            },
        )["JobRun"]
        if (
            row.get("Id") != plan["job_run"]["id"]
            or row.get("JobName") != expected["job"]
            or row.get("JobRunState") not in JOB_ACTIVE | JOB_TERMINAL
        ):
            raise ValueError("Bound Glue run identity/state differs")
        state = row["JobRunState"]
        _save(
            output / f"observation-{own_calls:02}.json",
            {"kind": "job", "id": row["Id"], "state": state},
            MAX_RECORD_BYTES,
        )
        return str(state)

    def observe_query(binding: dict[str, Any]) -> str:
        row = request(athena.get_query_execution, {"QueryExecutionId": binding["id"]})[
            "QueryExecution"
        ]
        sql = row.get("Query")
        state = row.get("Status", {}).get("State")
        if (
            row.get("QueryExecutionId") != binding["id"]
            or row.get("WorkGroup") != expected["workgroup"]
            or row.get("QueryExecutionContext")
            != {"Catalog": EXECUTION_CATALOG, "Database": expected["database"]}
            or not isinstance(sql, str)
            or len(sql.encode()) > 16384
            or hashlib.sha256(sql.encode()).hexdigest() != binding["query_sha256"]
            or state not in QUERY_ACTIVE | QUERY_TERMINAL
        ):
            raise ValueError("Bound query identity/SQL/context/state differs")
        _save(
            output / f"observation-{own_calls:02}.json",
            {
                "kind": "query",
                "id": binding["id"],
                "state": state,
                "query_sha256": binding["query_sha256"],
            },
            MAX_RECORD_BYTES,
        )
        return str(state)

    def change(label: str, method: Callable[..., dict[str, Any]], params: dict[str, Any]) -> None:
        unchanged()
        if deadline is not None and now() >= deadline:
            raise TimeoutError("Quiescence change deadline reached")
        index = len(changes) + 1
        _save(
            output / f"action-{index:02}-intent.json",
            {"plan_sha256": expected_plan_sha256, "action": label, "request": params},
            MAX_RECORD_BYTES,
        )
        result: dict[str, Any] = {"action": label, "acknowledged": False, "error_type": None}
        changes.append(result)
        try:
            response = request(method, params)
            result["acknowledged"] = (
                (
                    not response.get("Errors")
                    and response.get("SuccessfulSubmissions")
                    == [{"JobName": expected["job"], "JobRunId": plan["job_run"]["id"]}]
                )
                if label == "stop_job"
                else True
            )
        except Exception as error:
            result["error_type"] = type(error).__name__
        _save(output / f"action-{index:02}-response.json", result, MAX_RECORD_BYTES)

    try:
        before = inventory_services(glue, athena, release, pin, output / "before")
        unchanged()
        stage = "bound_identity_checks"
        if plan["job_run"] is not None:
            observations["job"] = observe_job()
        for binding in plan["queries"]:
            observations[binding["id"]] = observe_query(binding)
        deadline = now() + STOP_SECONDS
        stage = "disable_workgroup"
        if before["workgroup_state"] != "DISABLED":
            change("disable_workgroup", athena.update_work_group, plan["disable_request"])
        stage = "stop_owned_work"
        if plan["job_run"] is not None and observations["job"] in JOB_ACTIVE - {"STOPPING"}:
            change(
                "stop_job",
                glue.batch_stop_job_run,
                {"JobName": expected["job"], "JobRunIds": [plan["job_run"]["id"]]},
            )
        for binding in plan["queries"]:
            if observations[binding["id"]] in QUERY_ACTIVE:
                change(
                    "stop_query", athena.stop_query_execution, {"QueryExecutionId": binding["id"]}
                )
        stage = "confirm_stops"
        rounds = 0
        for number in range(MAX_ROUNDS):
            if now() >= deadline:
                break
            unchanged()
            rounds = number + 1
            group_ok = False
            try:
                group = workgroup_observation(
                    request(athena.get_work_group, {"WorkGroup": expected["workgroup"]}), scope
                )
                group_ok = group["state"] == "DISABLED"
                _save(output / f"workgroup-{number:02}.json", group, MAX_RECORD_BYTES)
                if plan["job_run"] is not None and observations["job"] not in JOB_TERMINAL:
                    observations["job"] = observe_job()
                for binding in plan["queries"]:
                    if observations[binding["id"]] not in QUERY_TERMINAL:
                        observations[binding["id"]] = observe_query(binding)
            except Exception as error:
                _save(
                    output / f"poll-error-{number:02}.json",
                    {"error_type": type(error).__name__},
                    MAX_RECORD_BYTES,
                )
                group_ok = False
            terminal = (plan["job_run"] is None or observations["job"] in JOB_TERMINAL) and all(
                observations[b["id"]] in QUERY_TERMINAL for b in plan["queries"]
            )
            if group_ok and terminal:
                break
            if number + 1 < MAX_ROUNDS and now() < deadline:
                pause(min(POLL_SECONDS, max(0, deadline - now())))
        deadline = None  # Post-inventory has its own existing fixed request/page ceilings.
        stage = "post_inventory"
        unchanged()
        after = inventory_services(glue, athena, release, pin, output / "after")
        unchanged()
        job_ids = set() if plan["job_run"] is None else {plan["job_run"]["id"]}
        query_ids = {b["id"] for b in plan["queries"]}
        unbound_jobs = sorted({r["id"] for r in after["job_runs"]} - job_ids)
        unbound_queries = sorted({q["id"] for q in after["queries"]} - query_ids)
        missing_jobs = sorted(job_ids - {r["id"] for r in after["job_runs"]})
        missing_queries = sorted(query_ids - {q["id"] for q in after["queries"]})
        quiet = (
            after["workgroup_state"] == "DISABLED"
            and not any(r["active"] for r in after["job_runs"])
            and not any(q["active"] for q in after["queries"])
        )
        bound_terminal = (plan["job_run"] is None or observations["job"] in JOB_TERMINAL) and all(
            observations[b["id"]] in QUERY_TERMINAL for b in plan["queries"]
        )
        requires_review = bool(
            after["review_reasons"]
            or unbound_jobs
            or unbound_queries
            or missing_jobs
            or missing_queries
            or not bound_terminal
        )
        report = {
            "version": 1,
            "status": "quiescence_requires_review" if requires_review else "quiescence_observed",
            "plan_sha256": expected_plan_sha256,
            "release_manifest_sha256": pin,
            "changes": changes,
            "owned_terminal_observed": bound_terminal,
            "quiescence_observed": quiet and bound_terminal,
            "catalog_observed": True,
            "quiescence_and_catalog_observed": quiet and bound_terminal,
            "unbound_job_runs": unbound_jobs,
            "unbound_queries": unbound_queries,
            "missing_bound_job_runs": missing_jobs,
            "missing_bound_queries": missing_queries,
            "requires_review": requires_review,
            "poll_rounds": rounds,
            "api_calls": before["api_calls"] + own_calls + after["api_calls"],
            "before_inventory_sha256": _saved_pin(
                output / "before/inventory.json", before, 512 * 1024
            ),
            "after_inventory_sha256": _saved_pin(
                output / "after/inventory.json", after, 512 * 1024
            ),
            "future_job_submissions_fenced": False,
            "deletion_authorized": False,
        }
        _save(output / "receipt.json", report, MAX_RECORD_BYTES)
        return report
    except BaseException as error:
        try:
            _save(
                output / "failure.json",
                {
                    "status": "quiescence_failed",
                    "stage": stage,
                    "plan_sha256": expected_plan_sha256,
                    "error_type": type(error).__name__,
                    "own_calls": own_calls,
                    "changes_attempted": len(changes),
                    "deletion_authorized": False,
                },
                MAX_RECORD_BYTES,
            )
        except Exception:
            pass
        raise


def run_quiescence(
    release: Path,
    release_pin: str,
    inventory_plan: Path,
    inventory_pin: str,
    principal_arn: str,
    content: bytes,
    *,
    job: Record | None,
    queries: tuple[QueryEvidence, ...],
    expected_plan_sha256: str,
    output: Path,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Connect one scoped session; no CLI, automatic retry, S3 client or data deletion."""
    prepared, _ = prepare_operation(
        release, release_pin, inventory_plan, inventory_pin, principal_arn, output
    )
    checked_plan(release, release_pin, job, queries, expected_plan_sha256)
    check_output(release, job, queries, output)
    acquisition = cleanup_credentials(
        release, release_pin, inventory_plan, inventory_pin, principal_arn, content
    )
    content = b""
    with acquisition as snapshot:
        snapshot.require_operation(REQUIRED_SECONDS)
        with ExitStack() as owned:
            clients = {}
            for service, calls in (("glue", GLUE_CALLS), ("athena", ATHENA_CALLS)):
                raw = create_service_client(snapshot, service)
                owned.callback(raw.close)
                clients[service] = LifetimeClient(
                    raw, snapshot, {name: READ_SECONDS for name in calls}
                )
            report = quiesce_services(
                clients["glue"],
                clients["athena"],
                release,
                release_pin,
                job,
                queries,
                expected_plan_sha256,
                output / "attempt",
                now=now,
                pause=pause,
            )
            snapshot.require_operation(READ_SECONDS)
            if not same(
                load_cleanup_plan(release, release_pin, inventory_plan, inventory_pin), prepared
            ):
                raise BridgeError("cleanup_preparation_changed")
            recheck_evidence(release, release_pin, job, queries)
    completion = {
        "version": 1,
        "status": "scoped_trial_quiescence_complete",
        "plan_sha256": expected_plan_sha256,
        "inventory_plan_sha256": inventory_pin,
        "receipt_sha256": _saved_pin(output / "attempt/receipt.json", report, MAX_RECORD_BYTES),
        "quiescence_observed": report["quiescence_observed"],
        "requires_review": report["requires_review"],
        "deletion_authorized": False,
    }
    write_once(output / "session-complete.json", canonical(completion))
    return completion
