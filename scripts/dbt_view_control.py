"""Bound two ordered dbt view submissions with injected clients; no credentials or live CLI."""

import copy
import hashlib
import json
import re
import secrets
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

from athena_control import (
    ACTIVE,
    TERMINAL,
    Attempt,
    QueryClient,
    SubmissionUnknown,
    canonical,
    controller_lease,
    monitor_query,
    write_once,
)
from athena_read_plan import preflight_workgroup, same
from dbt_view_plan import normalized_sql, view_build_plan
from prepare_dbt_project import ARTIFACTS


def ddl_observation(
    response: dict[str, Any], request: dict[str, Any], query_id: str
) -> dict[str, Any]:
    query = response["QueryExecution"]
    state = query.get("Status", {}).get("State")
    if query.get("QueryExecutionId") != query_id or state not in ACTIVE | TERMINAL:
        raise ValueError("Unexpected view query identity/state")
    drift: list[str] = []
    missing: list[str] = []

    def check(name: str, actual: Any, expected: Any) -> None:
        if actual is None:
            missing.append(name)
        elif not same(actual, expected):
            drift.append(name)

    for key in ("Query", "WorkGroup", "QueryExecutionContext"):
        check(key, query.get(key), request["QueryString" if key == "Query" else key])
    check("StatementType", query.get("StatementType"), "DDL")
    check(
        "EngineVersion",
        query.get("EngineVersion"),
        {
            "SelectedEngineVersion": "Athena engine version 3",
            "EffectiveEngineVersion": "Athena engine version 3",
        },
    )
    expected = request["ResultConfiguration"] | {
        "OutputLocation": request["ResultConfiguration"]["OutputLocation"] + query_id + ".txt"
    }
    actual = query.get("ResultConfiguration")
    if (
        state in ACTIVE
        and actual is not None
        and actual.get("OutputLocation") == request["ResultConfiguration"]["OutputLocation"]
    ):
        missing.append("result_object_location")
        actual = actual | {"OutputLocation": expected["OutputLocation"]}
    check("ResultConfiguration", actual, expected)
    # Reuse is disabled in our exact request. DDL is not a reusable SELECT;
    # optional service reuse fields, when returned, must still indicate no reuse.
    if "ResultReuseConfiguration" in query:
        check(
            "reuse",
            query["ResultReuseConfiguration"]
            .get("ResultReuseByAgeConfiguration", {})
            .get("Enabled"),
            False,
        )
    stats = query.get("Statistics", {})
    if "ResultReuseInformation" in stats:
        check("reused", stats["ResultReuseInformation"].get("ReusedPreviousResult"), False)
    scanned = stats.get("DataScannedInBytes")
    if scanned is None:
        missing.append("DataScannedInBytes")
    elif type(scanned) is not int or scanned < 0:
        drift.append("DataScannedInBytes")
        scanned = None
    if (
        query.get("ManagedQueryResultsConfiguration", {}).get("Enabled", False) is not False
        or query.get("QueryResultsS3AccessGrantsConfiguration", {}).get(
            "EnableS3AccessGrants", False
        )
        is not False
        or query.get("ExecutionParameters")
        or stats.get("DataManifestLocation")
    ):
        drift.append("unexpected_execution_mode")
    return {"state": state, "drift": drift, "missing": missing, "data_scanned_bytes": scanned}


class ViewClient(QueryClient, Protocol):
    def get_query_results(self, **kwargs: Any) -> dict[str, Any]: ...


class ViewAttempt(Attempt):
    """Share durable journal primitives, with a separately constrained DDL intent."""

    def __init__(
        self, output: Path, root_pin: str, plan: dict[str, Any], request: dict[str, Any]
    ) -> None:
        self.output = output
        self._content = canonical(
            {
                "contract_version": 1,
                "parent_intent_sha256": root_pin,
                "plan": plan,
                "request": request,
            }
        )
        self.intent_sha256 = hashlib.sha256(self._content).hexdigest()
        output.mkdir()
        write_once(output / "intent.json", self._content)
        self.record({"event": "view_attempt_prepared"})


class ViewController:
    """An injected-client boundary; its caller owns identity/provenance and client cleanup."""

    def __init__(
        self,
        client: ViewClient,
        project: Path,
        output: Path,
        *,
        deployment_name: str,
        account_id: str,
        region: str = "eu-west-1",
        now: Callable[[], float] = time.monotonic,
        pause: Callable[[float], None] = time.sleep,
        before_submit: Callable[[], None] | None = None,
    ) -> None:
        plan = self.make_plan(
            project, deployment_name=deployment_name, account_id=account_id, region=region
        )
        resolved = output.resolve()
        if (
            output.exists()
            or output.is_symlink()
            or resolved == ARTIFACTS.resolve()
            or not resolved.is_relative_to(ARTIFACTS.resolve())
            or resolved.is_relative_to(project.resolve())
            or project.resolve().is_relative_to(resolved)
        ):
            raise ValueError("Use fresh control output under artifacts, separate from the project")
        self.client, self.project, self.output = client, project, resolved
        self.target = dict(deployment_name=deployment_name, account_id=account_id, region=region)
        self.now, self.pause = now, pause
        self._before_submit = before_submit
        self._content = canonical(plan)
        self.intent_sha256 = hashlib.sha256(self._content).hexdigest()
        self.deadline = now() + plan["limits"]["wall_seconds"]
        self._active = self._closed = self._blocked = False
        self._steps: list[dict[str, Any]] = []
        self._responses: dict[str, dict[str, Any]] = {}
        self._reads: set[tuple[str, str]] = set()
        self._scanned = 0
        resolved.mkdir(parents=True)
        write_once(resolved / "intent.json", self._content)

    def make_plan(self, project: Path, **target: str) -> dict[str, Any]:
        return view_build_plan(project, **target)

    def select_recipe(self, plan: dict[str, Any], sql: str, index: int) -> dict[str, Any]:
        recipe: dict[str, Any] = plan["queries"][index]
        if normalized_sql(sql) != normalized_sql(recipe["sql"]):
            raise ValueError("dbt query differs from the next fixed view recipe")
        return recipe

    def inspect_execution(
        self, raw: dict[str, Any], request: dict[str, Any], identity: str
    ) -> dict[str, Any]:
        return ddl_observation(raw, request, identity)

    def plan(self) -> dict[str, Any]:
        path = self.output / "intent.json"
        if (
            path.is_symlink()
            or path.stat().st_size != len(self._content)
            or path.read_bytes() != self._content
        ):
            raise ValueError("dbt control intent changed")
        return json.loads(self._content)

    def _unchanged(self) -> dict[str, Any]:
        plan = self.plan()
        if not same(self.make_plan(self.project, **self.target), plan):
            raise ValueError("Prepared project or view recipe changed")
        return plan

    def _gate(self) -> dict[str, Any]:
        if not self._active or self._closed or self._blocked:
            raise RuntimeError("View control is inactive or blocked; never resubmit")
        plan = self._unchanged()
        if self.now() >= self.deadline:
            raise RuntimeError("dbt view-run deadline reached")
        return plan

    @contextmanager
    def session(self) -> Iterator["ViewController"]:
        if self._active or self._closed:
            raise RuntimeError("A view controller session can only be entered once")
        with controller_lease(self.output):
            self._active = True
            try:
                yield self
            except BaseException:
                self._blocked = True
                raise
            finally:
                self._active, self._closed = False, True
                try:
                    self._unchanged()
                    if self.now() >= self.deadline:
                        self._blocked = True
                except Exception:
                    self._blocked = True
                write_once(self.output / "receipt.json", canonical(self.report()))

    def report(self) -> dict[str, Any]:
        complete = len(self._steps) == 2 and all(
            s.get("outcome") == "query_succeeded" for s in self._steps
        )
        return {
            "status": "view_queries_completed"
            if complete and not self._blocked
            else "view_queries_not_verified",
            "intent_sha256": self.intent_sha256,
            "queries_reserved": len(self._steps),
            "data_scanned_bytes": self._scanned,
            "blocked": self._blocked,
            "queries": self._steps,
            "result_verified": False,
            "caller_identity_verified": False,
            "source_readback_verified": False,
            "scope": (
                "Query ownership and observed DDL outcomes only; not dbt/catalog/Gold acceptance"
            ),
        }

    def start_query_execution(self, **params: Any) -> dict[str, Any]:
        try:
            plan = self._gate()
            index = len(self._steps)
            if index >= plan["limits"]["max_queries"]:
                raise ValueError("The dbt query submission ceiling has been reached")
            source = plan["source_plan"]
            expected = {
                "QueryString": params.get("QueryString"),
                "WorkGroup": source["workgroup"]["Name"],
                "QueryExecutionContext": source["request_template"]["QueryExecutionContext"],
                "ResultConfiguration": {
                    "OutputLocation": source["request_template"]["ResultConfiguration"][
                        "OutputLocation"
                    ]
                },
            }
            if (
                params != expected
                or not isinstance(params.get("QueryString"), str)
                or len(params["QueryString"].encode()) > plan["limits"]["max_query_bytes"]
            ):
                raise ValueError("dbt query differs from the next fixed view recipe")
            recipe = self.select_recipe(plan, params["QueryString"], index)
            cap = source["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"]
            # Reserve a whole per-query allowance before admitting another query.
            # This two-DDL plan expects zero scans; any positive first scan leaves
            # insufficient room for a second full allowance in the 100 MiB total.
            if self._scanned + cap > plan["limits"]["max_total_scan_bytes"]:
                raise RuntimeError("Insufficient remaining cumulative scan allowance")
            preflight_workgroup(self.client, source)
            if self._before_submit is not None:
                self._before_submit()
            self._gate()  # Includes project/clock drift during workgroup preflight.
            request = {
                **params,
                "ResultConfiguration": source["request_template"]["ResultConfiguration"],
                "ResultReuseConfiguration": source["request_template"]["ResultReuseConfiguration"],
                "ClientRequestToken": secrets.token_hex(32),
            }
            query_plan = {
                "query_sha256": hashlib.sha256(params["QueryString"].encode()).hexdigest(),
                "workgroup": source["workgroup"],
            }
            attempt = ViewAttempt(
                self.output / f"query-{index + 1:02}", self.intent_sha256, query_plan, request
            )
            step: dict[str, Any] = {
                "name": recipe["name"],
                "intent_sha256": attempt.intent_sha256,
            }
            self._steps.append(step)
            write_once(
                attempt.output / "submission.json",
                canonical({"intent_sha256": attempt.intent_sha256}),
            )
            attempt.record({"event": "submission_intent"})
            saved_intent = attempt.intent()
            started = self.now()
            try:
                response = self.client.start_query_execution(**request)
                query_id = response.get("QueryExecutionId")
                if (
                    not isinstance(query_id, str)
                    or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query_id) is None
                ):
                    raise ValueError("Submission returned no usable view query ID")
                if query_id in self._responses:
                    raise ValueError("Submission reused an already owned view query ID")
            except BaseException as error:
                step["outcome"] = "submission_unknown"
                attempt.record({"event": "submission_unknown", "error_type": type(error).__name__})
                raise SubmissionUnknown(
                    "Retain this view request/token; do not start a replacement"
                ) from error
            highest_scan = 0

            def observe(
                client: QueryClient, saved: dict[str, Any], identity: str
            ) -> dict[str, Any]:
                nonlocal highest_scan
                raw = client.get_query_execution(QueryExecutionId=identity)
                checked = self.inspect_execution(raw, saved, identity)
                scanned = checked["data_scanned_bytes"]
                if scanned is not None:
                    if scanned < highest_scan:
                        checked["drift"].append("scan_counter_regressed")
                    highest_scan = max(highest_scan, scanned)
                    if self._scanned + highest_scan > plan["limits"]["max_total_scan_bytes"]:
                        checked["drift"].append("cumulative_scan_limit")
                self._responses[identity] = copy.deepcopy(raw)
                return checked

            try:
                execution = monitor_query(
                    self.client,
                    attempt,
                    query_id,
                    intent=saved_intent,
                    started=started,
                    now=self.now,
                    pause=self.pause,
                    observer=observe,
                    deadline=self.deadline,
                )
            finally:
                self._scanned += highest_scan
            step.update(execution)
            attempt.receipt(execution)
            attempt.intent()
            self._unchanged()
            if execution["outcome"] != "query_succeeded":
                raise RuntimeError("View query was not verified: " + execution["outcome"])
            return {"QueryExecutionId": query_id}
        except BaseException as error:
            self._blocked = True
            try:
                write_once(
                    self.output / "failure.json", canonical({"error_type": type(error).__name__})
                )
            except Exception:
                pass
            raise

    def _owned(self, operation: str, params: dict[str, Any]) -> str:
        try:
            self._gate()
            identity = params.get("QueryExecutionId")
            if (
                params != {"QueryExecutionId": identity}
                or not isinstance(identity, str)
                or identity not in self._responses
                or (identity, operation) in self._reads
            ):
                raise ValueError("Unexpected or repeated adapter query-result request")
            self._reads.add((identity, operation))
            return identity
        except BaseException:
            self._blocked = True
            raise

    def get_query_execution(self, **params: Any) -> dict[str, Any]:
        identity = self._owned("execution", params)
        # Monitoring already reached and checked terminal success. Do not poll again
        # through the adapter's separate, otherwise unbounded polling loop.
        return copy.deepcopy(self._responses[identity])

    def get_query_results(self, **params: Any) -> dict[str, Any]:
        identity = self._owned("results", params)
        try:
            response = self.client.get_query_results(QueryExecutionId=identity, MaxResults=1)
            if (
                len(canonical(response)) > 65536
                or response.get("NextToken")
                or response.get("ResultSet", {}).get("Rows", [])
            ):
                raise ValueError("Unexpected or oversized view DDL result response")
            self._gate()
            return response
        except BaseException:
            self._blocked = True
            raise
