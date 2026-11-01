"""Durable Athena attempt records; no execution CLI or credential/client construction."""

import hashlib
import json
import os
import re
import secrets
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from athena_read_plan import ARTIFACTS, preflight_workgroup, read_plan, same


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def verified_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Check plan integrity against the shared recipe; this does not prove provenance."""
    try:
        expected = read_plan(
            **plan["target"],
            snapshot_id=plan["binding"]["snapshot_id"],
            source_run_sha256=plan["binding"]["source_run_sha256"],
            max_query_scan_bytes=plan["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"],
        )
    except (KeyError, TypeError) as exc:
        raise ValueError("Malformed Athena preparation") from exc
    if not same(plan, expected):
        raise ValueError("Athena preparation differs from the fixed query recipe")
    return expected


def write_once(path: Path, content: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


class Attempt:
    """Persist one new token and exact request before any API call; never overwrite."""

    def __init__(self, output: Path, plan: dict[str, Any]) -> None:
        checked = verified_plan(plan)
        resolved, artifacts = output.resolve(), ARTIFACTS.resolve()
        if (
            output.exists()
            or output.is_symlink()
            or resolved == artifacts
            or not resolved.is_relative_to(artifacts)
        ):
            raise ValueError("Use a fresh Athena attempt directory under ignored artifacts")
        request = checked["request_template"] | {"ClientRequestToken": secrets.token_hex(32)}
        self.output = resolved
        self._content = canonical({"contract_version": 1, "plan": checked, "request": request})
        self.intent_sha256 = hashlib.sha256(self._content).hexdigest()
        resolved.mkdir(parents=True)
        write_once(resolved / "intent.json", self._content)
        self.record({"event": "attempt_prepared", "intent_sha256": self.intent_sha256})

    def intent(self) -> dict[str, Any]:
        path = self.output / "intent.json"
        # A changed record must never lead to a changed query or a replacement token.
        if path.is_symlink() or path.stat().st_size != len(self._content):
            raise ValueError("Athena attempt intent changed")
        if path.read_bytes() != self._content:
            raise ValueError("Athena attempt intent changed")
        return json.loads(self._content)

    def record(self, event: dict[str, Any]) -> None:
        row = event | {"recorded_at_utc": datetime.now(UTC).isoformat()}
        with (self.output / "events.jsonl").open("ab") as stream:
            stream.write(canonical(row))
            stream.flush()
            os.fsync(stream.fileno())

    def receipt(self, value: dict[str, Any]) -> None:
        write_once(self.output / "receipt.json", canonical(value))


POLL_SECONDS = 5
WATCH_SECONDS = 300
MAX_OBSERVATIONS = 60
STOP_SECONDS = 60
MAX_STOP_OBSERVATIONS = 12
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}
ACTIVE = {"QUEUED", "RUNNING"}


class QueryClient(Protocol):
    def get_work_group(self, **kwargs: Any) -> dict[str, Any]: ...
    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]: ...
    def stop_query_execution(self, **kwargs: Any) -> dict[str, Any]: ...


class SubmissionUnknown(RuntimeError):
    """Retain this attempt/token for separately approved reconciliation; never start anew."""


def observation(client: QueryClient, request: dict[str, Any], query_id: str) -> dict[str, Any]:
    query = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
    if query.get("QueryExecutionId") != query_id:
        raise ValueError("Athena observation is for another query ID")
    state = query.get("Status", {}).get("State")
    if state not in TERMINAL | ACTIVE:
        raise ValueError("Unknown Athena query state")
    drift, missing = [], []

    def check(label: str, actual: Any, expected: Any) -> None:
        if actual is None:
            missing.append(label)
        elif not same(actual, expected):
            drift.append(label)

    check("Query", query.get("Query"), request["QueryString"])
    check("WorkGroup", query.get("WorkGroup"), request["WorkGroup"])
    check(
        "QueryExecutionContext",
        query.get("QueryExecutionContext"),
        request["QueryExecutionContext"],
    )
    check("StatementType", query.get("StatementType"), "DML")
    check(
        "EngineVersion",
        query.get("EngineVersion"),
        {
            "SelectedEngineVersion": "Athena engine version 3",
            "EffectiveEngineVersion": "Athena engine version 3",
        },
    )
    expected_results = request["ResultConfiguration"] | {
        "OutputLocation": request["ResultConfiguration"]["OutputLocation"] + query_id + ".csv"
    }
    actual_results = query.get("ResultConfiguration")
    if (
        state in ACTIVE
        and actual_results is not None
        and actual_results.get("OutputLocation") == request["ResultConfiguration"]["OutputLocation"]
    ):
        missing.append("result_object_location")
        actual_results = actual_results | {"OutputLocation": expected_results["OutputLocation"]}
    check("ResultConfiguration", actual_results, expected_results)
    # Disabled reuse can still carry an irrelevant SDK/service default MaxAgeInMinutes.
    reuse = query.get("ResultReuseConfiguration", {}).get("ResultReuseByAgeConfiguration", {})
    check("result_reuse_setting", reuse.get("Enabled"), False)
    stats = query.get("Statistics", {})
    check(
        "reused_previous_result",
        stats.get("ResultReuseInformation", {}).get("ReusedPreviousResult"),
        False,
    )
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
        or query.get("SubstatementType", "SELECT") != "SELECT"
        or stats.get("DataManifestLocation")
    ):
        drift.append("unexpected_execution_mode")
    return {"state": state, "drift": drift, "missing": missing, "data_scanned_bytes": scanned}


def _execute_attempt(
    client: QueryClient,
    attempt: Attempt,
    *,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Monitor an injected client; future live use needs identity/provenance gates."""
    intent = attempt.intent()
    plan, request = intent["plan"], intent["request"]
    attempt.record({"event": "preflight_started"})
    checked = preflight_workgroup(client, plan)
    attempt.record({"event": "preflight_complete", **checked})
    attempt.intent()  # Detect disk changes during preflight before reserving the one submission.
    write_once(
        attempt.output / "submission.json", canonical({"intent_sha256": attempt.intent_sha256})
    )
    attempt.record({"event": "submission_intent", "intent_sha256": attempt.intent_sha256})
    started = now()
    try:
        response = client.start_query_execution(**request)
        query_id = response.get("QueryExecutionId")
        if not isinstance(query_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query_id) is None:
            raise ValueError("Submission returned no usable query ID")
    except BaseException as exc:
        try:
            attempt.record({"event": "submission_unknown", "error_type": type(exc).__name__})
        except Exception:
            pass
        raise SubmissionUnknown(
            "Keep the saved request/token; reconcile this attempt before any new submission"
        ) from exc

    return monitor_query(
        client, attempt, query_id, intent=intent, started=started, now=now, pause=pause
    )


def monitor_query(
    client: QueryClient,
    attempt: Attempt,
    query_id: str,
    *,
    intent: dict[str, Any],
    started: float,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
    stop_reason: str | None = None,
) -> dict[str, Any]:
    """Monitor a service-returned ID using the request captured before submission."""
    plan, request = intent["plan"], intent["request"]
    stop_attempted, stop_acknowledged = False, False
    stop_deadline: float | None = None
    stop_observations = 0
    journal_errors: list[str] = []
    last: dict[str, Any] | None = None
    cap = plan["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"]

    def safe_record(event: dict[str, Any]) -> None:
        try:
            attempt.record(event)
        except Exception as exc:
            journal_errors.append(type(exc).__name__)

    def problem(observed: dict[str, Any]) -> str | None:
        scanned = observed["data_scanned_bytes"]
        if scanned is not None and scanned > cap:
            return "scan_limit_exceeded"
        if observed["drift"] or (observed["state"] == "SUCCEEDED" and observed["missing"]):
            return "execution_contract_unverified"
        return None

    def finish(reason: str | None) -> dict[str, Any]:
        terminal = last is not None and last["state"] in TERMINAL
        outcome = "stop_unconfirmed"
        if terminal:
            outcome = (
                reason
                or (problem(last) if last is not None else None)
                or (
                    "query_succeeded"
                    if last is not None and last["state"] == "SUCCEEDED"
                    else "query_failed"
                )
            )
        return {
            "outcome": outcome,
            "reason": reason,
            "query_id": query_id,
            "intent_sha256": attempt.intent_sha256,
            "query_sha256": plan["query_sha256"],
            "terminal_confirmed": terminal,
            "last_observation": last,
            "stop_attempted": stop_attempted,
            "stop_acknowledged": stop_acknowledged,
            "journal_errors": journal_errors,
            "caller_identity_verified": False,
            "provenance_verified": False,
            "result_verified": False,
        }

    def stop_and_confirm(reason: str) -> dict[str, Any]:
        nonlocal stop_attempted, stop_acknowledged, last, stop_deadline, stop_observations
        if last is not None and last["state"] in TERMINAL:
            return finish(reason)
        if stop_deadline is None:
            stop_deadline = now() + STOP_SECONDS
        if not stop_attempted:
            safe_record({"event": "stop_intent", "query_id": query_id, "reason": reason})
            stop_attempted = True
            try:
                client.stop_query_execution(QueryExecutionId=query_id)
                stop_acknowledged = True  # API acceptance alone never proves termination.
                safe_record({"event": "stop_acknowledged", "query_id": query_id})
            except Exception as exc:
                safe_record({"event": "stop_error", "error_type": type(exc).__name__})
        while stop_observations < MAX_STOP_OBSERVATIONS and now() < stop_deadline:
            stop_observations += 1
            try:
                last = observation(client, request, query_id)
                safe_record({"event": "stop_observation", "query_id": query_id, **last})
                if last["state"] in TERMINAL:
                    return finish(reason)
            except Exception as exc:
                safe_record({"event": "stop_observation_error", "error_type": type(exc).__name__})
            pause(min(POLL_SECONDS, max(0, stop_deadline - now())))
        return finish(reason)

    try:
        write_once(attempt.output / "query.json", canonical({"query_id": query_id}))
        attempt.record({"event": "query_identified", "query_id": query_id})
        if stop_reason is not None:
            stop_deadline = now() + STOP_SECONDS
            try:
                last = observation(client, request, query_id)
                attempt.record({"event": "recovery_observation", "query_id": query_id, **last})
            except Exception as exc:
                attempt.record(
                    {"event": "recovery_observation_error", "error_type": type(exc).__name__}
                )
            return stop_and_confirm(stop_reason)
        for _ in range(MAX_OBSERVATIONS):
            if now() - started >= WATCH_SECONDS:
                return stop_and_confirm("watchdog_limit")
            try:
                last = observation(client, request, query_id)
            except Exception as exc:
                attempt.record({"event": "observation_error", "error_type": type(exc).__name__})
                return stop_and_confirm("observation_failed")
            attempt.record({"event": "observation", "query_id": query_id, **last})
            reason = problem(last)
            if now() - started >= WATCH_SECONDS:
                reason = reason or "watchdog_limit"
            if last["state"] in TERMINAL:
                return finish(reason)
            if reason is not None:
                return stop_and_confirm(reason)
            pause(min(POLL_SECONDS, max(0, WATCH_SECONDS - (now() - started))))
        return stop_and_confirm("watchdog_limit")
    except BaseException as exc:
        result = stop_and_confirm("controller_interrupted")
        exc.add_note(
            f"Owned Athena query {query_id}; terminal confirmed: {result['terminal_confirmed']}"
        )
        raise


@contextmanager
def controller_lease(directory: Path) -> Iterator[None]:
    """One live controller/recovery per original attempt; kernel releases on process exit."""
    directory = directory.resolve()
    if (
        not directory.is_dir()
        or not directory.is_relative_to(ARTIFACTS.resolve())
        or directory == ARTIFACTS.resolve()
    ):
        raise ValueError("Controller lease requires an existing attempt under artifacts")
    try:
        import fcntl
    except ImportError as exc:
        raise RuntimeError("Use the existing Linux Docker controller runtime") from exc
    lock = directory / ".controller.lock"
    if lock.is_symlink():
        raise ValueError("Controller lock must not be a symlink")
    with lock.open("a+b") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("This attempt already has an active controller or recovery") from exc
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def execute_attempt(
    client: QueryClient,
    attempt: Attempt,
    *,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """One initial submission under the same lease used by explicit recovery."""
    with controller_lease(attempt.output):
        return _execute_attempt(client, attempt, now=now, pause=pause)
