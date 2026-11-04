"""Execute only a pinned disposable-trial selection; all live use needs external approval."""

import re
import time
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from athena_runner import pinned_record
from trial_cleanup_inventory import _save, inventory_s3
from trial_cleanup_operation import (
    READ_CALLS,
    _saved_pin,
    create_s3_client,
    create_service_client,
    prepare_operation,
)
from trial_cleanup_plan import (
    BATCH_SIZE,
    EXECUTION_SECONDS,
    MAX_CALLS,
    MAX_PLAN_BYTES,
    CleanupEvidence,
    check_output,
    context,
    load_approved,
    prepare_cleanup,
    recheck,
    remaining,
    require_paused,
    require_quiet,
    snapshot,
    table_action,
)
from trial_cleanup_session import cleanup_credentials
from trial_quiescence_plan import Record
from trial_service_inventory import ATHENA_READ_CALLS, GLUE_READ_CALLS, inventory_services

S3_CALLS = (*READ_CALLS, "delete_object", "abort_multipart_upload", "list_parts")
GLUE_CALLS = (*GLUE_READ_CALLS, "get_table", "delete_table")
REQUIRED_SECONDS = 1800
PREPARATION_SECONDS = 1500


class _Budget:
    def __init__(self, now: Callable[[], float]) -> None:
        self.now = now
        self.deadline = now() + EXECUTION_SECONDS
        self.calls = 0

    def check_time(self) -> None:
        if self.now() >= self.deadline:
            raise TimeoutError("Cleanup execution deadline reached")

    def call(self, method: Callable[..., Any], params: dict[str, Any]) -> Any:
        self.check_time()
        if self.calls >= MAX_CALLS:
            raise TimeoutError("Cleanup API ceiling reached")
        self.calls += 1
        return method(**params)


class _Client:
    def __init__(self, client: Any, calls: tuple[str, ...], budget: _Budget) -> None:
        self.client, self.calls, self.budget = client, calls, budget

    def __getattr__(self, name: str) -> Any:
        if name not in self.calls:
            raise AttributeError(name)
        return lambda **params: self.budget.call(getattr(self.client, name), params)


def _error_code(error: BaseException) -> str | None:
    response = getattr(error, "response", None)
    value = (
        response.get("Error", {}).get("Code")
        if isinstance(response, dict) and isinstance(response.get("Error"), dict)
        else None
    )
    return (
        value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value) else None
    )


def _parts(s3: Any, action: dict[str, Any], owner: str) -> dict[str, Any]:
    params = {
        "Bucket": action["bucket"],
        "Key": action["key"],
        "UploadId": action["upload_id"],
        "ExpectedBucketOwner": owner,
        "MaxParts": 1,
    }
    try:
        value = s3.list_parts(**params)
        valid = (
            value.get("Bucket") == params["Bucket"]
            and value.get("Key") == params["Key"]
            and value.get("UploadId") == params["UploadId"]
        )
        empty = valid and value.get("IsTruncated") is False and value.get("Parts", []) == []
        return {
            "upload_id": action["upload_id"],
            "parts_empty_observed": empty,
            "upload_absent": False,
            "error_type": None,
            "error_code": None,
        }
    except Exception as error:
        code = _error_code(error)
        response = getattr(error, "response", {})
        absent = (
            code == "NoSuchUpload"
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
        )
        return {
            "upload_id": action["upload_id"],
            "parts_empty_observed": absent,
            "upload_absent": absent,
            "error_type": None if absent else type(error).__name__,
            "error_code": None if absent else code,
        }


def _request(action: dict[str, Any], ctx: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    scope = ctx["scope"]
    owner = scope["target"]["account_id"]
    kind = action["kind"]
    if kind == "table":
        return "delete_table", {
            "CatalogId": owner,
            "DatabaseName": scope["expected_services"]["database"],
            "Name": action["name"],
        }
    base = {"Bucket": action["bucket"], "Key": action["key"], "ExpectedBucketOwner": owner}
    if kind == "multipart_upload":
        return "abort_multipart_upload", base | {"UploadId": action["upload_id"]}
    if kind in {"version", "delete_marker"}:
        return "delete_object", base | {"VersionId": action["version_id"]}
    if kind == "object":
        return "delete_object", base | {"IfMatch": action["etag"]}
    raise ValueError("Unsupported exact cleanup action")


def execute_cleanup(
    glue: Any,
    athena: Any,
    s3: Any,
    release: Path,
    pin: str,
    evidence: CleanupEvidence,
    approved: Record,
    output: Path,
    *,
    coordinator_paused: bool = False,
    resume: bool = False,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """An explicit resume uses only unchanged remaining approved items; never expands scope."""
    require_paused(coordinator_paused)
    if type(resume) is not bool:
        raise ValueError("resume must be an explicit boolean")
    budget = _Budget(now)
    plan = load_approved(release, pin, evidence, approved)
    check_output(release, evidence, output, approved)
    ctx = plan["context"]
    glue, athena, s3 = (
        _Client(glue, GLUE_CALLS, budget),
        _Client(athena, ATHENA_READ_CALLS, budget),
        _Client(s3, S3_CALLS, budget),
    )
    output.mkdir(parents=True)
    _save(
        output / "intent.json",
        {
            "approved_plan_sha256": approved.sha256,
            "resume": resume,
            "coordinator_paused_assertion": True,
        },
        MAX_PLAN_BYTES,
    )
    changes: list[dict[str, Any]] = []
    stage = "fresh_selection"
    try:
        _, _, fresh = snapshot(glue, athena, s3, release, pin, ctx, output / "before")
        actions = remaining(plan, fresh, resume)
        for index, action in enumerate(actions):
            stage = "guard"
            if index % BATCH_SIZE == 0:
                recheck(release, pin, evidence)
                pinned_record(approved.path, approved.sha256)
                guard = inventory_services(
                    glue, athena, release, pin, output / f"guard-{index // BATCH_SIZE:02}"
                )
                require_quiet(guard, ctx)
            stage = "action_identity"
            if action["kind"] == "table" and canonical(
                table_action(glue, ctx, action["name"])
            ) != canonical(action):
                raise ValueError("Catalog generation changed before its delete request")
            recheck(release, pin, evidence)
            pinned_record(approved.path, approved.sha256)
            method, params = _request(action, ctx)
            stage = "delete_or_abort"
            _save(
                output / f"action-{index:02}-intent.json",
                {
                    "approved_plan_sha256": approved.sha256,
                    "action": action,
                    "method": method,
                    "request": params,
                },
                MAX_PLAN_BYTES,
            )
            result = {
                "action": action,
                "acknowledged": False,
                "error_type": None,
                "error_code": None,
            }
            changes.append(result)
            try:
                response = getattr(glue if method == "delete_table" else s3, method)(**params)
                if (
                    action["kind"] in {"version", "delete_marker"}
                    and response.get("VersionId", action["version_id"]) != action["version_id"]
                ):
                    raise ValueError("Deleted version response identity differs")
                result["acknowledged"] = True
            except Exception as error:
                result["error_type"], result["error_code"] = (
                    type(error).__name__,
                    _error_code(error),
                )
            _save(output / f"action-{index:02}-response.json", result, MAX_PLAN_BYTES)
            if not result["acknowledged"]:
                break
        stage = "absence_verification"
        recheck(release, pin, evidence)
        pinned_record(approved.path, approved.sha256)
        services = inventory_services(glue, athena, release, pin, output / "after/services")
        inventory = inventory_s3(s3, release, pin, output / "after/s3")
        quiet = True
        try:
            require_quiet(services, ctx)
        except ValueError:
            quiet = False
        parts = []
        for index, action in enumerate(
            a for a in plan["actions"] if a["kind"] == "multipart_upload"
        ):
            observation = _parts(s3, action, ctx["scope"]["target"]["account_id"])
            _save(output / f"parts-{index:02}.json", observation, MAX_PLAN_BYTES)
            parts.append(observation)
        budget.check_time()
        recheck(release, pin, evidence)
        pinned_record(approved.path, approved.sha256)
        parts_empty = all(row["parts_empty_observed"] for row in parts)
        absent = quiet and not services["relations"] and not inventory["entries"] and parts_empty
        report = {
            "version": 1,
            "status": "trial_data_absence_observed"
            if absent
            else "trial_data_cleanup_requires_review",
            "approved_plan_sha256": approved.sha256,
            "resume": resume,
            "changes": changes,
            "actions_selected": len(actions),
            "actions_attempted": len(changes),
            "api_calls": budget.calls,
            "catalog_entries_absent": not services["relations"],
            "bucket_contents_absent": not inventory["entries"],
            "multipart_parts_empty_observed": parts_empty,
            "quiescence_observed": quiet,
            "data_cleanup_observed": absent,
            "requires_review": not absent,
            "service_inventory_sha256": _saved_pin(
                output / "after/services/inventory.json", services, 512 * 1024
            ),
            "s3_inventory_sha256": _saved_pin(
                output / "after/s3/inventory.json", inventory, 4 * 1024 * 1024
            ),
            "coordinator_paused_assertion": True,
            "future_submissions_fenced": False,
            "catalog_orphan_versions_verified": False,
            "billing_zero_verified": False,
            "infrastructure_destroyed": False,
        }
        _save(output / "receipt.json", report, MAX_PLAN_BYTES)
        return report
    except BaseException as error:
        try:
            _save(
                output / "failure.json",
                {
                    "status": "trial_data_cleanup_failed",
                    "stage": stage,
                    "approved_plan_sha256": approved.sha256,
                    "error_type": type(error).__name__,
                    "error_code": _error_code(error),
                    "calls_attempted": budget.calls,
                    "changes_attempted": len(changes),
                    "data_cleanup_observed": False,
                },
                MAX_PLAN_BYTES,
            )
        except Exception:
            pass
        raise


def run_data_cleanup(
    release: Path,
    pin: str,
    evidence: CleanupEvidence,
    principal_arn: str,
    content: bytes,
    *,
    output: Path,
    coordinator_paused: bool = False,
    approved: Record | None = None,
    resume: bool = False,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Prepare by default; execution requires an explicitly pinned reviewed selection."""
    from aws_role_session import LifetimeClient

    require_paused(coordinator_paused)
    if type(resume) is not bool or (approved is None and resume):
        raise ValueError("Explicit resume requires a pinned approved cleanup plan")
    prepare_operation(
        release,
        pin,
        evidence.inventory_plan.path,
        evidence.inventory_plan.sha256,
        principal_arn,
        output,
    )
    check_output(release, evidence, output, approved)
    context(release, pin, evidence)
    if approved is not None:
        load_approved(release, pin, evidence, approved)
    acquisition = cleanup_credentials(
        release,
        pin,
        evidence.inventory_plan.path,
        evidence.inventory_plan.sha256,
        principal_arn,
        content,
    )
    content = b""
    with acquisition as credentials:
        credentials.require_operation(PREPARATION_SECONDS if approved is None else REQUIRED_SECONDS)
        with ExitStack() as owned:
            clients = {}
            for service, calls in (
                ("s3", S3_CALLS),
                ("glue", GLUE_CALLS),
                ("athena", ATHENA_READ_CALLS),
            ):
                raw = (
                    create_s3_client(credentials)
                    if service == "s3"
                    else create_service_client(credentials, service)
                )
                owned.callback(raw.close)
                clients[service] = LifetimeClient(raw, credentials, {name: 90 for name in calls})
            if approved is None:
                result = prepare_cleanup(
                    clients["glue"],
                    clients["athena"],
                    clients["s3"],
                    release,
                    pin,
                    evidence,
                    output / "attempt",
                    coordinator_paused=True,
                )
                record_path = output / "attempt/plan.json"
            else:
                result = execute_cleanup(
                    clients["glue"],
                    clients["athena"],
                    clients["s3"],
                    release,
                    pin,
                    evidence,
                    approved,
                    output / "attempt",
                    coordinator_paused=True,
                    resume=resume,
                    now=now,
                )
                record_path = output / "attempt/receipt.json"
            credentials.require_operation(90)
            recheck(release, pin, evidence)
    completion = {
        "version": 1,
        "status": "scoped_data_cleanup_prepared"
        if approved is None
        else "scoped_data_cleanup_completed",
        "record_sha256": _saved_pin(record_path, result, MAX_PLAN_BYTES),
        "executed": approved is not None,
        "data_cleanup_observed": result.get("data_cleanup_observed", False),
        "requires_review": result.get("requires_review", False),
    }
    write_once(output / "session-complete.json", canonical(completion))
    return completion
