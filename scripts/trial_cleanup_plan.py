"""Prepare an exact disposable-trial cleanup selection; preparation never deletes data."""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from athena_control import canonical
from athena_runner import pinned_record
from dbt_view_capture import _encode
from trial_cleanup_inventory import _save, _text, in_scope, inventory_s3, validate_inventory_output
from trial_cleanup_operation import _saved_pin
from trial_cleanup_session import load_cleanup_plan
from trial_quiescence_plan import (
    QueryEvidence,
    Record,
    evidence_records,
    plan_sha256,
    quiescence_plan,
    recheck_evidence,
)
from trial_service_inventory import inventory_services

MAX_ACTIONS = 64
BATCH_SIZE = 8
MAX_CALLS = 2 * (44 + 29 + 3) + (MAX_ACTIONS // BATCH_SIZE) * 29 + 2 * MAX_ACTIONS + 3
EXECUTION_SECONDS = 1500
MAX_PLAN_BYTES = 64 * 1024
TABLE_ORDER = ("release_proximity", "grid_proximity", "building_proximity")


@dataclass(frozen=True)
class CleanupEvidence:
    inventory_plan: Record
    quiescence_receipt: Record
    quiescence_completion: Record
    job: Record | None
    queries: tuple[QueryEvidence, ...]


def records(evidence: CleanupEvidence) -> list[Record]:
    return [
        evidence.inventory_plan,
        evidence.quiescence_receipt,
        evidence.quiescence_completion,
        *evidence_records(evidence.job, evidence.queries),
    ]


def recheck(release: Path, pin: str, evidence: CleanupEvidence) -> None:
    recheck_evidence(release, pin, evidence.job, evidence.queries)
    for record in records(evidence):
        pinned_record(record.path, record.sha256)


def context(release: Path, pin: str, evidence: CleanupEvidence) -> dict[str, Any]:
    scope = load_cleanup_plan(
        release, pin, evidence.inventory_plan.path, evidence.inventory_plan.sha256
    )
    quiet_plan = quiescence_plan(release, pin, evidence.job, evidence.queries)
    quiet_pin = plan_sha256(quiet_plan)
    receipt = pinned_record(evidence.quiescence_receipt.path, evidence.quiescence_receipt.sha256)
    completion = pinned_record(
        evidence.quiescence_completion.path, evidence.quiescence_completion.sha256
    )
    if (
        receipt.get("status") != "quiescence_observed"
        or receipt.get("plan_sha256") != quiet_pin
        or receipt.get("release_manifest_sha256") != pin
        or any(
            receipt.get(k) is not True
            for k in (
                "quiescence_observed",
                "owned_terminal_observed",
                "catalog_observed",
                "quiescence_and_catalog_observed",
            )
        )
        or receipt.get("requires_review") is not False
        or receipt.get("deletion_authorized") is not False
        or completion.get("status") != "scoped_trial_quiescence_complete"
        or completion.get("plan_sha256") != quiet_pin
        or completion.get("inventory_plan_sha256") != evidence.inventory_plan.sha256
        or completion.get("receipt_sha256") != evidence.quiescence_receipt.sha256
        or completion.get("quiescence_observed") is not True
        or completion.get("requires_review") is not False
        or completion.get("deletion_authorized") is not False
    ):
        raise ValueError("Cleanup requires matching successful scoped quiescence evidence")
    return {
        "scope": scope,
        "quiescence_plan_sha256": quiet_pin,
        "quiescence_receipt_sha256": evidence.quiescence_receipt.sha256,
        "quiescence_completion_sha256": evidence.quiescence_completion.sha256,
        "inventory_plan_sha256": evidence.inventory_plan.sha256,
        "job_ids": [] if quiet_plan["job_run"] is None else [quiet_plan["job_run"]["id"]],
        "query_ids": [row["id"] for row in quiet_plan["queries"]],
    }


def check_output(
    release: Path, evidence: CleanupEvidence, output: Path, approved: Record | None = None
) -> None:
    validate_inventory_output(release, output)
    for record in records(evidence) + ([] if approved is None else [approved]):
        protected = record.path.parent.resolve()
        if output.resolve().is_relative_to(protected) or protected.is_relative_to(output.resolve()):
            raise ValueError("Keep cleanup output separate from approved plans and evidence")


def require_paused(value: bool) -> None:
    if type(value) is not bool or value is not True:
        raise ValueError("The caller must explicitly confirm its trial coordinator is paused")


def require_quiet(report: dict[str, Any], ctx: dict[str, Any]) -> None:
    if (
        report.get("status") != "service_inventory_complete"
        or report.get("enumeration_complete") is not True
        or report.get("workgroup_state") != "DISABLED"
        or {r["id"] for r in report["job_runs"]} != set(ctx["job_ids"])
        or {r["id"] for r in report["queries"]} != set(ctx["query_ids"])
        or any(r["active"] for r in report["job_runs"])
        or any(r["active"] for r in report["queries"])
    ):
        raise ValueError("Live trial services are not quiet and exactly receipt-bound")


def table_action(glue: Any, ctx: dict[str, Any], name: str) -> dict[str, Any]:
    scope = ctx["scope"]
    table = glue.get_table(
        CatalogId=scope["target"]["account_id"],
        DatabaseName=scope["expected_services"]["database"],
        Name=name,
    )["Table"]
    kind = "EXTERNAL_TABLE" if name == "building_proximity" else "VIRTUAL_VIEW"
    created = table.get("CreateTime")
    if (
        name not in TABLE_ORDER
        or table.get("Name") != name
        or table.get("CatalogId") != scope["target"]["account_id"]
        or table.get("DatabaseName") != scope["expected_services"]["database"]
        or table.get("TableType") != kind
        or not isinstance(created, datetime)
        or created.utcoffset() is None
        or not isinstance(table.get("VersionId"), str)
        or not table["VersionId"]
        or any(table.get(k) for k in ("TargetTable", "FederatedTable", "PartitionKeys"))
    ):
        raise ValueError("Catalog cleanup identity, generation or table type differs")
    if name == "building_proximity":
        location = "s3://" + scope["buckets"]["lake"]["name"] + "/warehouse/building_proximity"
        parameters = table.get("Parameters", {})
        metadata = parameters.get("metadata_location")
        if (
            table.get("StorageDescriptor", {}).get("Location", "").rstrip("/") != location
            or parameters.get("table_type", "").upper() != "ICEBERG"
            or not isinstance(metadata, str)
            or not metadata.startswith(location + "/metadata/")
            or "/" in metadata[len(location + "/metadata/") :]
            or not metadata.endswith(".metadata.json")
        ):
            raise ValueError("Fact cleanup must stay inside the exact trial Iceberg warehouse")
    fields = (
        "Name",
        "CatalogId",
        "DatabaseName",
        "CreateTime",
        "VersionId",
        "TableType",
        "Parameters",
        "StorageDescriptor",
        "PartitionKeys",
        "ViewOriginalText",
        "ViewExpandedText",
        "ViewDefinition",
        "TargetTable",
        "FederatedTable",
        "IsRegisteredWithLakeFormation",
        "IsMultiDialectView",
    )
    fingerprint = hashlib.sha256(
        _encode({key: table[key] for key in fields if key in table})
    ).hexdigest()
    return {
        "kind": "table",
        "name": name,
        "table_type": kind,
        "version_id": table["VersionId"],
        "fingerprint": fingerprint,
    }


def object_action(row: dict[str, Any], scope: dict[str, Any]) -> dict[str, Any]:
    bucket_kind, kind = row["bucket_kind"], row["kind"]
    if (
        bucket_kind not in scope["buckets"]
        or row["bucket"] != scope["buckets"][bucket_kind]["name"]
        or not in_scope(scope, bucket_kind, row["key"])
    ):
        raise ValueError("Unknown S3 cleanup key or bucket")
    common = {"kind": kind, "bucket_kind": bucket_kind, "bucket": row["bucket"], "key": row["key"]}
    if kind == "multipart_upload":
        return common | {
            "upload_id": _text(row["upload_id"]),
            "initiated_utc": row["initiated_utc"],
        }
    if bucket_kind == "lake" and kind in {"version", "delete_marker"}:
        extra = {
            "version_id": _text(row["version_id"]),
            "modified_utc": row["modified_utc"],
            "bytes": row["bytes"],
        }
        if kind == "version":
            extra["etag"] = _text(row["etag"])
        return common | extra
    if bucket_kind == "results" and kind == "object":
        etag = _text(row["etag"])
        if etag in {"*", '"*"'} or any(ord(c) < 32 or ord(c) == 127 for c in etag):
            raise ValueError("Cleanup requires an exact non-wildcard results ETag")
        return common | {"etag": etag, "bytes": row["bytes"], "modified_utc": row["modified_utc"]}
    raise ValueError("Unexpected object kind for the configured bucket versioning")


def identity(action: dict[str, Any]) -> tuple[str, ...]:
    kind = action["kind"]
    if kind == "table":
        return ("table", action["name"])
    if kind == "multipart_upload":
        return ("upload", action["bucket"], action["key"], action["upload_id"])
    if kind in {"version", "delete_marker"}:
        return ("version", action["bucket"], action["key"], action["version_id"])
    if kind == "object":
        return ("object", action["bucket"], action["key"])
    raise ValueError("Unsupported cleanup action")


def make_plan(ctx: dict[str, Any], actions: list[dict[str, Any]]) -> dict[str, Any]:
    if len(actions) > MAX_ACTIONS or len({identity(a) for a in actions}) != len(actions):
        raise ValueError("Cleanup action ceiling or uniqueness differs")
    return {
        "version": 1,
        "status": "prepared_exact_trial_data_cleanup",
        "context": ctx,
        "actions": actions,
        "coordinator_paused_assertion": True,
        "limits": {
            "max_actions": MAX_ACTIONS,
            "batch_size": BATCH_SIZE,
            "max_calls": MAX_CALLS,
            "execution_seconds": EXECUTION_SECONDS,
            "max_plan_bytes": MAX_PLAN_BYTES,
        },
        "execution_authorized": False,
    }


def snapshot(
    glue: Any,
    athena: Any,
    s3: Any,
    release: Path,
    pin: str,
    ctx: dict[str, Any],
    output: Path,
    *,
    details: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    services = inventory_services(glue, athena, release, pin, output / "services")
    require_quiet(services, ctx)
    inventory = inventory_s3(s3, release, pin, output / "s3")
    if inventory["all_keys_in_cleanup_scope"] is not True:
        raise ValueError("Unknown bucket keys block trial data cleanup")
    names = {row["id"] for row in services["relations"]}
    actions = (
        [table_action(glue, ctx, name) for name in TABLE_ORDER if name in names] if details else []
    )
    order = {"multipart_upload": 0, "version": 1, "delete_marker": 2, "object": 3}
    rows = sorted(
        inventory["entries"],
        key=lambda row: (
            order[row["kind"]],
            row["bucket"],
            row["key"],
            row.get("is_latest", False),
            row.get("version_id", row.get("upload_id", "")),
        ),
    )
    actions.extend(object_action(row, ctx["scope"]) for row in rows)
    return services, inventory, actions


def prepare_cleanup(
    glue: Any,
    athena: Any,
    s3: Any,
    release: Path,
    pin: str,
    evidence: CleanupEvidence,
    output: Path,
    *,
    coordinator_paused: bool = False,
) -> dict[str, Any]:
    require_paused(coordinator_paused)
    ctx = context(release, pin, evidence)
    check_output(release, evidence, output)
    output.mkdir(parents=True)
    try:
        services, inventory, actions = snapshot(
            glue, athena, s3, release, pin, ctx, output / "observed"
        )
        recheck(release, pin, evidence)
        plan = make_plan(ctx, actions)
        plan_pin = _save(output / "plan.json", plan, MAX_PLAN_BYTES)
        _save(
            output / "preparation.json",
            {
                "status": "exact_cleanup_prepared",
                "plan_sha256": plan_pin,
                "service_inventory_sha256": _saved_pin(
                    output / "observed/services/inventory.json", services, 512 * 1024
                ),
                "s3_inventory_sha256": _saved_pin(
                    output / "observed/s3/inventory.json", inventory, 4 * 1024 * 1024
                ),
                "execution_authorized": False,
            },
            MAX_PLAN_BYTES,
        )
        return plan
    except BaseException as error:
        try:
            _save(
                output / "failure.json",
                {
                    "status": "cleanup_preparation_failed",
                    "error_type": type(error).__name__,
                    "changes_attempted": 0,
                },
                MAX_PLAN_BYTES,
            )
        except Exception:
            pass
        raise


def load_approved(
    release: Path, pin: str, evidence: CleanupEvidence, approved: Record
) -> dict[str, Any]:
    value = pinned_record(approved.path, approved.sha256)
    ctx = context(release, pin, evidence)
    try:
        expected = make_plan(ctx, value["actions"])
    except (KeyError, TypeError, AttributeError):
        raise ValueError("Malformed approved cleanup selection") from None
    if canonical(value) != canonical(expected):
        raise ValueError("Approved cleanup plan differs from the fixed recipe and evidence")
    return value


def remaining(
    approved: dict[str, Any], fresh: list[dict[str, Any]], resume: bool
) -> list[dict[str, Any]]:
    if type(resume) is not bool:
        raise ValueError("resume must be an explicit boolean")
    original = {identity(a): canonical(a) for a in approved["actions"]}
    observed = {identity(a): canonical(a) for a in fresh}
    if (not resume and original != observed) or any(
        original.get(key) != value for key, value in observed.items()
    ):
        raise ValueError("Live cleanup targets differ from the approved exact selection")
    return fresh
