"""Validate the fixed named-infrastructure records before transport accepts their outcome."""

import re
from datetime import datetime
from typing import Any

from athena_control import canonical
from aws_login_bridge import BridgeError
from trial_infrastructure_plan import MAX_CALLS, plan_sha256


def _require(actual: Any, expected: Any) -> None:
    if canonical(actual) != canonical(expected):
        raise BridgeError("infrastructure_saved_binding_differs")


def record_names(plan: dict[str, Any]) -> list[str]:
    return [
        "session-complete.json",
        "inspection/plan.json",
        "inspection/inventory.json",
        *[f"inspection/target-{i:02}.json" for i in range(len(plan["targets"]))],
    ]


def validate_completion(
    plan: dict[str, Any], records: dict[str, Any], hashes: dict[str, str]
) -> dict[str, Any]:
    """Check saved metadata only; never follow supplied paths or make another AWS call."""
    names = set(record_names(plan))
    if set(records) != names or set(hashes) != names:
        raise BridgeError("infrastructure_transport_layout_differs")
    _require(records["inspection/plan.json"], plan)
    _require(hashes["inspection/plan.json"], plan_sha256(plan))
    report = records["inspection/inventory.json"]
    rows = report["targets"]
    if not isinstance(rows, list) or len(rows) != len(plan["targets"]):
        raise BridgeError("infrastructure_saved_target_count_differs")
    for index, (row, target) in enumerate(zip(rows, plan["targets"], strict=True)):
        if not isinstance(row, dict) or set(row) != {
            "kind",
            "name",
            "state",
            "reason",
            "error_type",
            "error_code",
        }:
            raise BridgeError("infrastructure_saved_target_fields_differ")
        _require((row["kind"], row["name"]), (target["kind"], target["name"]))
        listed = target["kind"] in {"workgroup", "log_group"}
        if row["state"] == "absent":
            _require(row["reason"], "complete_listing" if listed else "typed_not_found")
        elif row["state"] == "present":
            _require(row["reason"], "matching_entry" if listed else "matching_identity")
        elif row["state"] == "unconfirmed":
            allowed = (
                {"listing_unconfirmed", "pagination_ceiling"} if listed else {"read_unconfirmed"}
            )
            if row["reason"] not in allowed:
                raise BridgeError("infrastructure_saved_reason_differs")
        else:
            raise BridgeError("infrastructure_saved_state_differs")
        if row["state"] != "unconfirmed" or row["reason"] == "pagination_ceiling":
            _require((row["error_type"], row["error_code"]), (None, None))
        else:
            if not isinstance(row["error_type"], str) or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]{0,127}", row["error_type"]
            ):
                raise BridgeError("infrastructure_saved_error_type_differs")
            code = row["error_code"]
            if code is not None and (
                not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", code)
            ):
                raise BridgeError("infrastructure_saved_error_code_differs")
        _require(records[f"inspection/target-{index:02}.json"], row)
    calls = report["api_calls"]
    if type(calls) is not int or not 1 <= calls <= MAX_CALLS:
        raise BridgeError("infrastructure_saved_call_count_differs")
    times = [datetime.fromisoformat(report[key]) for key in ("started_at_utc", "completed_at_utc")]
    if any(t.utcoffset() is None for t in times) or times[1] < times[0]:
        raise BridgeError("infrastructure_saved_times_differ")
    absent = all(row["state"] == "absent" for row in rows)
    expected = {
        "version": 1,
        "status": "named_infrastructure_absence_observed"
        if absent
        else "infrastructure_absence_requires_review",
        "plan_sha256": plan_sha256(plan),
        "release_manifest_sha256": plan["release_manifest_sha256"],
        "source_identity_verified": True,
        "targets": rows,
        "api_calls": calls,
        "started_at_utc": report["started_at_utc"],
        "completed_at_utc": report["completed_at_utc"],
        "all_targets_confirmed": all(r["state"] != "unconfirmed" for r in rows),
        "named_infrastructure_absence_observed": absent,
        "requires_review": not absent,
        "scope": "explicit_resource_names_only",
        "deletion_performed": False,
        "terraform_state_verified": False,
        "billing_zero_verified": False,
        "unmanaged_resources_checked": False,
        "orphaned_service_metadata_checked": False,
        "future_submissions_fenced": False,
    }
    _require(report, expected)
    _require(
        records["session-complete.json"],
        {
            "version": 1,
            "status": "source_session_infrastructure_inspection_complete",
            "plan_sha256": plan_sha256(plan),
            "inventory_sha256": hashes["inspection/inventory.json"],
            "named_infrastructure_absence_observed": absent,
            "requires_review": not absent,
            "read_only": True,
            "deletion_performed": False,
        },
    )
    return {
        "operation_observed": absent,
        "named_infrastructure_absence_observed": absent,
        "requires_review": not absent,
        "data_cleanup_observed": False,
        "prepared_plan_sha256": None,
    }
