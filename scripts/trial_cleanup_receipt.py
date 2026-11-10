"""Check fixed bounded cleanup outputs before accepting a small transport reply."""

import hashlib
import re
from pathlib import Path
from typing import Any

import trial_service_inventory as services
from athena_control import canonical, write_once
from aws_login_bridge import BridgeError, decode
from trial_cleanup_command import Operation
from trial_cleanup_plan import (
    MAX_ACTIONS,
    MAX_CALLS,
    TABLE_ORDER,
    identity,
    make_plan,
    object_action,
    require_quiet,
)
from trial_cleanup_session import load_cleanup_plan
from trial_quiescence_plan import plan_sha256

REPLY_BYTES = 4096
PROOF = "transport-proof.json"
SMALL = 64 * 1024


def _require(actual: Any, expected: Any) -> None:
    if canonical(actual) != canonical(expected):
        raise BridgeError("cleanup_saved_binding_differs")


def _read(root: Path, name: str, limit: int = SMALL) -> tuple[dict[str, Any], str]:
    path = root / name
    if not path.is_file() or any(p.is_symlink() for p in (path, *path.parents)):
        raise BridgeError("cleanup_record_not_regular")
    with path.open("rb") as stream:
        content = stream.read(limit + 1)
    return decode(content, limit), hashlib.sha256(content).hexdigest()


def _check_inventory(report: dict[str, Any], kind: str, scope: dict[str, Any]) -> None:
    if kind == "s3":
        from trial_cleanup_inventory import MAX_ENTRIES, in_scope

        if len(report["entries"]) > MAX_ENTRIES:
            raise BridgeError("cleanup_inventory_ceiling_differs")
        for row in report["entries"]:
            bucket = scope["buckets"][row["bucket_kind"]]
            _require(row["bucket"], bucket["name"])
            _require(row["in_cleanup_scope"], in_scope(scope, row["bucket_kind"], row["key"]))
        inside = all(row["in_cleanup_scope"] for row in report["entries"])
        _require(
            report["status"], "s3_inventory_complete" if inside else "s3_inventory_requires_review"
        )
    else:
        if any(
            len(report[key]) > services.MAX_ITEMS for key in ("relations", "job_runs", "queries")
        ):
            raise BridgeError("cleanup_inventory_ceiling_differs")
        for row in report["relations"]:
            inside = row["id"] in scope["expected_services"]["tables"]
            wanted = "EXTERNAL_TABLE" if row["id"] == "building_proximity" else "VIRTUAL_VIEW"
            _require(row["in_cleanup_scope"], inside)
            _require(row["expected_type_matches"], inside and row["table_type"] == wanted)
        for rows, active, terminal in [
            (report["job_runs"], services.JOB_ACTIVE, services.JOB_TERMINAL),
            (report["queries"], services.QUERY_ACTIVE, services.QUERY_TERMINAL),
        ]:
            for row in rows:
                if row["state"] not in active | terminal:
                    raise BridgeError("cleanup_state_differs")
                _require(row["active"], row["state"] in active)
        reasons = []
        if any(not r["expected_type_matches"] for r in report["relations"]):
            reasons.append("unexpected_relations")
        if any(r["active"] for r in report["job_runs"]):
            reasons.append("active_glue_runs")
        if any(r["active"] for r in report["queries"]):
            reasons.append("active_athena_queries")
        if any(r["matches_trial_context"] is not True for r in report["queries"]):
            reasons.append("query_context_differs")
        if report["workgroup_state"] != "DISABLED":
            reasons.append("workgroup_enabled")
        _require(report["review_reasons"], reasons)
        _require(
            report["status"],
            "service_inventory_requires_review" if reasons else "service_inventory_complete",
        )


def _layout(op: Operation, prepared: dict[str, Any]) -> tuple[list[str], dict[str, str]]:
    names = ["session-complete.json"]
    inventories: dict[str, str]
    if op.phase == "inventory":
        inventories = {"services": "services", "s3": "s3"}
        names += ["operation-intent.json"]
    elif op.phase == "quiesce":
        inventories = {"attempt/before": "services", "attempt/after": "services"}
        names += ["attempt/receipt.json"]
    elif op.phase == "prepare-data":
        inventories = {"attempt/observed/services": "services", "attempt/observed/s3": "s3"}
        names += ["attempt/plan.json", "attempt/preparation.json"]
    else:
        inventories = {
            "attempt/before/services": "services",
            "attempt/before/s3": "s3",
            "attempt/after/services": "services",
            "attempt/after/s3": "s3",
        }
        names += ["attempt/receipt.json", "attempt/intent.json"]
        assert op.approved is not None
        from athena_runner import pinned_record

        approved = pinned_record(op.approved.path, op.approved.sha256)
        uploads = [a for a in approved["actions"] if a["kind"] == "multipart_upload"]
        if len(uploads) > MAX_ACTIONS:
            raise BridgeError("cleanup_parts_ceiling_differs")
        names += [f"attempt/parts-{i:02}.json" for i in range(len(uploads))]
    for prefix in inventories:
        names += [prefix + "/plan.json", prefix + "/inventory.json"]
    return names, inventories


def _validate(
    op: Operation,
    prepared: dict[str, Any],
    records: dict[str, Any],
    hashes: dict[str, str],
    inventories: dict[str, str],
) -> dict[str, Any]:
    scope = prepared["scope"]
    for prefix, kind in inventories.items():
        report, plan = (records[prefix + "/" + name + ".json"] for name in ("inventory", "plan"))
        _require(report["release_manifest_sha256"], op.release_pin)
        _require(report["plan_sha256"], hashes[prefix + "/plan.json"])
        _require(report["enumeration_complete"], True)
        _require(report["deletion_authorized"], False)
        _check_inventory(report, kind, scope)
        if kind == "s3":
            _require(plan, scope)
            _require(
                report["all_keys_in_cleanup_scope"],
                all(a["in_cleanup_scope"] is True for a in report["entries"]),
            )
        else:
            _require(
                plan,
                {
                    "version": 1,
                    "release_manifest_sha256": op.release_pin,
                    "target": scope["target"],
                    "expected_services": scope["expected_services"],
                    "read_only": True,
                    "deletion_authorized": False,
                    "limits": {
                        "page_size": services.PAGE_SIZE,
                        "max_pages_per_listing": services.MAX_PAGES,
                        "max_items_per_listing": services.MAX_ITEMS,
                        "max_service_calls": services.MAX_CALLS,
                        "max_record_bytes": services.MAX_RECORD_BYTES,
                        "max_report_bytes": services.MAX_REPORT_BYTES,
                    },
                },
            )
    outer = records["session-complete.json"]
    observed = False
    prepared_pin = None
    if op.phase == "inventory":
        _require(outer["status"], "scoped_trial_inventory_complete")
        _require(outer["include_services"], True)
        _require(outer["release_manifest_sha256"], op.release_pin)
        _require(outer["inventory_plan_sha256"], op.inventory.sha256)
        _require(outer["inventory_sha256"], hashes["s3/inventory.json"])
        _require(outer["service_inventory_sha256"], hashes["services/inventory.json"])
        _require(outer["operation_intent_sha256"], hashes["operation-intent.json"])
        intent = records["operation-intent.json"]
        _require(
            intent,
            {
                "version": 1,
                "include_services": True,
                "release_manifest_sha256": op.release_pin,
                "inventory_plan_sha256": op.inventory.sha256,
                "required_lifetime_seconds": 1500,
                "read_only": True,
                "deletion_authorized": False,
            },
        )
        review = any(
            records[p + "/inventory.json"]["status"].endswith("requires_review")
            for p in inventories
        )
        _require(outer["requires_review"], review)
        _require(outer["deletion_authorized"], False)
        observed = not review
    elif op.phase == "quiesce":
        receipt, after = records["attempt/receipt.json"], records["attempt/after/inventory.json"]
        plan = prepared["recipe"]["payload"]
        _require(outer["status"], "scoped_trial_quiescence_complete")
        for key, value in {
            "plan_sha256": plan_sha256(plan),
            "inventory_plan_sha256": op.inventory.sha256,
            "receipt_sha256": hashes["attempt/receipt.json"],
            "deletion_authorized": False,
        }.items():
            _require(outer[key], value)
        _require(receipt["plan_sha256"], plan_sha256(plan))
        _require(receipt["release_manifest_sha256"], op.release_pin)
        _require(receipt["before_inventory_sha256"], hashes["attempt/before/inventory.json"])
        _require(receipt["after_inventory_sha256"], hashes["attempt/after/inventory.json"])
        for key in ("owned_terminal_observed", "quiescence_observed", "requires_review"):
            if type(receipt[key]) is not bool:
                raise BridgeError("cleanup_saved_boolean_differs")
        jobs = set() if plan["job_run"] is None else {plan["job_run"]["id"]}
        queries = {q["id"] for q in plan["queries"]}
        quiet = after["workgroup_state"] == "DISABLED" and not any(
            r["active"] for r in after["job_runs"] + after["queries"]
        )
        review = bool(
            after["review_reasons"]
            or {r["id"] for r in after["job_runs"]} != jobs
            or {r["id"] for r in after["queries"]} != queries
            or not receipt["owned_terminal_observed"]
        )
        quiet = quiet and receipt["owned_terminal_observed"]
        _require(receipt["quiescence_observed"], quiet)
        _require(receipt["requires_review"], review)
        _require(
            receipt["status"], "quiescence_requires_review" if review else "quiescence_observed"
        )
        _require(outer["quiescence_observed"], quiet)
        _require(outer["requires_review"], review)
        _require(receipt["deletion_authorized"], False)
        _require(receipt["future_job_submissions_fenced"], False)
        observed = quiet and not review
    elif op.phase == "prepare-data":
        plan = records["attempt/plan.json"]
        ctx = prepared["recipe"]["payload"]
        _require(plan, make_plan(ctx, plan["actions"]))
        service = records["attempt/observed/services/inventory.json"]
        s3 = records["attempt/observed/s3/inventory.json"]
        require_quiet(service, ctx)
        _require(s3["all_keys_in_cleanup_scope"], True)
        selected = sorted(canonical(a).decode() for a in plan["actions"] if a["kind"] != "table")
        expected = sorted(canonical(object_action(a, scope)).decode() for a in s3["entries"])
        _require(selected, expected)
        tables = [a for a in plan["actions"] if a["kind"] == "table"]
        _require(
            [a["name"] for a in tables],
            [n for n in TABLE_ORDER if n in {r["id"] for r in service["relations"]}],
        )
        for action in tables:
            if not re.fullmatch(r"[0-9a-f]{64}", action["fingerprint"]) or not action["version_id"]:
                raise BridgeError("cleanup_catalog_generation_differs")
        prepared_pin = hashes["attempt/plan.json"]
        _require(
            records["attempt/preparation.json"],
            {
                "status": "exact_cleanup_prepared",
                "plan_sha256": prepared_pin,
                "service_inventory_sha256": hashes["attempt/observed/services/inventory.json"],
                "s3_inventory_sha256": hashes["attempt/observed/s3/inventory.json"],
                "execution_authorized": False,
            },
        )
        _require(outer["status"], "scoped_data_cleanup_prepared")
        _require(outer["record_sha256"], prepared_pin)
        _require(outer["executed"], False)
        _require(outer["data_cleanup_observed"], False)
        _require(outer["requires_review"], False)
        review = False
    else:
        from athena_runner import pinned_record

        assert op.approved is not None
        approved = pinned_record(op.approved.path, op.approved.sha256)
        ctx = prepared["recipe"]["payload"]
        receipt = records["attempt/receipt.json"]
        after, s3 = (
            records["attempt/after/services/inventory.json"],
            records["attempt/after/s3/inventory.json"],
        )
        allowed = {identity(a): a for a in approved["actions"]}
        seen = set()
        for change in receipt["changes"]:
            action = change["action"]
            action_id = identity(action)
            _require(action, allowed[action_id])
            if action_id in seen or type(change["acknowledged"]) is not bool:
                raise BridgeError("cleanup_attempted_scope_differs")
            seen.add(action_id)
        _require(receipt["actions_attempted"], len(seen))
        if (
            not len(seen) <= receipt["actions_selected"] <= MAX_ACTIONS
            or not 0 <= receipt["api_calls"] <= MAX_CALLS
        ):
            raise BridgeError("cleanup_attempt_ceiling_differs")
        _require(
            records["attempt/intent.json"],
            {
                "approved_plan_sha256": op.approved.sha256,
                "resume": op.resume,
                "coordinator_paused_assertion": True,
            },
        )
        uploads = [a for a in approved["actions"] if a["kind"] == "multipart_upload"]
        parts_empty = True
        for index, upload in enumerate(uploads):
            part = records[f"attempt/parts-{index:02}.json"]
            _require(part["upload_id"], upload["upload_id"])
            if type(part["parts_empty_observed"]) is not bool:
                raise BridgeError("cleanup_saved_boolean_differs")
            parts_empty = parts_empty and part["parts_empty_observed"]
        quiet = True
        try:
            require_quiet(after, ctx)
        except ValueError:
            quiet = False
        observed = quiet and not after["relations"] and not s3["entries"] and parts_empty
        review = not observed
        for key, value in {
            "approved_plan_sha256": op.approved.sha256,
            "resume": op.resume,
            "coordinator_paused_assertion": True,
            "quiescence_observed": quiet,
            "catalog_entries_absent": not after["relations"],
            "bucket_contents_absent": not s3["entries"],
            "multipart_parts_empty_observed": parts_empty,
            "data_cleanup_observed": observed,
            "requires_review": review,
            "future_submissions_fenced": False,
            "catalog_orphan_versions_verified": False,
            "billing_zero_verified": False,
            "infrastructure_destroyed": False,
            "service_inventory_sha256": hashes["attempt/after/services/inventory.json"],
            "s3_inventory_sha256": hashes["attempt/after/s3/inventory.json"],
        }.items():
            _require(receipt[key], value)
        _require(
            receipt["status"],
            "trial_data_absence_observed" if observed else "trial_data_cleanup_requires_review",
        )
        _require(
            outer,
            {
                "version": 1,
                "status": "scoped_data_cleanup_completed",
                "record_sha256": hashes["attempt/receipt.json"],
                "executed": True,
                "data_cleanup_observed": observed,
                "requires_review": review,
            },
        )
    return {
        "operation_observed": observed,
        "requires_review": review,
        "data_cleanup_observed": observed if op.phase == "delete-data" else False,
        "prepared_plan_sha256": prepared_pin,
    }


def _collect(output: Path, op: Operation, prepared: dict[str, Any]) -> dict[str, Any]:
    _require(
        load_cleanup_plan(op.release, op.release_pin, op.inventory.path, op.inventory.sha256),
        prepared["scope"],
    )
    from aws_login_bridge import read_record

    for record in op.records():
        _, actual = read_record(record.path)
        _require(actual, record.sha256)
    names, inventories = _layout(op, prepared)
    records, hashes = {}, {}
    for name in names:
        limit = (
            4 * 1024 * 1024
            if name.endswith("/s3/inventory.json") or name == "s3/inventory.json"
            else SMALL
            if not name.endswith("/inventory.json")
            else 512 * 1024
        )
        records[name], hashes[name] = _read(output, name, limit)
    result = _validate(op, prepared, records, hashes, inventories)
    return {
        "version": 1,
        "request_sha256": prepared["request_sha256"],
        "phase": op.phase,
        "files": hashes,
        **result,
    }


def save_completion(output: Path, op: Operation, prepared: dict[str, Any]) -> str:
    proof = _collect(output, op, prepared)
    content = canonical(proof)
    if len(content) > SMALL:
        raise BridgeError("cleanup_proof_ceiling_differs")
    write_once(output / PROOF, content)
    return hashlib.sha256(content).hexdigest()


def reply(pin: str, phase: str, proof: str | None = None) -> dict[str, Any]:
    return {
        "version": 1,
        "request_sha256": pin,
        "phase": phase,
        "status": "operation_failed" if proof is None else "phase_completed",
        "proof_sha256": proof,
    }


def verify_reply(content: bytes, pin: str, phase: str) -> dict[str, Any]:
    value = decode(content, REPLY_BYTES)
    proof = value.get("proof_sha256")
    if canonical(value) != canonical(reply(pin, phase, proof)) or (
        proof is not None
        and (not isinstance(proof, str) or re.fullmatch(r"[0-9a-f]{64}", proof) is None)
    ):
        raise BridgeError("unexpected_cleanup_reply")
    return value


def load_completion(
    output: Path, op: Operation, prepared: dict[str, Any], value: dict[str, Any]
) -> dict[str, Any]:
    if value["status"] != "phase_completed":
        raise BridgeError("cleanup_phase_not_completed")
    proof, pin = _read(output, PROOF)
    _require(pin, value["proof_sha256"])
    _require(proof, _collect(output, op, prepared))
    return proof
