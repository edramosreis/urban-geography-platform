"""Bind a small dbt transport reply to fixed, bounded saved completion records."""

import hashlib
import re
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from athena_preflight import expected_caller
from athena_read_plan import same
from athena_results import expected_gold
from aws_login_bridge import BridgeError, decode, read_record
from dbt_view_plan import VIEWS, normalized_sql

from urban_geography.glue_runtime import runtime_plan

REPLY_BYTES = 4096
PROOF = "operation-proof.json"
FILES = {
    "intent.json",
    "source-verification.json",
    "submission-gate-01.json",
    "submission-gate-02.json",
    "final-gate.json",
    "model-outcome.json",
    "receipt.json",
    "builder-operation.json",
    "queries/intent.json",
    "dbt/worker-request.json",
    "dbt/process.json",
    "dbt/worker-report.json",
} | {
    f"queries/query-{i:02}/{name}.json"
    for i in (1, 2)
    for name in ("intent", "submission", "query", "receipt")
}


def _pin(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def operation_reply(pin: str, proof: str | None = None) -> dict[str, Any]:
    return {
        "version": 1,
        "plan_sha256": pin,
        "status": "operation_failed" if proof is None else "scoped_view_build_completed",
        "models_succeeded": proof is not None,
        "proof_sha256": proof,
    }


def verify_reply(content: bytes, pin: str) -> dict[str, Any]:
    value = decode(content, REPLY_BYTES)
    if (
        set(value) != {"version", "plan_sha256", "status", "models_succeeded", "proof_sha256"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["plan_sha256"] != pin
        or not _pin(pin)
        or type(value["models_succeeded"]) is not bool
        or value["status"] not in {"operation_failed", "scoped_view_build_completed"}
        or (
            value["status"] == "operation_failed"
            and (value["models_succeeded"] or value["proof_sha256"] is not None)
        )
        or (
            value["status"] == "scoped_view_build_completed"
            and (not value["models_succeeded"] or not _pin(value["proof_sha256"]))
        )
    ):
        raise BridgeError("unexpected_builder_reply")
    return value


def _require(actual: Any, expected: Any) -> None:
    if not same(actual, expected):
        raise BridgeError("builder_saved_binding_differs")


def _fields(value: dict[str, Any], expected: dict[str, Any]) -> None:
    _require({key: value.get(key) for key in expected}, expected)


def _validate(
    r: dict[str, dict[str, Any]],
    h: dict[str, str],
    plan: dict[str, Any],
    pin: str,
    input_path: Path,
    writer_pin: str,
) -> None:
    source_plan = plan["source_plan"]
    owner, deployment = (source_plan["target"][k] for k in ("account_id", "deployment_name"))
    intent, receipt = r["intent.json"], r["receipt.json"]
    caller = intent["expected_caller"]
    _require(caller, expected_caller(owner, caller["Arn"], caller["UserId"]))
    if (
        re.fullmatch(
            re.escape(f"arn:aws:sts::{owner}:assumed-role/{deployment}-dbt-view-builder/")
            + r"dbt-builder-[0-9a-f]{32}",
            caller["Arn"],
        )
        is None
    ):
        raise BridgeError("builder_saved_caller_differs")
    _fields(
        intent,
        {
            "contract_version": 1,
            "view_plan": plan,
            "view_readback": False,
            "gold_result_plan": None,
            "data_test_plan": None,
            "writer_receipt_sha256": writer_pin,
        },
    )
    root_pin, source_pin = h["intent.json"], h["source-verification.json"]
    _require(r["source-verification.json"]["run_intent_sha256"], root_pin)
    source = r["source-verification.json"]["source"]
    _fields(
        source,
        {
            "contract_version": 1,
            "status": "query_source_checked",
            "binding": source_plan["binding"],
            "query_sha256": source_plan["query_sha256"],
            "caller": caller,
            "caller_identity_verified": True,
            "source_readback_verified": True,
            "catalog_stable": True,
            "writer_job_identity_verified": False,
            "result_verified": False,
            "queries_submitted": 0,
            "execution_authorized": False,
        },
    )
    recipe = runtime_plan(**source_plan["target"])
    row = expected_gold(input_path / "summary.json", source_plan["binding"]["source_run_sha256"])
    _fields(
        source["source_readback"],
        {
            "status": "verified_snapshot_readback",
            "binding": source_plan["binding"],
            "table": recipe["catalog"] + "." + recipe["database"] + ".building_proximity",
            "location": recipe["warehouse"] + "building_proximity",
            "writer_receipt_sha256": writer_pin,
            "verified_spark_settings": recipe["spark_settings"],
            "readback_matches": True,
            "metadata_stable": True,
            "verification_engine": "spark_iceberg",
            "rows": row["mapped_buildings"],
        },
    )
    catalog = source["catalog"]
    _fields(
        catalog,
        {"CatalogId": owner, "DatabaseName": recipe["database"], "Name": "building_proximity"},
    )
    _require(
        catalog["StorageDescriptor"]["Location"].rstrip("/"),
        recipe["warehouse"] + "building_proximity",
    )
    for name in ("submission-gate-01.json", "submission-gate-02.json", "final-gate.json"):
        _require(
            r[name],
            {
                "run_intent_sha256": root_pin,
                "source_verification_sha256": source_pin,
                "caller": caller,
                "catalog": catalog,
            },
        )
    view_pin = h["queries/intent.json"]
    _require(r["queries/intent.json"], plan)
    _require(view_pin, hashlib.sha256(canonical(plan)).hexdigest())
    queries = []
    tokens, identities = set(), set()
    scans = 0
    for i, view in enumerate(VIEWS, 1):
        if (
            scans + source_plan["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"]
            > plan["limits"]["max_total_scan_bytes"]
        ):
            raise BridgeError("builder_saved_scan_differs")
        prefix = f"queries/query-{i:02}/"
        saved, step = r[prefix + "intent.json"], r[prefix + "receipt.json"]
        request = saved["request"]
        sql, token = request["QueryString"], request["ClientRequestToken"]
        if (
            not isinstance(sql, str)
            or len(sql.encode()) > plan["limits"]["max_query_bytes"]
            or normalized_sql(sql) != normalized_sql(plan["queries"][i - 1]["sql"])
            or not _pin(token)
            or token in tokens
        ):
            raise BridgeError("builder_saved_query_differs")
        tokens.add(token)
        sql_pin = hashlib.sha256(sql.encode()).hexdigest()
        _require(
            request,
            source_plan["request_template"] | {"QueryString": sql, "ClientRequestToken": token},
        )
        _require(
            saved,
            {
                "contract_version": 1,
                "parent_intent_sha256": view_pin,
                "plan": {"query_sha256": sql_pin, "workgroup": source_plan["workgroup"]},
                "request": request,
            },
        )
        query_pin = h[prefix + "intent.json"]
        _require(r[prefix + "submission.json"], {"intent_sha256": query_pin})
        identity = step["query_id"]
        if (
            not isinstance(identity, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", identity) is None
            or identity in identities
        ):
            raise BridgeError("builder_saved_query_identity_differs")
        identities.add(identity)
        _require(r[prefix + "query.json"], {"query_id": identity})
        scanned = step["last_observation"]["data_scanned_bytes"]
        if type(scanned) is not int or scanned < 0:
            raise BridgeError("builder_saved_scan_differs")
        scans += scanned
        _fields(
            step,
            {
                "intent_sha256": query_pin,
                "query_sha256": sql_pin,
                "outcome": "query_succeeded",
                "reason": None,
                "terminal_confirmed": True,
                "stop_attempted": False,
                "stop_acknowledged": False,
                "journal_errors": [],
                "caller_identity_verified": False,
                "provenance_verified": False,
                "result_verified": False,
                "last_observation": {
                    "state": "SUCCEEDED",
                    "drift": [],
                    "missing": [],
                    "data_scanned_bytes": scanned,
                },
            },
        )
        queries.append(step | {"name": view})
    if scans > plan["limits"]["max_total_scan_bytes"]:
        raise BridgeError("builder_saved_scan_differs")
    _fields(
        receipt,
        {
            "status": "source_checked_view_build",
            "run_intent_sha256": root_pin,
            "source_verification_sha256": source_pin,
            "final_gate_sha256": h["final-gate.json"],
            "binding": source_plan["binding"],
            "caller_identity_verified": True,
            "source_readback_verified": True,
            "source_catalog_stable": True,
            "controlled_view_queries_completed": True,
            "models_succeeded": True,
            "data_tests_executed": 0,
            "data_tests_passed": False,
            "test_control": None,
            "view_catalog_verified": False,
            "view_catalog_initial_sha256": None,
            "view_catalog_final_sha256": None,
            "result_verified": False,
            "gold_control": None,
            "gold_result_verification_sha256": None,
            "result_scope": None,
            "writer_job_identity_verified": False,
        },
    )
    _fields(
        receipt["query_control"],
        {
            "status": "view_queries_completed",
            "intent_sha256": view_pin,
            "blocked": False,
            "queries_reserved": 2,
            "data_scanned_bytes": scans,
            "queries": queries,
            "caller_identity_verified": False,
            "source_readback_verified": False,
            "result_verified": False,
        },
    )
    outcome = {"success": True, "model_statuses": {name: "success" for name in VIEWS}}
    _require(r["model-outcome.json"], outcome)
    request = r["dbt/worker-request.json"]
    if set(request) != {
        "version",
        "project",
        "output",
        "view_plan",
        "controller_intent_sha256",
    } or not all(isinstance(request[k], str) and request[k] for k in ("project", "output")):
        raise BridgeError("builder_worker_request_differs")
    _fields(request, {"version": 1, "view_plan": plan, "controller_intent_sha256": view_pin})
    process = r["dbt/process.json"]
    count, metadata = process["ipc_requests"], process["metadata_requests"]
    if (
        type(count) is not int
        or not 1 <= count <= 64
        or type(metadata) is not int
        or not 0 <= metadata <= min(32, count)
        or type(process["pid"]) is not int
        or process["pid"] <= 0
    ):
        raise BridgeError("builder_worker_process_differs")
    _fields(
        process,
        {
            "returncode": 0,
            "owned_group_stopped": True,
            "credentials_sent": False,
            "failure_type": None,
        },
    )
    _require(
        r["dbt/worker-report.json"],
        {
            "request_sha256": h["dbt/worker-request.json"],
            "binding_restored": True,
            "credentials_received": False,
            "network_attempts": 0,
            "ipc_requests": count,
            "outcome": outcome,
        },
    )
    _require(
        r["builder-operation.json"],
        {
            "version": 1,
            "status": "scoped_view_build_completed",
            "view_plan_sha256": pin,
            "project_receipt_sha256": plan["project_receipt_sha256"],
            "writer_receipt_sha256": writer_pin,
            "binding": source_plan["binding"],
            "caller": caller,
            "run_intent_sha256": root_pin,
            "receipt_sha256": h["receipt.json"],
        },
    )


def _records(output: Path) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    for name in (".", "queries", "queries/query-01", "queries/query-02", "dbt"):
        directory = output / name
        if directory.is_symlink() or not directory.is_dir():
            raise BridgeError("builder_attempt_not_regular")
    pairs = {name: read_record(output / name) for name in FILES}
    return {k: v[0] for k, v in pairs.items()}, {k: v[1] for k, v in pairs.items()}


def _check(
    records: dict[str, dict[str, Any]],
    hashes: dict[str, str],
    plan: dict[str, Any],
    pin: str,
    input_path: Path,
    writer_pin: str,
) -> None:
    try:
        _validate(records, hashes, plan, pin, input_path, writer_pin)
    except (KeyError, TypeError, AttributeError, IndexError):
        raise BridgeError("builder_saved_records_malformed") from None


def save_completion(
    output: Path,
    plan: dict[str, Any],
    pin: str,
    input_path: Path,
    writer_pin: str,
    result: dict[str, Any],
) -> str:
    records, hashes = _records(output)
    _check(records, hashes, plan, pin, input_path, writer_pin)
    _require(records["receipt.json"], result)
    content = canonical(
        {"version": 1, "plan_sha256": pin, "writer_receipt_sha256": writer_pin, "files": hashes}
    )
    write_once(output / PROOF, content)
    return hashlib.sha256(content).hexdigest()


def load_completion(
    output: Path,
    plan: dict[str, Any],
    pin: str,
    input_path: Path,
    writer_pin: str,
    reply: dict[str, Any],
) -> dict[str, Any]:
    proof, proof_pin = read_record(output / PROOF)
    records, hashes = _records(output)
    _require(proof_pin, reply["proof_sha256"])
    _require(
        proof,
        {"version": 1, "plan_sha256": pin, "writer_receipt_sha256": writer_pin, "files": hashes},
    )
    _check(records, hashes, plan, pin, input_path, writer_pin)
    _require(reply, operation_reply(pin, proof_pin))
    return {
        "status": "scoped_view_build_completed",
        "models_succeeded": True,
        "result_verified": False,
    }
