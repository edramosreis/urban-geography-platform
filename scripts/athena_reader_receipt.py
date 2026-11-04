"""Bind a closed transport reply to bounded saved Athena workflow evidence."""

import hashlib
import math
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from athena_control import TERMINAL, canonical, write_once
from athena_preflight import expected_caller
from athena_read_plan import same
from athena_results import SHARE_TOLERANCE, expected_gold
from aws_login_bridge import BridgeError, decode
from aws_login_bridge import read_record as read_record

from urban_geography.glue_runtime import runtime_plan

MAX_RECORD_BYTES = 64 * 1024
REPLY_BYTES = 4096
BASE_FILES = {
    "intent.json",
    "submission.json",
    "query.json",
    "source-verification.json",
    "execution.json",
    "receipt.json",
}
PROOF = "operation-proof.json"


def _pin(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def operation_reply(
    pin: str, result: dict[str, Any] | None = None, proof: str | None = None
) -> dict[str, Any]:
    return {
        "version": 1,
        "plan_sha256": pin,
        "status": "operation_failed" if result is None else result["status"],
        "terminal_confirmed": False if result is None else result["terminal_confirmed"],
        "proof_sha256": proof,
    }


def verify_reply(content: bytes, pin: str) -> dict[str, Any]:
    value = decode(content, REPLY_BYTES)
    if (
        set(value) != {"version", "plan_sha256", "status", "terminal_confirmed", "proof_sha256"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["plan_sha256"] != pin
        or not _pin(pin)
        or type(value["terminal_confirmed"]) is not bool
        or value["status"] not in {"operation_failed", "verified_trial", "query_not_verified"}
    ):
        raise BridgeError("unexpected_reader_reply")
    if value["status"] == "operation_failed":
        if value["terminal_confirmed"] or value["proof_sha256"] is not None:
            raise BridgeError("unexpected_reader_reply")
    elif not _pin(value["proof_sha256"]) or (
        value["status"] == "verified_trial" and not value["terminal_confirmed"]
    ):
        raise BridgeError("unexpected_reader_reply")
    return value


def _row_matches(value: Any, expected: dict[str, Any]) -> bool:
    if not isinstance(value, dict) or set(value) != set(expected):
        return False
    share = value.get("share_within")
    return (
        same(
            {k: v for k, v in value.items() if k != "share_within"},
            {k: v for k, v in expected.items() if k != "share_within"},
        )
        and isinstance(share, (int, float))
        and not isinstance(share, bool)
        and math.isfinite(share)
        and math.isclose(share, expected["share_within"], rel_tol=0, abs_tol=SHARE_TOLERANCE)
    )


def _check(
    records: dict[str, dict[str, Any]],
    hashes: dict[str, str],
    plan: dict[str, Any],
    input_path: Path,
    writer_pin: str,
) -> dict[str, Any]:
    intent, source, execution, receipt = (
        records[n]
        for n in ("intent.json", "source-verification.json", "execution.json", "receipt.json")
    )
    intent_pin = hashes["intent.json"]
    request = intent.get("request", {})
    token = request.get("ClientRequestToken")
    query_id = execution.get("query_id")
    if (
        not same(
            {k: intent.get(k) for k in ("contract_version", "plan")},
            {"contract_version": 1, "plan": plan},
        )
        or not _pin(token)
        or not same(request, plan["request_template"] | {"ClientRequestToken": token})
        or not same(records["submission.json"], {"intent_sha256": intent_pin})
        or not isinstance(query_id, str)
        or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query_id) is None
        or not same(records["query.json"], {"query_id": query_id})
        or execution.get("intent_sha256") != intent_pin
        or execution.get("query_sha256") != plan["query_sha256"]
    ):
        raise BridgeError("reader_attempt_binding_differs")
    required_source = {
        "contract_version": 1,
        "status": "query_source_checked",
        "intent_sha256": intent_pin,
        "binding": plan["binding"],
        "query_sha256": plan["query_sha256"],
        "caller_identity_verified": True,
        "source_readback_verified": True,
        "catalog_stable": True,
        "writer_job_identity_verified": False,
        "result_verified": False,
    }
    if not same({k: source.get(k) for k in required_source}, required_source):
        raise BridgeError("reader_source_binding_differs")
    caller = source.get("caller", {})
    owner, deployment = plan["target"]["account_id"], plan["target"]["deployment_name"]
    arn, user_id = caller.get("Arn"), caller.get("UserId")
    if not isinstance(arn, str) or not isinstance(user_id, str):
        raise BridgeError("reader_caller_binding_differs")
    if (
        not same(caller, expected_caller(owner, arn, user_id))
        or re.fullmatch(
            re.escape(f"arn:aws:sts::{owner}:assumed-role/{deployment}-athena-reader/")
            + r"athena-reader-[0-9a-f]{32}",
            caller["Arn"],
        )
        is None
    ):
        raise BridgeError("reader_caller_binding_differs")
    recipe = runtime_plan(**plan["target"])
    row = expected_gold(input_path / "summary.json", plan["binding"]["source_run_sha256"])
    readback = source.get("source_readback", {})
    expected_readback = {
        "status": "verified_snapshot_readback",
        "binding": plan["binding"],
        "table": recipe["catalog"] + "." + recipe["database"] + ".building_proximity",
        "location": recipe["warehouse"] + "building_proximity",
        "writer_receipt_sha256": writer_pin,
        "verified_spark_settings": recipe["spark_settings"],
        "readback_matches": True,
        "metadata_stable": True,
        "verification_engine": "spark_iceberg",
        "rows": row["mapped_buildings"],
    }
    if not same({k: readback.get(k) for k in expected_readback}, expected_readback):
        raise BridgeError("reader_readback_binding_differs")
    catalog = source.get("catalog", {})
    if (
        not same(
            {k: catalog.get(k) for k in ("CatalogId", "DatabaseName", "Name")},
            {"CatalogId": owner, "DatabaseName": recipe["database"], "Name": "building_proximity"},
        )
        or catalog.get("StorageDescriptor", {}).get("Location", "").rstrip("/")
        != expected_readback["location"]
    ):
        raise BridgeError("reader_catalog_binding_differs")
    raw_last = execution.get("last_observation")
    last = raw_last if isinstance(raw_last, dict) else {}
    terminal = last.get("state") in TERMINAL
    if (
        not same(execution.get("terminal_confirmed"), terminal)
        or not same(receipt.get("terminal_confirmed"), terminal)
        or receipt.get("query_id") != query_id
    ):
        raise BridgeError("reader_terminal_binding_differs")
    verified = receipt.get("status") == "verified_trial"
    if verified:
        observed_row = receipt.get("row")
        if not _row_matches(observed_row, row):
            raise BridgeError("reader_result_binding_differs")
        expected = {
            "status": "verified_trial",
            "intent_sha256": intent_pin,
            "binding": plan["binding"],
            "caller_identity_verified": True,
            "source_readback_verified": True,
            "result_verified": True,
            "writer_job_identity_verified": False,
            "terminal_confirmed": True,
            "row": observed_row,
        }
        result = records["result-verification.json"]
        expected_result = {
            "contract_version": 1,
            "status": "gold_result_matches",
            "query_id": query_id,
            "intent_sha256": intent_pin,
            "query_sha256": plan["query_sha256"],
            "binding": plan["binding"],
            "row": observed_row,
            "expected": row,
            "result_verified": True,
        }
        scanned = receipt.get("data_scanned_bytes")
        if (
            not same({k: receipt.get(k) for k in expected}, expected)
            or not same({k: result.get(k) for k in expected_result}, expected_result)
            or execution.get("outcome") != "query_succeeded"
            or not terminal
            or last["state"] != "SUCCEEDED"
            or last.get("drift")
            or last.get("missing")
            or type(scanned) is not int
            or not 0 <= scanned <= plan["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"]
            or not same(result.get("data_scanned_bytes"), scanned)
            or not same(last.get("data_scanned_bytes"), scanned)
        ):
            raise BridgeError("reader_result_binding_differs")
        obj = result.get("result_object", {})
        prefix = urlsplit(plan["request_template"]["ResultConfiguration"]["OutputLocation"])
        if (
            obj.get("bucket") != prefix.netloc
            or obj.get("key") != prefix.path.lstrip("/") + query_id + ".csv"
            or not _pin(obj.get("sha256"))
            or type(obj.get("bytes")) is not int
            or not 0 < obj["bytes"] <= MAX_RECORD_BYTES
            or not isinstance(obj.get("etag"), str)
            or re.fullmatch(r'"[A-Za-z0-9-]{1,128}"', obj["etag"]) is None
        ):
            raise BridgeError("reader_result_object_differs")
    elif (
        receipt.get("status") != "query_not_verified"
        or receipt.get("result_verified") is not False
        or receipt.get("execution_outcome") != execution.get("outcome")
        or execution.get("outcome") == "query_succeeded"
    ):
        raise BridgeError("reader_failure_binding_differs")
    return {
        "status": receipt["status"],
        "terminal_confirmed": terminal,
        "result_verified": verified,
    }


def _records(output: Path, verified: bool) -> tuple[dict[str, Any], dict[str, str]]:
    if output.is_symlink() or not output.is_dir():
        raise BridgeError("reader_attempt_not_regular")
    names = BASE_FILES | ({"result-verification.json"} if verified else set())
    pairs = {name: read_record(output / name) for name in names}
    return {k: v[0] for k, v in pairs.items()}, {k: v[1] for k, v in pairs.items()}


def save_completion(
    output: Path,
    plan: dict[str, Any],
    pin: str,
    input_path: Path,
    writer_pin: str,
    result: dict[str, Any],
) -> str:
    records, hashes = _records(output, result["status"] == "verified_trial")
    _check(records, hashes, plan, input_path, writer_pin)
    if not same(records["receipt.json"], result):
        raise BridgeError("reader_returned_receipt_differs")
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
    records, hashes = _records(output, reply["status"] == "verified_trial")
    expected = {
        "version": 1,
        "plan_sha256": pin,
        "writer_receipt_sha256": writer_pin,
        "files": hashes,
    }
    if proof_pin != reply["proof_sha256"] or not same(proof, expected):
        raise BridgeError("reader_proof_binding_differs")
    result = _check(records, hashes, plan, input_path, writer_pin)
    if (
        result["status"] != reply["status"]
        or result["terminal_confirmed"] != reply["terminal_confirmed"]
    ):
        raise BridgeError("reader_reply_binding_differs")
    return result
