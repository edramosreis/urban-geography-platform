"""Join caller/source verification, one owned query attempt and bounded Gold result checks."""

import hashlib
import json
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from athena_control import Attempt, canonical, execute_attempt, verified_plan, write_once
from athena_preflight import caller_identity, catalog_state, expected_caller, verify_query_source
from athena_read_plan import same
from athena_results import expected_gold, verify_query_result

from urban_geography.glue_runtime import runtime_plan

MAX_PLAN_BYTES = 64 * 1024


def pinned_record(path: Path, pin: str) -> dict[str, Any]:
    if (
        not isinstance(pin, str)
        or re.fullmatch(r"[0-9a-f]{64}", pin) is None
        or path.is_symlink()
        or not path.is_file()
    ):
        raise ValueError("Use an explicit pin and a regular local record")
    with path.open("rb") as stream:
        content = stream.read(MAX_PLAN_BYTES + 1)
    if len(content) > MAX_PLAN_BYTES or hashlib.sha256(content).hexdigest() != pin:
        raise ValueError("Local record checksum or size differs")
    value = json.loads(content)
    if not isinstance(value, dict):
        raise ValueError("Local record must be a JSON object")
    return value


def load_plan(path: Path, pin: str) -> dict[str, Any]:
    return verified_plan(pinned_record(path, pin))


def check_inputs(plan: dict[str, Any], input_path: Path, writer_receipt: Path, pin: str) -> None:
    expected = expected_gold(input_path / "summary.json", plan["binding"]["source_run_sha256"])
    claim = pinned_record(writer_receipt, pin)
    recipe = runtime_plan(**plan["target"])
    required = {
        "format_version": 2,
        "snapshot_id": plan["binding"]["snapshot_id"],
        "source_run_sha256": plan["binding"]["source_run_sha256"],
        "rows": expected["mapped_buildings"],
        "readback_matches": True,
        "table": recipe["catalog"] + "." + recipe["database"] + ".building_proximity",
        "verified_spark_settings": recipe["spark_settings"],
    }
    if claim.get("status") not in {"created", "reused"} or not same(
        {key: claim.get(key) for key in required}, required
    ):
        raise ValueError("Writer receipt does not match the planned cloud query/input")


def run_trial(
    clients: dict[str, Any],
    attempt: Attempt,
    *,
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    caller_arn: str,
    caller_user_id: str,
    snapshot_reader: Callable[..., dict[str, Any]],
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """One explicitly requested trial; never retries or replaces an uncertain submission."""
    plan = verified_plan(attempt.intent()["plan"])
    expected = expected_caller(plan["target"]["account_id"], caller_arn, caller_user_id)
    stage = "local_inputs"
    execution: dict[str, Any] | None = None
    try:
        check_inputs(plan, input_path, writer_receipt, writer_receipt_sha256)
        stage = "source_preflight"
        source = verify_query_source(
            clients["sts"],
            clients["glue"],
            plan,
            caller_arn=caller_arn,
            caller_user_id=caller_user_id,
            input_path=input_path,
            writer_receipt=writer_receipt,
            writer_receipt_sha256=writer_receipt_sha256,
            snapshot_reader=snapshot_reader,
        )
        attempt.intent()
        write_once(
            attempt.output / "source-verification.json",
            canonical({"intent_sha256": attempt.intent_sha256, **source}),
        )
        attempt.record({"event": "source_verified"})
        stage = "query_execution"
        execution = execute_attempt(clients["athena"], attempt, now=now, pause=pause)
        write_once(attempt.output / "execution.json", canonical(execution))
        if execution["outcome"] != "query_succeeded":
            report = {
                "status": "query_not_verified",
                "query_id": execution["query_id"],
                "execution_outcome": execution["outcome"],
                "terminal_confirmed": execution["terminal_confirmed"],
                "result_verified": False,
            }
            attempt.receipt(report)
            return report
        stage = "result_verification"
        result = verify_query_result(
            clients["athena"],
            clients["s3"],
            attempt,
            execution,
            summary_path=input_path / "summary.json",
        )
        write_once(attempt.output / "result-verification.json", canonical(result))
        stage = "final_source_observation"
        if not same(catalog_state(clients["glue"], plan), source["catalog"]):
            raise ValueError("Source catalog changed after query execution")
        caller_identity(clients["sts"], expected)
        check_inputs(plan, input_path, writer_receipt, writer_receipt_sha256)
        attempt.intent()
        report = {
            "status": "verified_trial",
            "query_id": execution["query_id"],
            "intent_sha256": attempt.intent_sha256,
            "binding": plan["binding"],
            "caller_identity_verified": True,
            "source_readback_verified": True,
            "result_verified": True,
            "writer_job_identity_verified": False,
            "terminal_confirmed": True,
            "row": result["row"],
            "data_scanned_bytes": result["data_scanned_bytes"],
            "scope": "Observed workflow; separate cloud approval required",
        }
        attempt.receipt(report)
        return report
    except BaseException as exc:
        # The existing controller owns query cancellation; do not issue a second stop here.
        try:
            write_once(
                attempt.output / "failure.json",
                canonical(
                    {
                        "stage": stage,
                        "error_type": type(exc).__name__,
                        "query_id": execution.get("query_id") if execution else None,
                        "result_verified": False,
                        "recovery_required": stage == "query_execution"
                        and execution is None
                        and (attempt.output / "submission.json").exists(),
                    }
                ),
            )
        except Exception:
            pass
        raise
