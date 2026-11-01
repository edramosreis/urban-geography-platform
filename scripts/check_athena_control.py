"""Exercise query lifecycle with the pinned SDK and Stubber only; never contact AWS."""

import argparse
import copy
import json
from pathlib import Path
from typing import Any

from athena_control import (
    MAX_OBSERVATIONS,
    MAX_STOP_OBSERVATIONS,
    Attempt,
    SubmissionUnknown,
    execute_attempt,
)
from athena_read_plan import ARTIFACTS, create_athena_client, read_plan, tooling_sdk_pins


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def now(self) -> float:
        return self.value

    def pause(self, seconds: float) -> None:
        self.value += seconds


def fake_execution(request: dict[str, Any], state: str, scanned: int = 4096) -> dict[str, Any]:
    """Wholly synthetic service data, validated against the actual SDK response model."""
    return {
        "QueryExecution": {
            "QueryExecutionId": "query-001",
            "Query": request["QueryString"],
            "WorkGroup": request["WorkGroup"],
            "QueryExecutionContext": request["QueryExecutionContext"],
            "StatementType": "DML",
            "SubstatementType": "SELECT",
            "Status": {"State": state},
            "EngineVersion": {
                "SelectedEngineVersion": "Athena engine version 3",
                "EffectiveEngineVersion": "Athena engine version 3",
            },
            "ResultConfiguration": request["ResultConfiguration"]
            | {
                "OutputLocation": request["ResultConfiguration"]["OutputLocation"] + "query-001.csv"
            },
            "ResultReuseConfiguration": request["ResultReuseConfiguration"],
            "Statistics": {
                "DataScannedInBytes": scanned,
                "ResultReuseInformation": {"ReusedPreviousResult": False},
            },
        }
    }


def check(output: Path) -> dict[str, Any]:
    resolved, artifacts = output.resolve(), ARTIFACTS.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or resolved == artifacts
        or not resolved.is_relative_to(artifacts)
    ):
        raise ValueError("Use fresh Athena controller check output under artifacts")
    plan = read_plan(
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=123,
        source_run_sha256="a" * 64,
    )
    # Optional packages are imported only for this explicit, offline SDK check.
    import boto3
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="offline-testing",
        aws_secret_access_key="offline-testing",
        region_name="eu-west-1",
    )
    client = create_athena_client(session=session)
    results = {}
    try:
        for mode in (
            "success",
            "watchdog_cancelled",
            "stop_unconfirmed",
            "scan_exceeded",
            "submission_unknown",
        ):
            attempt = Attempt(output / mode, plan)
            request = attempt.intent()["request"]
            group = copy.deepcopy(plan["workgroup"])
            group["State"] = "ENABLED"
            group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                "Athena engine version 3"
            )
            owned = {"QueryExecutionId": "query-001"}
            clock = Clock()
            with Stubber(client) as stub:
                stub.add_response(
                    "get_work_group", {"WorkGroup": group}, {"WorkGroup": group["Name"]}
                )
                if mode == "submission_unknown":
                    stub.add_client_error(
                        "start_query_execution",
                        service_error_code="InternalServerException",
                        http_status_code=500,
                        expected_params=request,
                    )
                else:
                    stub.add_response("start_query_execution", owned, request)
                    if mode == "success":
                        for state in ("QUEUED", "RUNNING", "QUEUED", "SUCCEEDED"):
                            stub.add_response(
                                "get_query_execution", fake_execution(request, state), owned
                            )
                    else:
                        if mode == "scan_exceeded":
                            scanned = (
                                plan["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"] + 1
                            )
                            stub.add_response(
                                "get_query_execution",
                                fake_execution(request, "RUNNING", scanned),
                                owned,
                            )
                        else:
                            for _ in range(MAX_OBSERVATIONS):
                                stub.add_response(
                                    "get_query_execution", fake_execution(request, "RUNNING"), owned
                                )
                        stub.add_response("stop_query_execution", {}, owned)
                        if mode == "stop_unconfirmed":
                            for _ in range(MAX_STOP_OBSERVATIONS):
                                stub.add_response(
                                    "get_query_execution", fake_execution(request, "RUNNING"), owned
                                )
                        else:
                            stub.add_response(
                                "get_query_execution", fake_execution(request, "CANCELLED"), owned
                            )
                try:
                    result = execute_attempt(client, attempt, now=clock.now, pause=clock.pause)
                except SubmissionUnknown:
                    if mode != "submission_unknown":
                        raise
                    result = {"outcome": "submission_unknown", "result_verified": False}
                expected = {
                    "success": "query_succeeded",
                    "watchdog_cancelled": "watchdog_limit",
                    "stop_unconfirmed": "stop_unconfirmed",
                    "scan_exceeded": "scan_limit_exceeded",
                    "submission_unknown": "submission_unknown",
                }[mode]
                assert result["outcome"] == expected and result["result_verified"] is False
                stub.assert_no_pending_responses()
            attempt.receipt(result)
            results[mode] = {"outcome": result["outcome"], "fake_elapsed_seconds": clock.value}
    finally:
        client.close()
    report = {
        "sdk_pins": tooling_sdk_pins(),
        "scenarios": results,
        "stubbed_start_calls": 5,
        "aws_requests": 0,
        "live_identity_verified": False,
        "provenance_verified": False,
        "result_verified": False,
        "scope": "SDK Stubber only; no service idempotency or live execution proof",
    }
    (output / "check.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(args.output), indent=2))


if __name__ == "__main__":
    main()
