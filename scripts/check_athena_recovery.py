"""Check exact-token recovery with the locked SDK and synthetic original/replay outcomes."""

import argparse
import copy
import hashlib
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from athena_clients import query_resources
from athena_control import Attempt, SubmissionUnknown, canonical, write_once
from athena_read_plan import ARTIFACTS, read_plan, tooling_sdk_pins
from athena_recovery import CLAIM, inspect_submission, recover_submission
from athena_results import expected_gold
from athena_runner import run_trial
from check_athena_control import Clock, fake_execution
from check_athena_preflight import CALLER, fake_readback, fake_table


def check(summary: Path, pin: str, output: Path) -> dict[str, Any]:
    expected = expected_gold(summary, pin)
    resolved = output.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
    ):
        raise ValueError("Use fresh recovery SDK-check output under artifacts")
    plan = read_plan(
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=123,
        source_run_sha256=pin,
    )
    output.mkdir(parents=True)
    fixtures = output / "fixtures"
    fixtures.mkdir()
    source = fake_readback(plan)
    receipt = fixtures / "writer.json"
    write_once(
        receipt,
        canonical(
            {
                "status": "created",
                "format_version": 2,
                "snapshot_id": 123,
                "source_run_sha256": pin,
                "rows": expected["mapped_buildings"],
                "readback_matches": True,
                "table": source["table"],
                "verified_spark_settings": source["verified_spark_settings"],
            }
        ),
    )
    receipt_pin = hashlib.sha256(receipt.read_bytes()).hexdigest()
    source.update(rows=expected["mapped_buildings"], writer_receipt_sha256=receipt_pin)
    import boto3
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="offline-testing",
        aws_secret_access_key="offline-testing",
        aws_session_token="offline-token",
        region_name="eu-west-1",
    )
    scenarios: dict[str, Any] = {}
    plan_example = {}
    with ExitStack() as watches:
        with query_resources(session=session, include_results=True) as resources:
            clients = resources.clients
            closers = [
                watches.enter_context(
                    patch.object(
                        c._endpoint.http_session, "close", wraps=c._endpoint.http_session.close
                    )
                )
                for c in clients.values()
            ]
            for mode in (
                "active_cancelled",
                "already_succeeded",
                "not_received_cancelled",
                "stop_unconfirmed",
                "replay_unknown",
                "catalog_changed",
                "disabled_workgroup",
                "wrong_caller",
                "plan_only",
            ):
                original = Attempt(output / mode / "original", plan)
                request = original.intent()["request"]
                reader = Mock(return_value=copy.deepcopy(source))
                group = copy.deepcopy(plan["workgroup"])
                group["State"] = "ENABLED"
                group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                    "Athena engine version 3"
                )
                target = {
                    "CatalogId": "123456789012",
                    "DatabaseName": plan["binding"]["source_schema"],
                    "Name": "building_proximity",
                }
                clock = Clock()
                with ExitStack() as stack:
                    sts = stack.enter_context(Stubber(clients["sts"]))
                    glue = stack.enter_context(Stubber(clients["glue"]))
                    athena = stack.enter_context(Stubber(clients["athena"]))
                    s3 = stack.enter_context(Stubber(clients["s3"]))
                    sts.add_response("get_caller_identity", CALLER, {})
                    glue.add_response("get_table", fake_table(plan), target)
                    glue.add_response("get_table", fake_table(plan), target)
                    sts.add_response("get_caller_identity", CALLER, {})
                    athena.add_response(
                        "get_work_group", {"WorkGroup": group}, {"WorkGroup": group["Name"]}
                    )
                    athena.add_client_error(
                        "start_query_execution",
                        service_error_code="InternalServerException",
                        http_status_code=500,
                        expected_params=request,
                    )
                    try:
                        run_trial(
                            clients,
                            original,
                            input_path=summary.parent,
                            writer_receipt=receipt,
                            writer_receipt_sha256=receipt_pin,
                            caller_arn=CALLER["Arn"],
                            caller_user_id=CALLER["UserId"],
                            snapshot_reader=reader,
                            now=clock.now,
                            pause=clock.pause,
                        )
                    except SubmissionUnknown:
                        pass
                    else:
                        raise AssertionError("Expected uncertain original submission")
                    before = {
                        p.name: p.read_bytes() for p in original.output.iterdir() if p.is_file()
                    }
                    saved = inspect_submission(original.output, original.intent_sha256)
                    if mode == "plan_only":
                        plan_example = {
                            "attempt": str(original.output.resolve()),
                            "intent_sha256": original.intent_sha256,
                        }
                        write_once(output / "plan-example.json", canonical(plan_example))
                        assert saved.plan()["execution_authorized"] is False
                    else:
                        sts.add_response(
                            "get_caller_identity",
                            CALLER
                            | ({"Account": "999999999999"} if mode == "wrong_caller" else {}),
                            {},
                        )
                        if mode != "wrong_caller":
                            current = fake_table(plan)
                            if mode == "catalog_changed":
                                current["Table"]["VersionId"] = "2"
                            glue.add_response("get_table", current, target)
                        if mode not in {"wrong_caller", "catalog_changed"}:
                            current_group = copy.deepcopy(group)
                            if mode == "disabled_workgroup":
                                current_group["State"] = "DISABLED"
                            athena.add_response(
                                "get_work_group",
                                {"WorkGroup": current_group},
                                {"WorkGroup": group["Name"]},
                            )
                        will_replay = mode not in {
                            "wrong_caller",
                            "catalog_changed",
                            "disabled_workgroup",
                        }
                        query_id = (
                            "query-new" if mode == "not_received_cancelled" else "query-original"
                        )
                        owned = {"QueryExecutionId": query_id}

                        def response(
                            state: str, request: dict[str, Any] = request, query_id: str = query_id
                        ) -> dict[str, Any]:
                            value = fake_execution(request, state)
                            value["QueryExecution"]["QueryExecutionId"] = query_id
                            value["QueryExecution"]["ResultConfiguration"]["OutputLocation"] = (
                                request["ResultConfiguration"]["OutputLocation"] + query_id + ".csv"
                            )
                            return value

                        if will_replay:
                            if mode == "replay_unknown":
                                athena.add_client_error(
                                    "start_query_execution",
                                    service_error_code="InternalServerException",
                                    http_status_code=500,
                                    expected_params=request,
                                )
                            else:
                                # Replay the identical request and token.
                                athena.add_response("start_query_execution", owned, request)
                                athena.add_response(
                                    "get_query_execution",
                                    response(
                                        "SUCCEEDED" if mode == "already_succeeded" else "RUNNING"
                                    ),
                                    owned,
                                )
                                if mode != "already_succeeded":
                                    athena.add_response("stop_query_execution", {}, owned)
                                    if mode == "stop_unconfirmed":
                                        for _ in range(12):
                                            athena.add_response(
                                                "get_query_execution", response("RUNNING"), owned
                                            )
                                    else:
                                        athena.add_response(
                                            "get_query_execution", response("CANCELLED"), owned
                                        )
                        recovery_output = output / mode / "recovery"
                        try:
                            result = recover_submission(
                                clients,
                                original.output,
                                original.intent_sha256,
                                recovery_output,
                                caller_arn=CALLER["Arn"],
                                caller_user_id=CALLER["UserId"],
                                now=clock.now,
                                pause=clock.pause,
                            )
                        except (ValueError, SubmissionUnknown) as exc:
                            if mode not in {
                                "wrong_caller",
                                "catalog_changed",
                                "disabled_workgroup",
                                "replay_unknown",
                            }:
                                raise
                            scenarios[mode] = {
                                "status": "rejected_or_unknown",
                                "error_type": type(exc).__name__,
                            }
                        else:
                            assert result["status"] == (
                                "stop_unconfirmed"
                                if mode == "stop_unconfirmed"
                                else "reconciled_terminal"
                            )
                            assert result["result_verified"] is False
                            scenarios[mode] = {
                                "status": result["status"],
                                "terminal_confirmed": result["terminal_confirmed"],
                                "stop_attempted": result["stop_attempted"],
                                "fake_elapsed_seconds": clock.value,
                            }
                        if mode == "replay_unknown":
                            try:
                                recover_submission(
                                    clients,
                                    original.output,
                                    original.intent_sha256,
                                    output / mode / "second-recovery",
                                    caller_arn=CALLER["Arn"],
                                    caller_user_id=CALLER["UserId"],
                                    now=clock.now,
                                    pause=clock.pause,
                                )
                            except ValueError:
                                pass
                            else:
                                raise AssertionError("Second replay must be refused")
                        assert (original.output / CLAIM).exists() is will_replay
                    assert all(
                        (original.output / name).read_bytes() == content
                        for name, content in before.items()
                    )
                    for stub in (sts, glue, athena, s3):
                        stub.assert_no_pending_responses()
        assert resources.reader_environment == {} and all(c.call_count == 1 for c in closers)
    report = {
        "sdk_pins": tooling_sdk_pins(),
        "scenarios": scenarios,
        "plan_example": plan_example,
        "aws_requests": 0,
        "spark_started": False,
        "live_idempotency_verified": False,
        "original_proof_bytes_unchanged": True,
        "owned_clients_closed": 4,
        "scope": "SDK shapes and synthetic outcomes; live idempotency unverified",
    }
    write_once(output / "check.json", canonical(report))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--source-run-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(args.summary, args.source_run_sha256, args.output), indent=2))


if __name__ == "__main__":
    main()
