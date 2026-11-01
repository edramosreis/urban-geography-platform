"""Exercise the joined runner with locked SDK stubs; Spark/cloud responses are synthetic."""

import argparse
import copy
import csv
import hashlib
import io
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from athena_clients import query_resources
from athena_control import Attempt, SubmissionUnknown, canonical, write_once
from athena_read_plan import ARTIFACTS, read_plan, tooling_sdk_pins
from athena_results import MAX_RESULT_BYTES, compare_gold_csv, expected_gold
from athena_runner import run_trial
from check_athena_control import Clock, fake_execution
from check_athena_preflight import CALLER, fake_readback, fake_table


def check(
    summary: Path, source_pin: str, reference: Path, reference_pin: str, output: Path
) -> dict[str, Any]:
    expected = expected_gold(summary, source_pin)
    with reference.open("rb") as stream:
        data = stream.read(MAX_RESULT_BYTES + 1)
    if hashlib.sha256(data).hexdigest() != reference_pin:
        raise ValueError("Reference CSV checksum differs")
    compare_gold_csv(data, expected)
    resolved = output.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
    ):
        raise ValueError("Use fresh runner SDK-check output under artifacts")
    plan = read_plan(
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=123,
        source_run_sha256=source_pin,
    )
    output.mkdir(parents=True)
    plan_bytes = canonical(plan)
    write_once(output / "plan.json", plan_bytes)
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
                "source_run_sha256": source_pin,
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
    from botocore.response import StreamingBody
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="offline-testing",
        aws_secret_access_key="offline-testing",
        aws_session_token="offline-token",
        region_name="eu-west-1",
    )
    outcomes = {}
    with ExitStack() as watches:
        with query_resources(session=session, include_results=True) as resources:
            environment = resources.reader_environment
            closers = [
                watches.enter_context(
                    patch.object(
                        c._endpoint.http_session, "close", wraps=c._endpoint.http_session.close
                    )
                )
                for c in resources.clients.values()
            ]
            clients = resources.clients
            assert (
                clients["athena"]._request_signer._credentials.access_key
                == environment["AWS_ACCESS_KEY_ID"]
            )
            for mode in (
                "verified",
                "wrong_caller",
                "reader_failed",
                "wrong_result",
                "catalog_changed",
                "submission_unknown",
                "query_failed",
            ):
                attempt = Attempt(output / mode, plan)
                request = attempt.intent()["request"]
                reader = Mock(return_value=copy.deepcopy(source))
                if mode == "reader_failed":
                    reader.side_effect = RuntimeError("synthetic reader failure")
                payload = data
                if mode == "wrong_result":
                    rows = list(csv.reader(io.StringIO(data.decode())))
                    rows[1][4] = str(int(rows[1][4]) + 1)
                    buffer = io.StringIO(newline="")
                    csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(rows)
                    payload = buffer.getvalue().encode()
                raw = io.BytesIO(payload)
                body = StreamingBody(raw, len(payload))
                with ExitStack() as stubs:
                    sts = stubs.enter_context(Stubber(clients["sts"]))
                    glue = stubs.enter_context(Stubber(clients["glue"]))
                    athena = stubs.enter_context(Stubber(clients["athena"]))
                    s3 = stubs.enter_context(Stubber(clients["s3"]))
                    sts.add_response(
                        "get_caller_identity",
                        CALLER | ({"Account": "999999999999"} if mode == "wrong_caller" else {}),
                        {},
                    )
                    target = {
                        "CatalogId": "123456789012",
                        "DatabaseName": plan["binding"]["source_schema"],
                        "Name": "building_proximity",
                    }
                    if mode != "wrong_caller":
                        glue.add_response("get_table", fake_table(plan), target)
                    passed_source = mode not in {"wrong_caller", "reader_failed"}
                    if passed_source:
                        glue.add_response("get_table", fake_table(plan), target)
                        sts.add_response("get_caller_identity", CALLER, {})
                        group = copy.deepcopy(plan["workgroup"])
                        group["State"] = "ENABLED"
                        group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                            "Athena engine version 3"
                        )
                        athena.add_response(
                            "get_work_group", {"WorkGroup": group}, {"WorkGroup": group["Name"]}
                        )
                        owned = {"QueryExecutionId": "query-001"}
                        if mode == "submission_unknown":
                            athena.add_client_error(
                                "start_query_execution",
                                service_error_code="InternalServerException",
                                http_status_code=500,
                                expected_params=request,
                            )
                        else:
                            athena.add_response("start_query_execution", owned, request)
                            athena.add_response(
                                "get_query_execution",
                                fake_execution(
                                    request, "FAILED" if mode == "query_failed" else "SUCCEEDED"
                                ),
                                owned,
                            )
                        if mode not in {"submission_unknown", "query_failed"}:
                            athena.add_response(
                                "get_query_execution", fake_execution(request, "SUCCEEDED"), owned
                            )
                            object_target = {
                                "Bucket": "urban-geo-test-results-123456789012-eu-west-1",
                                "Key": "queries/query-001.csv",
                                "ExpectedBucketOwner": "123456789012",
                            }
                            head = {
                                "ContentLength": len(payload),
                                "ETag": '"offline-etag"',
                                "ServerSideEncryption": "AES256",
                                "ResponseMetadata": {"HTTPStatusCode": 200},
                            }
                            s3.add_response("head_object", head, object_target)
                            s3.add_response(
                                "get_object",
                                head
                                | {
                                    "Body": body,
                                    "ContentRange": f"bytes 0-{len(payload) - 1}/{len(payload)}",
                                    "ResponseMetadata": {"HTTPStatusCode": 206},
                                },
                                object_target
                                | {
                                    "IfMatch": '"offline-etag"',
                                    "Range": f"bytes=0-{len(payload) - 1}",
                                },
                            )
                            if mode != "wrong_result":
                                athena.add_response(
                                    "get_query_execution",
                                    fake_execution(request, "SUCCEEDED"),
                                    owned,
                                )
                                final_table = fake_table(plan)
                                if mode == "catalog_changed":
                                    final_table["Table"]["VersionId"] = "2"
                                glue.add_response("get_table", final_table, target)
                                if mode == "verified":
                                    sts.add_response("get_caller_identity", CALLER, {})
                    clock = Clock()
                    try:
                        report = run_trial(
                            clients,
                            attempt,
                            input_path=summary.parent,
                            writer_receipt=receipt,
                            writer_receipt_sha256=receipt_pin,
                            caller_arn=CALLER["Arn"],
                            caller_user_id=CALLER["UserId"],
                            snapshot_reader=reader,
                            now=clock.now,
                            pause=clock.pause,
                        )
                    except (ValueError, RuntimeError, SubmissionUnknown) as exc:
                        if mode in {"verified", "query_failed"}:
                            raise
                        outcomes[mode] = {"status": "rejected", "error_type": type(exc).__name__}
                    else:
                        assert report["status"] == (
                            "verified_trial" if mode == "verified" else "query_not_verified"
                        )
                        outcomes[mode] = {
                            "status": report["status"],
                            "result_verified": report["result_verified"],
                        }
                    if mode in {"verified", "wrong_result", "catalog_changed"}:
                        assert raw.closed
                    else:
                        body.close()
                    for stub in (sts, glue, athena, s3):
                        stub.assert_no_pending_responses()
        assert environment == {} and all(c.call_count == 1 for c in closers)
    report = {
        "sdk_pins": tooling_sdk_pins(),
        "scenarios": outcomes,
        "plan_sha256": hashlib.sha256(plan_bytes).hexdigest(),
        "frozen_reader_environment_cleared": True,
        "owned_clients_closed": 4,
        "aws_requests": 0,
        "live_workflow_verified": False,
        "scope": "Runner with SDK stubs and fake Spark reader; no live AWS proof",
    }
    write_once(output / "check.json", canonical(report))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--source-run-sha256", required=True)
    parser.add_argument("--reference-csv", type=Path, required=True)
    parser.add_argument("--reference-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            check(
                args.summary,
                args.source_run_sha256,
                args.reference_csv,
                args.reference_sha256,
                args.output,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
