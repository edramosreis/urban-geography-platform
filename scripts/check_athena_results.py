"""Exercise completed-query result checks with pinned local reference bytes and SDK stubs."""

import argparse
import copy
import csv
import hashlib
import io
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

from athena_clients import query_clients
from athena_control import Attempt, canonical, execute_attempt, write_once
from athena_read_plan import ARTIFACTS, read_plan, tooling_sdk_pins
from athena_results import MAX_RESULT_BYTES, compare_gold_csv, expected_gold, verify_query_result
from check_athena_control import Clock, fake_execution


def check(
    summary: Path, source_pin: str, reference: Path, reference_pin: str, output: Path
) -> dict[str, Any]:
    expected = expected_gold(summary, source_pin)
    with reference.open("rb") as stream:
        content = stream.read(MAX_RESULT_BYTES + 1)
    if hashlib.sha256(content).hexdigest() != reference_pin:
        raise ValueError("Reference CSV differs from its explicit checksum")
    compare_gold_csv(content, expected)
    resolved, artifacts = output.resolve(), ARTIFACTS.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or resolved == artifacts
        or not resolved.is_relative_to(artifacts)
        or resolved.is_relative_to(summary.parent.resolve())
    ):
        raise ValueError(
            "Use fresh result-check output under artifacts, separate from source input"
        )
    plan = read_plan(
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=123,
        source_run_sha256=source_pin,
    )
    import boto3
    from botocore.exceptions import ClientError
    from botocore.response import StreamingBody
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="offline-testing",
        aws_secret_access_key="offline-testing",
        aws_session_token="offline-token",
        region_name="eu-west-1",
    )
    scenarios = {}
    with ExitStack() as watches:
        with patch.dict("os.environ", {"AWS_ENDPOINT_URL_S3": "https://example.invalid"}):
            with query_clients(session=session, include_results=True) as clients:
                closers = [
                    watches.enter_context(
                        patch.object(
                            c._endpoint.http_session, "close", wraps=c._endpoint.http_session.close
                        )
                    )
                    for c in clients.values()
                ]
                credentials = clients["athena"]._request_signer._credentials
                assert all(c._request_signer._credentials is credentials for c in clients.values())
                s3 = clients["s3"]
                assert s3.meta.endpoint_url == "https://s3.eu-west-1.amazonaws.com"
                assert (
                    s3.meta.config.signature_version == "s3v4"
                    and s3.meta.config.s3["addressing_style"] == "virtual"
                )
                assert s3.meta.config.retries["total_max_attempts"] == 1
                assert s3.meta.config.connect_timeout == 5 and s3.meta.config.read_timeout == 10
                for mode in (
                    "matched",
                    "oversized_head",
                    "object_changed",
                    "wrong_counts",
                    "wrong_query",
                    "changed_final_observation",
                    "bad_get_headers",
                ):
                    attempt = Attempt(output / mode, plan)
                    request = attempt.intent()["request"]
                    group = copy.deepcopy(plan["workgroup"])
                    group["State"] = "ENABLED"
                    group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                        "Athena engine version 3"
                    )
                    owned = {"QueryExecutionId": "query-001"}
                    data = content
                    if mode == "wrong_counts":
                        rows = list(csv.reader(io.StringIO(content.decode())))
                        rows[1][4] = str(int(rows[1][4]) + 1)
                        buffer = io.StringIO(newline="")
                        csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(
                            rows
                        )
                        data = buffer.getvalue().encode()
                    raw = io.BytesIO(data)
                    body = StreamingBody(raw, len(data))
                    fetched = mode not in {"oversized_head", "wrong_query", "object_changed"}
                    with ExitStack() as stack:
                        athena = stack.enter_context(Stubber(clients["athena"]))
                        objects = stack.enter_context(Stubber(s3))
                        athena.add_response(
                            "get_work_group", {"WorkGroup": group}, {"WorkGroup": group["Name"]}
                        )
                        athena.add_response("start_query_execution", owned, request)
                        athena.add_response(
                            "get_query_execution", fake_execution(request, "SUCCEEDED"), owned
                        )
                        before = fake_execution(request, "SUCCEEDED")
                        if mode == "wrong_query":
                            before["QueryExecution"]["Query"] = "SELECT 1"
                        athena.add_response("get_query_execution", before, owned)
                        target = {
                            "Bucket": "urban-geo-test-results-123456789012-eu-west-1",
                            "Key": "queries/query-001.csv",
                            "ExpectedBucketOwner": "123456789012",
                        }
                        head = {
                            "ContentLength": len(data),
                            "ETag": '"offline-etag"',
                            "ServerSideEncryption": "AES256",
                            "ResponseMetadata": {"HTTPStatusCode": 200},
                        }
                        if mode != "wrong_query":
                            objects.add_response(
                                "head_object",
                                head
                                | (
                                    {"ContentLength": MAX_RESULT_BYTES + 1}
                                    if mode == "oversized_head"
                                    else {}
                                ),
                                target,
                            )
                        get_request = target | {
                            "IfMatch": '"offline-etag"',
                            "Range": f"bytes=0-{len(data) - 1}",
                        }
                        if mode == "object_changed":
                            objects.add_client_error(
                                "get_object",
                                service_error_code="PreconditionFailed",
                                http_status_code=412,
                                expected_params=get_request,
                            )
                        elif fetched:
                            response = head | {
                                "Body": body,
                                "ContentRange": f"bytes 0-{len(data) - 1}/{len(data)}",
                                "ResponseMetadata": {"HTTPStatusCode": 206},
                            }
                            if mode == "bad_get_headers":
                                response["ContentLength"] = len(data) + 1
                            objects.add_response("get_object", response, get_request)
                        if mode in {"matched", "changed_final_observation"}:
                            after = fake_execution(
                                request,
                                "SUCCEEDED",
                                scanned=4097 if mode == "changed_final_observation" else 4096,
                            )
                            athena.add_response("get_query_execution", after, owned)
                        clock = Clock()
                        execution = execute_attempt(
                            clients["athena"], attempt, now=clock.now, pause=clock.pause
                        )
                        try:
                            result = verify_query_result(
                                clients["athena"], s3, attempt, execution, summary_path=summary
                            )
                        except (ValueError, ClientError) as exc:
                            if mode == "matched":
                                raise
                            scenarios[mode] = {
                                "outcome": "rejected",
                                "error_type": type(exc).__name__,
                            }
                        else:
                            assert (
                                mode == "matched"
                                and result["row"]["mapped_buildings"]
                                == expected["mapped_buildings"]
                            )
                            write_once(
                                attempt.output / "result-verification.json", canonical(result)
                            )
                            scenarios[mode] = {
                                "outcome": "matched",
                                "row": result["row"],
                                "csv_bytes": result["result_object"]["bytes"],
                            }
                        if fetched:
                            assert raw.closed
                        else:
                            body.close()  # Fixture body never handed to the verifier.
                        athena.assert_no_pending_responses()
                        objects.assert_no_pending_responses()
        assert all(closer.call_count == 1 for closer in closers)
    report = {
        "sdk_pins": tooling_sdk_pins(),
        "source_summary_sha256": source_pin,
        "reference_csv_sha256": reference_pin,
        "scenarios": scenarios,
        "maximum_accepted_csv_bytes": MAX_RESULT_BYTES,
        "owned_clients_closed": 4,
        "stubbed_query_submissions": 7,
        "aws_requests": 0,
        "spark_started": False,
        "live_result_verified": False,
        "live_source_binding_verified": False,
        "scope": "Pinned local reference and SDK stubs only; no Athena execution or live S3 reads",
    }
    (output / "check.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
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
