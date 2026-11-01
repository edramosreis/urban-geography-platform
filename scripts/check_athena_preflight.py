"""Check caller/source-to-controller ordering with real SDK stubs and a fake Spark reader."""

import argparse
import copy
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from athena_clients import query_clients
from athena_control import Attempt, canonical, execute_attempt, write_once
from athena_preflight import verify_query_source
from athena_read_plan import ARTIFACTS, read_plan, tooling_sdk_pins
from check_athena_control import Clock, fake_execution

CALLER = {
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/urban-geo-test-reader/offline-session",
    "UserId": "AROA" + "A" * 17 + ":offline-session",
}


def fake_table(plan: dict[str, Any]) -> dict[str, Any]:
    location = "s3://urban-geo-test-lake-123456789012-eu-west-1/warehouse/building_proximity"
    return {
        "Table": {
            "CatalogId": "123456789012",
            "DatabaseName": plan["binding"]["source_schema"],
            "Name": "building_proximity",
            "TableType": "EXTERNAL_TABLE",
            "VersionId": "1",
            "Parameters": {
                "table_type": "ICEBERG",
                "metadata_location": location + "/metadata/00000-offline.metadata.json",
            },
            "StorageDescriptor": {"Location": location},
        }
    }


def fake_readback(plan: dict[str, Any]) -> dict[str, Any]:
    from urban_geography.glue_runtime import runtime_plan

    recipe = runtime_plan(**plan["target"])
    return {
        "status": "verified_snapshot_readback",
        "binding": plan["binding"],
        "table": "glue_catalog." + recipe["database"] + ".building_proximity",
        "location": recipe["warehouse"] + "building_proximity",
        "writer_receipt_sha256": "b" * 64,
        "verified_spark_settings": recipe["spark_settings"],
        "rows": 2,
        "table_uuid": "b6309a11-cc67-422d-8789-374651d36e67",
        "readback_matches": True,
        "metadata_stable": True,
        "verification_engine": "spark_iceberg",
    }


def check(output: Path) -> dict[str, Any]:
    resolved, artifacts = output.resolve(), ARTIFACTS.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or resolved == artifacts
        or not resolved.is_relative_to(artifacts)
    ):
        raise ValueError("Use fresh caller/source check output under artifacts")
    plan = read_plan(
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=123,
        source_run_sha256="a" * 64,
    )
    import boto3
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="offline-testing",
        aws_secret_access_key="offline-testing",
        aws_session_token="offline-token",
        region_name="eu-west-1",
    )
    outcomes = {}
    with ExitStack() as patches:
        with patch.dict(
            "os.environ",
            {
                "AWS_ENDPOINT_URL_STS": "https://example.invalid",
                "AWS_ENDPOINT_URL_GLUE": "https://example.invalid",
            },
        ):
            with query_clients(session=session) as clients:
                closers = [
                    patches.enter_context(
                        patch.object(
                            c._endpoint.http_session, "close", wraps=c._endpoint.http_session.close
                        )
                    )
                    for c in clients.values()
                ]
                credentials = clients["athena"]._request_signer._credentials
                for service, client in clients.items():
                    assert client._request_signer._credentials is credentials
                    assert client.meta.endpoint_url == f"https://{service}.eu-west-1.amazonaws.com"
                    assert client.meta.config.retries["total_max_attempts"] == 1
                    assert (
                        client.meta.config.connect_timeout == 5
                        and client.meta.config.read_timeout == 10
                    )
                for mode in (
                    "verified_then_control",
                    "wrong_caller",
                    "changed_catalog",
                    "wrong_snapshot",
                    "row_mismatch",
                    "changed_caller",
                    "disabled_workgroup",
                ):
                    attempt = Attempt(output / mode, plan)
                    source = fake_readback(plan)
                    before = fake_table(plan)
                    after = copy.deepcopy(before)
                    if mode == "wrong_snapshot":
                        source["binding"] = source["binding"] | {"snapshot_id": 124}
                    if mode == "changed_catalog":
                        after["Table"]["VersionId"] = "2"
                    reader = Mock(return_value=source)
                    if mode == "row_mismatch":
                        reader.side_effect = ValueError("synthetic row mismatch")
                    factory = Mock(side_effect=AssertionError("No Spark startup in SDK checker"))
                    with ExitStack() as stack:
                        sts = stack.enter_context(Stubber(clients["sts"]))
                        glue = stack.enter_context(Stubber(clients["glue"]))
                        athena = stack.enter_context(Stubber(clients["athena"]))
                        stack.enter_context(patch("athena_preflight.read_snapshot", reader))
                        sts.add_response(
                            "get_caller_identity",
                            CALLER
                            | ({"Account": "999999999999"} if mode == "wrong_caller" else {}),
                            {},
                        )
                        target = {
                            "CatalogId": "123456789012",
                            "DatabaseName": plan["binding"]["source_schema"],
                            "Name": "building_proximity",
                        }
                        if mode != "wrong_caller":
                            glue.add_response("get_table", before, target)
                        if mode in {
                            "verified_then_control",
                            "changed_catalog",
                            "changed_caller",
                            "disabled_workgroup",
                        }:
                            glue.add_response("get_table", after, target)
                        if mode in {
                            "verified_then_control",
                            "changed_caller",
                            "disabled_workgroup",
                        }:
                            sts.add_response(
                                "get_caller_identity",
                                CALLER
                                | (
                                    {"UserId": "AROA" + "B" * 17 + ":offline-session"}
                                    if mode == "changed_caller"
                                    else {}
                                ),
                                {},
                            )
                        if mode in {"verified_then_control", "disabled_workgroup"}:
                            group = copy.deepcopy(plan["workgroup"])
                            group["State"] = (
                                "DISABLED" if mode == "disabled_workgroup" else "ENABLED"
                            )
                            group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                                "Athena engine version 3"
                            )
                            athena.add_response(
                                "get_work_group", {"WorkGroup": group}, {"WorkGroup": group["Name"]}
                            )
                        if mode == "verified_then_control":
                            owned = {"QueryExecutionId": "query-001"}
                            athena.add_response(
                                "start_query_execution", owned, attempt.intent()["request"]
                            )
                            athena.add_response(
                                "get_query_execution",
                                fake_execution(attempt.intent()["request"], "SUCCEEDED"),
                                owned,
                            )
                        try:
                            verified = verify_query_source(
                                clients["sts"],
                                clients["glue"],
                                attempt.intent()["plan"],
                                caller_arn=CALLER["Arn"],
                                caller_user_id=CALLER["UserId"],
                                input_path=output / "synthetic-input",
                                writer_receipt=output / "synthetic-writer.json",
                                writer_receipt_sha256="b" * 64,
                                session_factory=factory,
                            )
                            write_once(
                                attempt.output / "source-verification.json",
                                canonical({"intent_sha256": attempt.intent_sha256, **verified}),
                            )
                            clock = Clock()
                            result = execute_attempt(
                                clients["athena"], attempt, now=clock.now, pause=clock.pause
                            )
                            assert (
                                mode == "verified_then_control"
                                and result["outcome"] == "query_succeeded"
                            )
                            attempt.receipt(result)
                            outcomes[mode] = (
                                "synthetic_source_verified_then_stubbed_query_succeeded"
                            )
                        except ValueError:
                            if mode == "verified_then_control":
                                raise
                            outcomes[mode] = "rejected_before_submission"
                            assert not (attempt.output / "submission.json").exists()
                        factory.assert_not_called()
                        for stub in (sts, glue, athena):
                            stub.assert_no_pending_responses()
        assert all(closer.call_count == 1 for closer in closers)
    report = {
        "sdk_pins": tooling_sdk_pins(),
        "scenarios": outcomes,
        "client_credential_snapshot_shared": True,
        "owned_clients_closed": 3,
        "stubbed_query_submissions": 1,
        "aws_requests": 0,
        "spark_started": False,
        "live_caller_identity_verified": False,
        "live_source_binding_verified": False,
        "result_verified": False,
        "scope": "Real SDK shapes/order; fake identity/catalog/reader only",
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
