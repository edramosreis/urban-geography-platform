"""Rehearse named infrastructure absence with dummy keys and the pinned SDK Stubber only."""

import argparse
import json
import os
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import trial_infrastructure_operation as operation
from aws_login_bridge import envelope, temporary
from trial_cleanup_inventory import validate_inventory_output
from trial_infrastructure_plan import SERVICES, absence_plan, plan_sha256

MODES = (
    "all_absent",
    "empty_bucket",
    "denied_policy",
    "wrong_caller",
    "paged_athena",
    "ambiguous_athena",
    "truncated_logs",
    "remaining_role",
    "federated_database",
)


def check(release: Path, pin: str, principal: str, output: Path) -> dict[str, Any]:
    plan = absence_plan(release, pin, principal)
    validate_inventory_output(release, output)
    expected_pin = plan_sha256(plan)
    owner = plan["target"]["account_id"]
    source_name = principal.rsplit("/", 1)[1]
    from botocore.stub import Stubber

    original_factory = operation.create_client
    results = {}
    for mode in MODES:
        clients: list[Any] = []
        closers: list[Any] = []
        snapshots: list[Any] = []
        stubs: list[Any] = []
        source = temporary(
            {
                "Version": 1,
                "AccessKeyId": "ASIA" + "A" * 16,
                "SecretAccessKey": "s" * 40,
                "SessionToken": "dummyInfrastructureToken",
                "AccountId": owner,
                "Expiration": (datetime.now(UTC) + timedelta(seconds=900)).isoformat(),
            }
        )
        content = envelope(source, owner)
        source.clear()
        with ExitStack() as harness:
            harness.enter_context(
                patch.dict(
                    os.environ,
                    {
                        "AWS_ACCESS_KEY_ID": "ambient-must-not-win",
                        "AWS_SECRET_ACCESS_KEY": "ambient-must-not-win",
                        "AWS_SESSION_TOKEN": "ambient-must-not-win",
                        "AWS_ENDPOINT_URL": "https://unapproved.invalid",
                        "AWS_CONFIG_FILE": "/dev/null",
                        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
                    },
                )
            )

            os.environ.pop("AWS_PROFILE", None)
            os.environ.pop("AWS_DEFAULT_PROFILE", None)

            def factory(
                snapshot: Any,
                service: str,
                *,
                mode: str = mode,
                clients: list[Any] = clients,
                closers: list[Any] = closers,
                snapshots: list[Any] = snapshots,
                stubs: list[Any] = stubs,
                harness: ExitStack = harness,
            ) -> Any:
                snapshots.append(snapshot)
                client = original_factory(snapshot, service)
                clients.append(client)
                close = harness.enter_context(patch.object(client, "close", wraps=client.close))
                closers.append(close)

                def ensure_closed(client: Any = client, close: Any = close) -> None:
                    if close.call_count == 0:
                        client.close()

                harness.callback(ensure_closed)
                endpoint = (
                    "https://iam.amazonaws.com"
                    if service == "iam"
                    else f"https://{service}.eu-north-1.amazonaws.com"
                )
                assert client.meta.endpoint_url == endpoint
                assert client.meta.config.signature_version == ("s3v4" if service == "s3" else "v4")
                assert (
                    client.meta.config.connect_timeout == 5
                    and client.meta.config.read_timeout == 10
                )
                assert client.meta.config.retries["total_max_attempts"] == 1
                assert client._endpoint.http_session._verify is True
                assert client._request_signer._credentials.access_key == snapshot.access_key
                stub = harness.enter_context(Stubber(client))
                stubs.append(stub)
                if service == "sts":
                    stub.add_response(
                        "get_caller_identity",
                        {
                            "Account": "000000000000" if mode == "wrong_caller" else owner,
                            "Arn": f"arn:aws:sts::{owner}:assumed-role/{source_name}/browser",
                            "UserId": "AROA" + "B" * 17 + ":browser",
                        },
                        {},
                    )
                    return client
                if mode == "wrong_caller":
                    return client

                def missing(
                    method: str, params: dict[str, Any], code: str, http_code: int, **extra: Any
                ) -> None:
                    stub.add_client_error(
                        method,
                        service_error_code=code,
                        http_status_code=http_code,
                        service_message="sentinel-private-service-message",
                        expected_params=params,
                        **extra,
                    )

                params: dict[str, Any]
                if service == "s3":
                    for index, t in enumerate(t for t in plan["targets"] if t["kind"] == "bucket"):
                        params = {"Bucket": t["name"], "ExpectedBucketOwner": owner, "MaxKeys": 1}
                        if mode == "empty_bucket" and index == 0:
                            stub.add_response(
                                "list_objects_v2",
                                {
                                    "Name": t["name"],
                                    "Contents": [],
                                    "IsTruncated": False,
                                    "KeyCount": 0,
                                },
                                params,
                            )
                        else:
                            missing("list_objects_v2", params, "NoSuchBucket", 404)
                elif service == "glue":
                    for t in (t for t in plan["targets"] if t["kind"] in {"database", "job"}):
                        params = (
                            {"CatalogId": owner, "Name": t["name"]}
                            if t["kind"] == "database"
                            else {"JobName": t["name"]}
                        )
                        extra = (
                            {"modeled_fields": {"FromFederationSource": True}}
                            if mode == "federated_database" and t["kind"] == "database"
                            else {}
                        )
                        missing("get_" + t["kind"], params, "EntityNotFoundException", 400, **extra)
                elif service == "iam":
                    for kind in ("role", "policy"):
                        for index, t in enumerate(t for t in plan["targets"] if t["kind"] == kind):
                            params = (
                                {"RoleName": t["name"]}
                                if kind == "role"
                                else {"PolicyArn": t["arn"]}
                            )
                            if mode == "remaining_role" and kind == "role" and index == 0:
                                stub.add_response(
                                    "get_role",
                                    {
                                        "Role": {
                                            "RoleName": t["name"],
                                            "Arn": t["arn"],
                                            "Path": "/service-role/",
                                            "RoleId": "AROA" + "C" * 17,
                                            "CreateDate": datetime(2026, 11, 10, tzinfo=UTC),
                                            "AssumeRolePolicyDocument": json.dumps(
                                                {
                                                    "Version": "2012-10-17",
                                                    "Statement": [],
                                                    "private": "sentinel-private-policy",
                                                }
                                            ),
                                        }
                                    },
                                    params,
                                )
                            elif mode == "denied_policy" and kind == "policy" and index == 0:
                                missing("get_policy", params, "AccessDenied", 403)
                            else:
                                missing("get_" + kind, params, "NoSuchEntity", 404)
                elif service == "athena":
                    params = {"MaxResults": 50}
                    if mode == "ambiguous_athena":
                        missing("list_work_groups", params, "InvalidRequestException", 400)
                    elif mode == "paged_athena":
                        stub.add_response(
                            "list_work_groups",
                            {"WorkGroups": [{"Name": "primary"}], "NextToken": "page-2"},
                            params,
                        )
                        stub.add_response(
                            "list_work_groups", {"WorkGroups": []}, params | {"NextToken": "page-2"}
                        )
                    else:
                        stub.add_response(
                            "list_work_groups",
                            {
                                "WorkGroups": [
                                    {
                                        "Name": "primary",
                                        "Description": "sentinel-private-description",
                                    }
                                ]
                            },
                            params,
                        )
                else:
                    params = {
                        "logGroupIdentifiers": sorted(
                            t["name"] for t in plan["targets"] if t["kind"] == "log_group"
                        ),
                        "includeLinkedAccounts": False,
                    }
                    if mode == "truncated_logs":
                        stub.add_response(
                            "describe_log_groups", {"logGroups": [], "nextToken": "page-2"}, params
                        )
                        stub.add_response(
                            "describe_log_groups",
                            {"logGroups": [], "nextToken": "page-3"},
                            params | {"nextToken": "page-2"},
                        )
                    else:
                        stub.add_response("describe_log_groups", {"logGroups": []}, params)
                return client

            harness.enter_context(patch.object(operation, "create_client", side_effect=factory))
            try:
                completion = operation.run_absence(
                    release, pin, principal, expected_pin, content, output / mode
                )
            except ValueError:
                assert mode == "wrong_caller"
                assert not (output / mode / "session-complete.json").exists()
                observed, status, calls = False, "caller_refused", 1
            else:
                assert mode != "wrong_caller"
                observed = completion["named_infrastructure_absence_observed"]
                assert observed == (mode in {"all_absent", "paged_athena"})
                report = json.loads((output / mode / "inspection/inventory.json").read_bytes())
                assert all(
                    r["error_type"] not in {"UnStubbedResponseError", "StubAssertionError"}
                    for r in report["targets"]
                )
                if mode in {"empty_bucket", "remaining_role"}:
                    assert any(r["state"] == "present" for r in report["targets"])
                status, calls = report["status"], report["api_calls"]
            for stub in stubs:
                stub.assert_no_pending_responses()
            assert len(clients) == len(SERVICES) and all(c.call_count == 1 for c in closers)
            assert all(s is snapshots[0] for s in snapshots) and not any(
                snapshots[0].session_options().values()
            )
            assert all(
                "sentinel-private-" not in p.read_text() for p in (output / mode).rglob("*.json")
            )
            results[mode] = {
                "status": status,
                "named_infrastructure_absence_observed": observed,
                "simulated_read_calls": calls,
                "sdk_clients_closed": len(clients),
                "owned_source_cleared": True,
            }
    result = {
        "sdk_pins": operation.tooling_sdk_pins(),
        "scenarios": results,
        "synthetic_service_responses": True,
        "live_identity_verified": False,
        "live_absence_verified": False,
        "aws_requests": 0,
        "aws_mutations": 0,
        "credentials_exported": False,
        "scope": "SDK Stubber only; not live teardown or billing proof",
    }
    (output / "checks.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = check(args.release, args.release_sha256, args.source_role_arn, args.output)
    print(
        json.dumps(
            {
                "mode": "offline_sdk_check",
                "scenarios": len(result["scenarios"]),
                "sdk_clients_closed": sum(
                    r["sdk_clients_closed"] for r in result["scenarios"].values()
                ),
                "aws_requests": 0,
                "aws_mutations": 0,
                "credentials_exported": False,
            }
        )
    )


if __name__ == "__main__":
    main()
