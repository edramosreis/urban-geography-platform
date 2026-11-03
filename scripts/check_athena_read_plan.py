"""Check Athena preparation with the actual locked adapter/SDK and mocked responses only."""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

from athena_read_plan import (
    ARTIFACTS,
    create_athena_client,
    preflight_workgroup,
    prepare,
    tooling_sdk_pins,
)


def one(values: list[Any]) -> Any:
    if len(values) != 1:
        raise ValueError("Expected exactly one Terraform configuration block")
    return values[0]


def compare_terraform(plan: dict[str, Any], path: Path) -> str:
    if path.stat().st_size > 64 * 1024:
        raise ValueError("Mocked Terraform contract exceeds its byte ceiling")
    content = path.read_bytes()
    saved = json.loads(content)
    group = saved["workgroup"]
    config = one(group["configuration"])
    result = one(config["result_configuration"])
    engine = one(config["engine_version"])
    actual = {
        "Name": group["name"],
        "Configuration": {
            "EnforceWorkGroupConfiguration": config["enforce_workgroup_configuration"],
            "PublishCloudWatchMetricsEnabled": config["publish_cloudwatch_metrics_enabled"],
            "BytesScannedCutoffPerQuery": config["bytes_scanned_cutoff_per_query"],
            "EngineVersion": {"SelectedEngineVersion": engine["selected_engine_version"]},
            "ResultConfiguration": {
                "OutputLocation": result["output_location"],
                "ExpectedBucketOwner": result["expected_bucket_owner"],
                "EncryptionConfiguration": {
                    "EncryptionOption": one(result["encryption_configuration"])["encryption_option"]
                },
            },
        },
    }
    if actual != plan["workgroup"] or group["state"] != "DISABLED":
        raise ValueError("Preparation differs from the disabled Terraform foundation")
    statements = {s["Sid"]: s for s in saved["policy"]["Statement"]}
    target = plan["target"]
    arn = f"arn:aws:athena:{target['region']}:{target['account_id']}:workgroup/{actual['Name']}"
    if statements["UsePilotWorkgroup"]["Resource"] != arn:
        raise ValueError("Policy and prepared workgroup differ")
    return hashlib.sha256(content).hexdigest()


def check_plan(terraform_contract: Path, output: Path) -> dict[str, Any]:
    resolved = output.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
    ):
        raise ValueError("Use fresh SDK check output under artifacts")
    plan = prepare(
        output / "prepared",
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=123,
        source_run_sha256="a" * 64,
    )
    contract_sha = compare_terraform(plan, terraform_contract)
    import boto3
    from botocore.stub import Stubber
    from botocore.validate import validate_parameters
    from dbt.adapters.athena.connections import AthenaCredentials

    # The real adapter accepts these settings without opening a connection.
    profile = dict(plan["profile"]["urban_geo_athena"]["outputs"]["prepared_athena"])
    profile.pop("type")
    credentials = AthenaCredentials.from_dict(profile)
    assert credentials.work_group == plan["workgroup"]["Name"]
    assert credentials.effective_num_retries == 0 and credentials.num_iceberg_retries == 0
    session = boto3.Session(
        aws_access_key_id="offline-testing",
        aws_secret_access_key="offline-testing",
        region_name="eu-north-1",
    )
    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_ATHENA": "https://example.invalid"}):
        client = create_athena_client(session=session)
    try:
        assert client.meta.endpoint_url == "https://athena.eu-north-1.amazonaws.com"
        assert client.meta.config.retries["total_max_attempts"] == 1
        assert client.meta.config.connect_timeout == 5 and client.meta.config.read_timeout == 10
        assert (
            client.meta.config.signature_version == "v4"
            and client.meta.config.max_pool_connections == 1
        )
        # Validate the future request's SDK shape without invoking StartQueryExecution.
        request = plan["request_template"] | {
            "ClientRequestToken": "offline-shape-check-token-" + "a" * 32
        }
        validate_parameters(
            request, client.meta.service_model.operation_model("StartQueryExecution").input_shape
        )
        results: dict[str, Any] = {}
        for mode in ("enabled_match", "disabled", "drifted_output"):
            group = copy.deepcopy(plan["workgroup"])
            group["State"] = "DISABLED" if mode == "disabled" else "ENABLED"
            group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                "Athena engine version 3"
            )
            if mode == "drifted_output":
                group["Configuration"]["ResultConfiguration"]["OutputLocation"] = (
                    "s3://unexpected/results/"
                )
            with Stubber(client) as stub:
                stub.add_response(
                    "get_work_group", {"WorkGroup": group}, {"WorkGroup": plan["workgroup"]["Name"]}
                )
                if mode == "enabled_match":
                    results[mode] = preflight_workgroup(client, plan)
                else:
                    try:
                        preflight_workgroup(client, plan)
                    except ValueError:
                        results[mode] = "rejected"
                    else:
                        raise AssertionError("Expected workgroup rejection")
                stub.assert_no_pending_responses()
    finally:
        client.close()
    report = {
        "sdk_pins": tooling_sdk_pins(),
        "profile_accepted_by_real_adapter": True,
        "future_request_shape_valid": True,
        "terraform_contract_sha256": contract_sha,
        "scenarios": results,
        "queries_submitted": 0,
        "live_identity_verified": False,
        "provenance_verified": False,
        "scope": "Synthetic preparation and real SDK Stubber only; no AWS requests",
    }
    (output / "check.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--terraform-contract", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check_plan(args.terraform_contract, args.output), indent=2))


if __name__ == "__main__":
    main()
