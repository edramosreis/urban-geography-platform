"""Prepare one fixed Gold SELECT and a credential-free Athena profile; never submit SQL."""

import argparse
import hashlib
import json
import tomllib
from importlib import import_module
from importlib.metadata import version
from pathlib import Path
from typing import Any, Protocol

from prepare_dbt_project import ARTIFACTS, ROOT, validate_binding

from urban_geography.glue_runtime import runtime_plan

MIN_SCAN_BYTES = 10 * 1024 * 1024
MAX_SCAN_BYTES = 100 * 1024 * 1024


class AthenaClient(Protocol):
    def get_work_group(self, **kwargs: Any) -> dict[str, Any]: ...


def same(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(
        right, sort_keys=True, allow_nan=False
    )


def read_plan(
    *,
    deployment_name: str,
    account_id: str,
    snapshot_id: int,
    source_run_sha256: str,
    region: str = "eu-west-1",
    max_query_scan_bytes: int = MAX_SCAN_BYTES,
) -> dict[str, Any]:
    if not all(isinstance(value, str) for value in (deployment_name, account_id, region)):
        raise ValueError("Athena target fields must be strings")
    recipe = runtime_plan(deployment_name=deployment_name, account_id=account_id, region=region)
    validate_binding(recipe["database"], snapshot_id, source_run_sha256)
    if (
        type(max_query_scan_bytes) is not int
        or not MIN_SCAN_BYTES <= max_query_scan_bytes <= MAX_SCAN_BYTES
    ):
        raise ValueError("Use an integer per-query scan cutoff between 10 and 100 MiB")
    models = {
        name: (ROOT / "src/urban_geography/sql" / (name + ".sql")).read_text(encoding="utf-8")
        for name in ("grid_proximity", "release_proximity")
    }
    facts = (
        "SELECT source_release, area_id, grid_id, within_threshold, "
        f"'{source_run_sha256}' AS source_run_sha256\n"
        f'FROM "AwsDataCatalog"."{recipe["database"]}"."building_proximity"\n'
        f"FOR VERSION AS OF {snapshot_id}"
    )
    query = (
        "WITH facts AS (\n"
        + facts
        + "\n), grids AS (\n"
        + models["grid_proximity"].format(facts="facts")
        + "\n), releases AS (\n"
        + models["release_proximity"].format(grids="grids")
        + "\n)\nSELECT * FROM releases ORDER BY source_release, area_id LIMIT 2\n"
    )
    results = f"s3://{deployment_name}-results-{account_id}-{region}/queries/"
    workgroup = deployment_name + "-m4"
    result_config = {
        "OutputLocation": results,
        "ExpectedBucketOwner": account_id,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }
    return {
        "contract_version": 1,
        "status": "local_plan_only",
        "target": {"deployment_name": deployment_name, "account_id": account_id, "region": region},
        "binding": {
            "source_schema": recipe["database"],
            "snapshot_id": snapshot_id,
            "source_run_sha256": source_run_sha256,
        },
        "workgroup": {
            "Name": workgroup,
            "Configuration": {
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": max_query_scan_bytes,
                "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
                "ResultConfiguration": result_config,
            },
        },
        "request_template": {
            "QueryString": query,
            "WorkGroup": workgroup,
            "QueryExecutionContext": {"Catalog": "AwsDataCatalog", "Database": recipe["database"]},
            "ResultConfiguration": result_config,
            "ResultReuseConfiguration": {"ResultReuseByAgeConfiguration": {"Enabled": False}},
        },
        "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
        "canonical_sql_sha256": {
            name: hashlib.sha256(sql.encode()).hexdigest() for name, sql in models.items()
        },
        "profile": {
            "urban_geo_athena": {
                "target": "prepared_athena",
                "outputs": {
                    "prepared_athena": {
                        "type": "athena",
                        "database": "AwsDataCatalog",
                        "schema": recipe["database"],
                        "region_name": region,
                        "work_group": workgroup,
                        "s3_staging_dir": results,
                        "threads": 1,
                        "num_retries": 0,
                        "num_boto3_retries": 0,
                        "num_iceberg_retries": 0,
                    }
                },
            }
        },
        "terraform_inputs": {
            "deployment_name": deployment_name,
            "aws_account_id": account_id,
            "aws_region": region,
            "max_query_scan_bytes": max_query_scan_bytes,
            "enable_athena_reader_policy": True,
        },
        "created_workgroup_state": "DISABLED",
        "required_query_state": "ENABLED",
        "maximum_result_groups": 2,
        "provenance_verified": False,
        "execution_authorized": False,
        "scope": (
            "Preparation and workgroup preflight only; "
            "no identity/provenance proof or query execution"
        ),
    }


def preflight_workgroup(client: AthenaClient, plan: dict[str, Any]) -> dict[str, Any]:
    """One read-only API request; require the separately enabled, enforced configuration."""
    expected = plan["workgroup"]
    group = client.get_work_group(WorkGroup=expected["Name"])["WorkGroup"]
    if group.get("Name") != expected["Name"] or group.get("State") != "ENABLED":
        raise ValueError("Athena workgroup is wrong or disabled; this helper never enables it")
    config = group.get("Configuration", {})
    for key, value in expected["Configuration"].items():
        actual = config.get(key)
        if key == "EngineVersion":
            if actual != value | {"EffectiveEngineVersion": "Athena engine version 3"}:
                raise ValueError("Athena selected/effective engine differs")
        elif not same(actual, value):
            raise ValueError("Athena workgroup differs: " + key)
    if (
        config.get("RequesterPaysEnabled", False) is not False
        or config.get("ManagedQueryResultsConfiguration", {}).get("Enabled", False) is not False
        or config.get("IdentityCenterConfiguration", {}).get("EnableIdentityCenter", False)
        is not False
        or config.get("ExecutionRole")
        or config.get("AdditionalConfiguration")
        or config.get("CustomerContentEncryptionConfiguration")
        or config.get("EngineConfiguration")
    ):
        raise ValueError("Unexpected Athena execution/storage mode")
    return {
        "workgroup_settings_verified": True,
        "caller_identity_verified": False,
        "provenance_verified": False,
        "queries_submitted": 0,
    }


def tooling_sdk_pins() -> dict[str, str]:
    lock = tomllib.loads((ROOT / "analytics/athena/uv.lock").read_text(encoding="utf-8"))
    packages = [p for p in lock["package"] if p["name"] in {"boto3", "botocore"}]
    if len(packages) != 2 or {p["name"] for p in packages} != {"boto3", "botocore"}:
        raise ValueError("Expected one Linux tooling resolution of each AWS SDK package")
    return {p["name"]: p["version"] for p in packages}


def create_athena_client(*, region: str = "eu-west-1", session: Any = None) -> Any:
    """Use tooling pins, verified TLS, fixed endpoint policy and one request attempt."""
    if region != "eu-west-1":
        raise ValueError("Athena preparation is scoped to eu-west-1")
    for name, pin in tooling_sdk_pins().items():
        if version(name) != pin:
            raise ValueError("Use the isolated analytics/athena tooling environment")
    boto3 = import_module("boto3")
    config_class = import_module("botocore.config").Config
    config = config_class(
        region_name=region,
        signature_version="v4",
        connect_timeout=5,
        read_timeout=10,
        max_pool_connections=1,
        retries={"mode": "standard", "total_max_attempts": 1},
        ignore_configured_endpoint_urls=True,
        use_fips_endpoint=False,
        use_dualstack_endpoint=False,
    )
    selected = boto3.Session(region_name=region) if session is None else session
    return selected.client("athena", region_name=region, use_ssl=True, verify=True, config=config)


def prepare(output: Path, **kwargs: Any) -> dict[str, Any]:
    plan = read_plan(**kwargs)
    resolved = output.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
    ):
        raise ValueError("Use fresh Athena preparation output under artifacts")
    output.mkdir(parents=True)
    files = {
        "plan.json": plan,
        "profiles.yml": plan["profile"],
        "terraform.tfvars.json": plan["terraform_inputs"],
    }
    for name, value in files.items():
        (output / name).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    (output / "query.sql").write_text(plan["request_template"]["QueryString"], encoding="utf-8")
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-name", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--snapshot-id", type=int, required=True)
    parser.add_argument("--source-run-sha256", required=True)
    parser.add_argument("--max-query-scan-bytes", type=int, default=MAX_SCAN_BYTES)
    parser.add_argument("--output", type=Path, required=True)
    args = vars(parser.parse_args())
    output = args.pop("output")
    plan = prepare(output, **args)
    print(
        json.dumps(
            {
                "status": plan["status"],
                "query_sha256": plan["query_sha256"],
                "execution_authorized": False,
            }
        )
    )


if __name__ == "__main__":
    main()
