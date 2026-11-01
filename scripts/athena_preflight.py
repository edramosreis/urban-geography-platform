"""Read-only caller/catalog checks joined to the existing exact Iceberg row readback."""

import re
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from athena_control import verified_plan
from athena_read_plan import same

from urban_geography.glue_runtime import runtime_plan


class IdentityClient(Protocol):
    def get_caller_identity(self, **kwargs: Any) -> dict[str, Any]: ...


class CatalogClient(Protocol):
    def get_table(self, **kwargs: Any) -> dict[str, Any]: ...


def expected_caller(account: str, arn: str, user_id: str) -> dict[str, str]:
    """Require one explicit IAM user or assumed-role session; never wildcard/root matching."""
    if not isinstance(arn, str) or not isinstance(user_id, str):
        raise ValueError("An exact caller ARN and UserId are required")
    user = re.fullmatch(r"arn:aws:iam::" + account + r":user/[A-Za-z0-9_+=,.@/-]+", arn)
    role = re.fullmatch(
        r"arn:aws:sts::"
        + account
        + r":assumed-role/[A-Za-z0-9_+=,.@-]+/([A-Za-z0-9_+=,.@-]{2,64})",
        arn,
    )
    valid_user = user is not None and re.fullmatch(r"AIDA[A-Z0-9]{12,124}", user_id) is not None
    valid_role = (
        role is not None
        and re.fullmatch(r"AROA[A-Z0-9]{12,124}:" + re.escape(role[1]), user_id) is not None
    )
    if not (valid_user or valid_role):
        raise ValueError("Use an exact same-account IAM user or assumed-role session and UserId")
    return {"Account": account, "Arn": arn, "UserId": user_id}


def caller_identity(client: IdentityClient, expected: dict[str, str]) -> dict[str, str]:
    response = client.get_caller_identity()
    actual = {key: response.get(key) for key in expected}
    if not same(actual, expected):
        raise ValueError("AWS caller differs from the explicitly expected identity")
    return dict(expected)


def catalog_state(client: CatalogClient, plan: dict[str, Any]) -> dict[str, Any]:
    account = plan["target"]["account_id"]
    database = plan["binding"]["source_schema"]
    table = client.get_table(CatalogId=account, DatabaseName=database, Name="building_proximity")[
        "Table"
    ]
    recipe = runtime_plan(**plan["target"])
    location = recipe["warehouse"] + "building_proximity"
    parameters = table.get("Parameters", {})
    storage = table.get("StorageDescriptor", {})
    metadata = parameters.get("metadata_location")
    metadata_prefix = location + "/metadata/"
    if (
        table.get("CatalogId") != account
        or table.get("DatabaseName") != database
        or table.get("Name") != "building_proximity"
        or table.get("TableType") != "EXTERNAL_TABLE"
        or parameters.get("table_type", "").upper() != "ICEBERG"
        or storage.get("Location", "").rstrip("/") != location
        or not isinstance(metadata, str)
        or not metadata.startswith(metadata_prefix)
        or re.fullmatch(
            r"[A-Za-z0-9_-][A-Za-z0-9._-]*\.metadata\.json", metadata[len(metadata_prefix) :]
        )
        is None
        or not isinstance(table.get("VersionId"), str)
        or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", table["VersionId"])
    ):
        raise ValueError(
            "Glue table identity, location, Iceberg metadata pointer or version differs"
        )
    if (
        table.get("TargetTable")
        or table.get("FederatedTable")
        or table.get("ViewDefinition")
        or table.get("ViewOriginalText")
        or table.get("ViewExpandedText")
        or table.get("IsRegisteredWithLakeFormation", False) is not False
        or storage.get("AdditionalLocations")
        or table.get("PartitionKeys")
    ):
        raise ValueError("Unsupported Glue redirect, view, partition or access mode")
    # Ignore timestamps/owners; retain all source parameters and storage settings for drift checks.
    return {
        "CatalogId": account,
        "DatabaseName": database,
        "Name": "building_proximity",
        "VersionId": table["VersionId"],
        "Parameters": parameters,
        "StorageDescriptor": storage,
    }


def read_snapshot(*args: Any, **kwargs: Any) -> dict[str, Any]:
    # The isolated SDK environment need not import/install Spark merely to inspect this module.
    reader = import_module("writer_readback")
    result: dict[str, Any] = reader.verify_writer_output(*args, **kwargs)
    return result


def verify_query_source(
    sts: IdentityClient,
    glue: CatalogClient,
    plan: dict[str, Any],
    *,
    caller_arn: str,
    caller_user_id: str,
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    session_factory: Callable[[], Any],
) -> dict[str, Any]:
    """Observe the caller/catalog around exact row readback; never submit or authorize SQL."""
    plan = verified_plan(plan)
    expected = expected_caller(plan["target"]["account_id"], caller_arn, caller_user_id)
    if not isinstance(writer_receipt_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", writer_receipt_sha256
    ):
        raise ValueError("An independently pinned writer publication receipt is required")
    caller = caller_identity(sts, expected)
    before = catalog_state(glue, plan)
    recipe = runtime_plan(**plan["target"])
    location = recipe["warehouse"] + "building_proximity"
    source = read_snapshot(
        input_path,
        plan["binding"]["source_run_sha256"],
        writer_receipt,
        writer_receipt_sha256,
        catalog_name=recipe["catalog"],
        namespace=recipe["database"],
        expected_settings=recipe["spark_settings"],
        expected_location=location,
        session_factory=session_factory,
    )
    required = {
        "status": "verified_snapshot_readback",
        "binding": plan["binding"],
        "table": recipe["catalog"] + "." + recipe["database"] + ".building_proximity",
        "location": location,
        "writer_receipt_sha256": writer_receipt_sha256,
        "verified_spark_settings": recipe["spark_settings"],
        "readback_matches": True,
        "metadata_stable": True,
        "verification_engine": "spark_iceberg",
    }
    if not same({key: source.get(key) for key in required}, required):
        raise ValueError("Iceberg readback differs from the prepared query binding or AWS target")
    after = catalog_state(glue, plan)
    if not same(before, after):
        raise ValueError("Glue table changed during source verification")
    caller_identity(sts, expected)
    return {
        "contract_version": 1,
        "status": "query_source_checked",
        "caller": caller,
        "catalog": before,
        "binding": plan["binding"],
        "query_sha256": plan["query_sha256"],
        "source_readback": source,
        "caller_identity_verified": True,
        "source_readback_verified": True,
        "catalog_stable": True,
        "writer_job_identity_verified": False,
        "result_verified": False,
        "execution_authorized": False,
        "queries_submitted": 0,
        "scope": "Caller/input observations; permissions and query results remain unverified",
    }
