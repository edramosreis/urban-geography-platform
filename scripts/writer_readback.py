"""Read-only verification of a writer receipt against one immutable Iceberg snapshot.

This administration helper stays outside the frozen worker wheel. Callers own
approval/configuration of a session factory; the helper never submits a job.
"""

import hashlib
import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

from prepare_dbt_project import validate_binding
from pyspark.sql import SparkSession

from urban_geography.glue_input import (
    MAX_PARQUET_FILES,
    MAX_PAYLOAD_BYTES,
    InputPlan,
    load_prepared_input,
)
from urban_geography.glue_writer import _driver_facts
from urban_geography.ingest import digest
from urban_geography.lakehouse import FACT_TYPES, _table_metadata, _validate_facts

MAX_WRITER_RECEIPT_BYTES = 64 * 1024


def same(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(
        right, sort_keys=True, allow_nan=False
    )


def load_claim(
    path: Path, pin: str, prepared: InputPlan, table: str, settings: dict[str, str]
) -> dict[str, Any]:
    """Check a caller-pinned publication object from run_writer, before session startup."""
    if not isinstance(pin, str) or not re.fullmatch(r"[0-9a-f]{64}", pin):
        raise ValueError("An explicit writer receipt SHA256 is required")
    if path.is_symlink() or not path.is_file():
        raise ValueError("Writer receipt must be a regular file")
    with path.open("rb") as source:
        content = source.read(MAX_WRITER_RECEIPT_BYTES + 1)
    if len(content) > MAX_WRITER_RECEIPT_BYTES or hashlib.sha256(content).hexdigest() != pin:
        raise ValueError("Writer receipt size or SHA256 differs")
    claim = json.loads(content)
    return validate_claim(claim, prepared, table, settings)


def validate_claim(
    claim: Any, prepared: InputPlan, table: str, settings: dict[str, str]
) -> dict[str, Any]:
    """Validate the existing publication schema without writing it or opening Spark."""
    expected = {
        "table": table,
        "format_version": 2,
        "rows": prepared.run.summary["mapped_buildings"],
        "source_run_sha256": prepared.run.summary_sha256,
        "readback_matches": True,
        "input_files": len(prepared.files),
        "input_payload_bytes": prepared.payload_bytes,
        "input_manifest_sha256": digest(prepared.run.path / "input-manifest.json"),
        "verified_spark_settings": settings,
    }
    if (
        not isinstance(claim, dict)
        or set(claim) != set(expected) | {"status", "snapshot_id"}
        or claim.get("status") not in ("created", "reused")
        or not same({key: claim.get(key) for key in expected}, expected)
    ):
        raise ValueError("Writer receipt differs from the verified input or expected target")
    validate_binding(table.split(".")[1], claim["snapshot_id"], prepared.run.summary_sha256)
    return claim


def check_metadata(
    metadata: dict[str, Any], prepared: InputPlan, claim: dict[str, Any], location: str
) -> dict[str, Any]:
    """Reject changed identity/provenance and oversized snapshots before reading fact data."""
    summary = prepared.run.summary
    properties = metadata["properties"]
    snapshots = metadata.get("snapshots", [])
    if (
        metadata.get("format-version") != 2
        or metadata.get("current-snapshot-id") != claim["snapshot_id"]
        or metadata.get("location", "").rstrip("/") != location.rstrip("/")
        or properties.get("source-run-sha256") != prepared.run.summary_sha256
        or properties.get("source-run-summary") != json.dumps(summary, sort_keys=True)
        or len(snapshots) != 1
        or snapshots[0].get("snapshot-id") != claim["snapshot_id"]
    ):
        raise ValueError("Iceberg metadata differs from the immutable writer/input contract")
    UUID(metadata["table-uuid"])
    schema = [s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"]]
    if len(schema) != 1 or {f["name"]: f["type"] for f in schema[0]["fields"]} != FACT_TYPES:
        raise ValueError("Iceberg metadata fact schema differs")
    stats = snapshots[0]["summary"]
    if (
        stats.get("operation") != "append"
        or stats.get("source-run-sha256") != prepared.run.summary_sha256
    ):
        raise ValueError("Iceberg snapshot provenance differs")
    bounds = {
        "total-records": (summary["mapped_buildings"], summary["mapped_buildings"]),
        "total-data-files": (1, MAX_PARQUET_FILES),
        "total-files-size": (1, min(summary["config"]["max_output_bytes"], MAX_PAYLOAD_BYTES)),
        "total-delete-files": (0, 0),
    }
    observed = {}
    for key, (minimum, maximum) in bounds.items():
        value = stats.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value):
            raise ValueError("Missing/invalid Iceberg snapshot bound: " + key)
        observed[key] = int(value)
        if not minimum <= observed[key] <= maximum:
            raise ValueError("Iceberg snapshot exceeds the readback contract: " + key)
    return observed


def verify_writer_output(
    input_path: Path,
    expected_summary_sha256: str,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    *,
    catalog_name: str,
    namespace: str,
    expected_settings: dict[str, str],
    expected_location: str,
    session_factory: Callable[[], SparkSession],
    table_name: str = "building_proximity",
) -> dict[str, Any]:
    """Own one reader session; verify pinned rows, then refresh metadata to detect drift."""
    for name in (catalog_name, namespace, table_name):
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Readback target requires safe lowercase identifiers")
    table = f"{catalog_name}.{namespace}.{table_name}"
    prefix = "spark.sql.catalog." + catalog_name
    settings = dict(expected_settings)
    required = {
        "spark.sql.session.timeZone": "UTC",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        prefix + ".table-override.format-version": "2",
    }
    warehouse = settings.get(prefix + ".warehouse", "")
    if (
        any(settings.get(key) != value for key, value in required.items())
        or not warehouse
        or not expected_location.startswith(warehouse.rstrip("/") + "/")
    ):
        raise ValueError(
            "Readback requires explicit UTC, v2 catalog and a location within its warehouse"
        )
    prepared = load_prepared_input(input_path, expected_summary_sha256)
    claim = load_claim(writer_receipt, writer_receipt_sha256, prepared, table, settings)
    spark = session_factory()
    try:
        if spark.version != "3.5.6" or any(
            spark.conf.get(key, "") != value for key, value in settings.items()
        ):
            raise ValueError("Readback Spark settings differ from the explicit target")
        before = _table_metadata(spark, table)
        stats = check_metadata(before, prepared, claim, expected_location)
        stored = spark.read.option("snapshot-id", str(claim["snapshot_id"])).table(table)
        rows = _validate_facts(stored, prepared.run.summary)
        source = _driver_facts(spark, prepared).select(*FACT_TYPES)
        if (
            source.exceptAll(stored.select(*FACT_TYPES)).limit(1).count()
            or stored.select(*FACT_TYPES).exceptAll(source).limit(1).count()
        ):
            raise ValueError("Iceberg snapshot rows differ from the verified input")
        after = _table_metadata(spark, table)
        if not same(before, after):
            raise ValueError("Iceberg metadata changed during readback; inspect before retrying")
        current = load_prepared_input(input_path, expected_summary_sha256)
        if current.files != prepared.files or current.payload_bytes != prepared.payload_bytes:
            raise ValueError("Readback input changed during verification")
        if (
            digest(writer_receipt) != writer_receipt_sha256
            or digest(input_path / "input-manifest.json") != claim["input_manifest_sha256"]
        ):
            raise ValueError("Writer receipt or input manifest changed during verification")
        return {
            "contract_version": 1,
            "status": "verified_snapshot_readback",
            "table": table,
            "table_uuid": before["table-uuid"],
            "location": before["location"],
            "schema_id": before["current-schema-id"],
            "binding": {
                "source_schema": namespace,
                "snapshot_id": claim["snapshot_id"],
                "source_run_sha256": expected_summary_sha256,
            },
            "writer_receipt_sha256": writer_receipt_sha256,
            "input_manifest_sha256": claim["input_manifest_sha256"],
            "rows": rows,
            "snapshot_statistics": stats,
            "readback_matches": True,
            "metadata_stable": True,
            "verified_spark_settings": settings,
            "verification_engine": "spark_iceberg",
            "athena_verified": False,
            "writer_job_identity_verified": False,
            "scope": (
                "Configured catalog snapshot and exact input rows; "
                "no Athena or job-identity verification"
            ),
        }
    finally:
        spark.stop()
