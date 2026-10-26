"""Validate processed facts and publish local Iceberg snapshots and release history."""

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from urban_geography.config import RunConfig
from urban_geography.history_schema import (
    PROCESSING_TIME,
    processing_time,
    source_has_processing_time,
    source_snapshot,
)
from urban_geography.ingest import digest
from urban_geography.transform import METRIC_VERSION

FACT_TYPES = {
    "building_id": "string",
    "park_id": "string",
    "distance_m": "double",
    "within_threshold": "boolean",
    "grid_id": "string",
    "source_release": "string",
    "area_id": "string",
    "metric_version": "string",
    "threshold_m": "double",
    "selection_version": "string",
    "boundary_id": "string",
    "boundary_release": "string",
    "boundary_geometry_sha256": "string",
}


@dataclass(frozen=True)
class CompletedRun:
    path: Path
    summary: dict[str, Any]
    summary_sha256: str


def load_completed_run(path: Path) -> CompletedRun:
    """Check completion, explicit selection lineage and every Parquet hash before Spark."""
    summary_path = path / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary.get("status") != "complete" or summary.get("metric_version") != METRIC_VERSION:
        raise ValueError("Iceberg publication requires a completed supported processing run")
    values = dict(summary["config"])
    values["bbox"] = tuple(values["bbox"])
    config = RunConfig(**values)
    selection = summary.get("selection", {})
    boundary = selection.get("boundary")
    expected_version = (
        "point_on_surface_bbox_boundary_v1" if boundary else "point_on_surface_bbox_v1"
    )
    expected_scope = "pilot_bbox_intersection_boundary" if boundary else "pilot_bbox"
    if (
        selection.get("version") != expected_version
        or selection.get("scope") != expected_scope
        or selection.get("bbox") != list(config.bbox)
    ):
        raise ValueError("Processing run requires explicit, consistent selection lineage")
    if boundary and not all(
        isinstance(boundary.get(key), str) and boundary[key]
        for key in (
            "area_id",
            "source_release",
            "geometry_sha256",
            "manifest_sha256",
            "attribution",
        )
    ):
        raise ValueError("Processing boundary lineage is incomplete")
    files = summary.get("files", {})
    if not isinstance(files, dict) or not files:
        raise ValueError("Processing run has no Parquet file inventory")
    root = path.resolve()
    for relative in files:
        if not isinstance(relative, str) or not (root / relative).resolve().is_relative_to(root):
            raise ValueError("Processing file path escapes the run directory")
    actual = {p.relative_to(path).as_posix() for p in path.rglob("*.parquet")}
    if set(files) != actual or not any(p.startswith("building_proximity/") for p in actual):
        raise ValueError("Processing Parquet file inventory differs")
    for relative, expected in files.items():
        if digest(path / relative) != expected:
            raise ValueError(f"Processing file checksum differs: {relative}")
    return CompletedRun(path, summary, digest(summary_path))


def _validate_facts(facts: DataFrame, summary: dict[str, Any]) -> int:
    if {field.name: field.dataType.simpleString() for field in facts.schema} != FACT_TYPES:
        raise ValueError("Processing fact schema differs from the supported contract")
    config = summary["config"]
    total = facts.count()
    if not 0 < total <= config["max_features"] or total != summary["mapped_buildings"]:
        raise ValueError("Processing fact count differs or exceeds the pilot limit")
    if facts.select("building_id").distinct().count() != total:
        raise ValueError("Duplicate processing building IDs")
    matched = F.col("within_threshold")
    distance = F.col("distance_m")
    park = F.col("park_id")
    bad = (
        F.col("building_id").isNull()
        | (F.col("building_id") == "")
        | F.col("grid_id").isNull()
        | (F.col("grid_id") == "")
        | matched.isNull()
        | (
            matched
            & (
                park.isNull()
                | (park == "")
                | distance.isNull()
                | F.isnan(distance)
                | (distance < 0)
                | (distance > config["threshold_m"])
            )
        )
        | (~matched & (park.isNotNull() | distance.isNotNull()))
    )
    selection = summary["selection"]
    boundary = selection["boundary"] or {}
    expected = {
        "source_release": config["release"],
        "area_id": config["area_id"],
        "metric_version": summary["metric_version"],
        "threshold_m": config["threshold_m"],
        "selection_version": selection["version"],
        "boundary_id": boundary.get("area_id"),
        "boundary_release": boundary.get("source_release"),
        "boundary_geometry_sha256": boundary.get("geometry_sha256"),
    }
    for name, value in expected.items():
        bad = bad | ~F.col(name).eqNullSafe(F.lit(value))
    if facts.filter(bad).limit(1).count():
        raise ValueError("Processing facts disagree with metric or source metadata")
    count_key = "buildings_within_500m" if config["threshold_m"] == 500 else "buildings_within"
    if facts.filter(matched).count() != summary[count_key]:
        raise ValueError("Processing matched count disagrees with the summary")
    return total


def _table_metadata(spark: SparkSession, table: str) -> dict[str, Any]:
    # SHOW TBLPROPERTIES is a redacted display, not a lossless metadata API.
    entry = (
        spark.table(table + ".metadata_log_entries")
        .orderBy(F.col("timestamp").desc())
        .select("file")
        .first()
    )
    if entry is None:
        raise ValueError("Iceberg table has no committed metadata")
    location = urlparse(entry.file)
    if location.scheme != "file" or location.netloc:
        raise ValueError("This publisher supports a local filesystem catalog only")
    return json.loads(Path(unquote(location.path)).read_text(encoding="utf-8"))


def publish_facts(
    spark: SparkSession, run: CompletedRun, *, table_name: str = "building_proximity"
) -> dict[str, Any]:
    """Create one v2 fact table or verify an exact replay; never append or replace here."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
        raise ValueError("Iceberg table_name must be a safe lowercase identifier")
    table = "local.analytics." + table_name
    facts = spark.read.parquet(str(run.path / "building_proximity"))
    total = _validate_facts(facts, run.summary)
    facts = facts.select(*FACT_TYPES)
    exists = spark.catalog.tableExists(table)
    if not exists:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.analytics")
        (
            facts.writeTo(table)
            .using("iceberg")
            .partitionedBy(F.col("source_release"), F.col("area_id"))
            .tableProperty("format-version", "2")
            .tableProperty("source-run-sha256", run.summary_sha256)
            .tableProperty("source-run-summary", json.dumps(run.summary, sort_keys=True))
            .option("snapshot-property.source-run-sha256", run.summary_sha256)
            .create()
        )
    metadata = _table_metadata(spark, table)
    properties = metadata["properties"]
    if metadata["format-version"] != 2 or properties.get("source-run-sha256") != run.summary_sha256:
        raise ValueError(
            "Iceberg table already contains a different run; replacement is not enabled"
        )
    if properties.get("source-run-summary") != json.dumps(run.summary, sort_keys=True):
        raise ValueError("Iceberg source summary provenance differs")
    stored = spark.table(table).select(*FACT_TYPES)
    if facts.exceptAll(stored).limit(1).count() or stored.exceptAll(facts).limit(1).count():
        raise ValueError("Iceberg readback differs from the verified processing facts")
    current = spark.table(table + ".refs").filter("name = 'main'").select("snapshot_id").first()
    if current is None:
        raise ValueError("Iceberg table has no current snapshot")
    return {
        "status": "reused" if exists else "created",
        "table": table,
        "format_version": 2,
        "snapshot_id": current.snapshot_id,
        "rows": total,
        "source_run_sha256": run.summary_sha256,
        "readback_matches": True,
    }


def analysis_definition(run: CompletedRun) -> dict[str, Any]:
    """Identify comparable analyses independently of source-release and retrieval time."""
    config = run.summary["config"]
    selection = run.summary["selection"]
    boundary = selection["boundary"]
    return {
        "contract_version": 1,
        "area_id": config["area_id"],
        "bbox": [float(value) for value in config["bbox"]],
        "metric_crs": config["metric_crs"],
        "threshold_m": float(config["threshold_m"]),
        "metric_version": run.summary["metric_version"],
        "selection_version": selection["version"],
        "boundary": (
            {key: boundary[key] for key in ("area_id", "source_release", "geometry_sha256")}
            if boundary
            else None
        ),
    }


def analysis_sha256(run: CompletedRun) -> str:
    canonical = json.dumps(analysis_definition(run), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


HISTORY_CONTRACT = "building_proximity_releases_v1"
HISTORY_TYPES = FACT_TYPES | {"source_run_sha256": "string"}


def _history_table_contract(metadata: dict[str, Any]) -> None:
    if (
        metadata["format-version"] != 2
        or metadata["properties"].get("release-history-contract") != HISTORY_CONTRACT
    ):
        raise ValueError("Target is not a supported release-history table")
    spec = next(
        (
            item
            for item in metadata["partition-specs"]
            if item["spec-id"] == metadata["default-spec-id"]
        ),
        None,
    )
    if spec is None or [(field["name"], field["transform"]) for field in spec["fields"]] != [
        ("source_release", "identity"),
        ("area_id", "identity"),
    ]:
        raise ValueError("History partition layout differs from the supported contract")


def _schema_fields(metadata: dict[str, Any]) -> dict[str, Any]:
    for schema in metadata["schemas"]:
        if schema["schema-id"] == metadata["current-schema-id"]:
            fields = {field["name"]: field for field in schema["fields"]}
            if len(fields) != len(schema["fields"]):
                raise ValueError("Duplicate history schema field names")
            return fields
    raise ValueError("Current history schema is unavailable")


def _history_types(metadata: dict[str, Any]) -> dict[str, str]:
    fields = _schema_fields(metadata)
    expected = HISTORY_TYPES
    if PROCESSING_TIME in fields:
        if fields[PROCESSING_TIME]["type"] != "timestamptz" or fields[PROCESSING_TIME]["required"]:
            raise ValueError("processed_at_utc must be a nullable UTC timestamp")
        expected = HISTORY_TYPES | {PROCESSING_TIME: "timestamp"}
    actual = {
        name: ("timestamp" if name == PROCESSING_TIME else field["type"])
        for name, field in fields.items()
    }
    if actual != expected:
        raise ValueError("History fact schema differs from the supported contract")
    return expected


def _history_frame(frame: DataFrame, types: dict[str, str]) -> DataFrame:
    actual = {field.name: field.dataType.simpleString() for field in frame.schema}
    if PROCESSING_TIME in types and actual == HISTORY_TYPES:
        frame = frame.withColumn(PROCESSING_TIME, F.lit(None).cast(TimestampType()))
        actual = actual | {PROCESSING_TIME: "timestamp"}
    if actual != types:
        raise ValueError("History fact schema differs from the supported contract")
    return frame.select(*types)


def _same_history_rows(left: DataFrame, right: DataFrame, message: str) -> None:
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise ValueError(message)


def publish_release(
    spark: SparkSession,
    run: CompletedRun,
    *,
    table_name: str = "building_proximity_history",
    replace: bool = False,
    expected_snapshot_id: int | None = None,
) -> dict[str, Any]:
    """Publish or explicitly replace one release/area in a comparable-analysis table."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
        raise ValueError("Iceberg table_name must be a safe lowercase identifier")
    if replace and expected_snapshot_id is None:
        raise ValueError("Replacement requires an expected_snapshot_id")
    if expected_snapshot_id is not None and expected_snapshot_id <= 0:
        raise ValueError("expected_snapshot_id must be positive")
    table = "local.analytics." + table_name
    facts = spark.read.parquet(str(run.path / "building_proximity"))
    total = _validate_facts(facts, run.summary)
    incoming = facts.select(*FACT_TYPES).withColumn("source_run_sha256", F.lit(run.summary_sha256))
    definition = json.dumps(analysis_definition(run), sort_keys=True, separators=(",", ":"))
    definition_hash = analysis_sha256(run)
    summary_json = json.dumps(run.summary, sort_keys=True)
    config = run.summary["config"]
    scope = (F.col("source_release") == config["release"]) & (F.col("area_id") == config["area_id"])
    exists = spark.catalog.tableExists(table)
    base: int | None = None
    before: DataFrame | None = None
    status = "created"
    types = HISTORY_TYPES
    if exists:
        metadata = _table_metadata(spark, table)
        properties = metadata["properties"]
        _history_table_contract(metadata)
        if (
            properties.get("analysis-sha256") != definition_hash
            or properties.get("analysis-definition") != definition
        ):
            raise ValueError("Analysis definition differs; use a separate history table")
        types = _history_types(metadata)
        base = int(metadata["current-snapshot-id"])
        before = _history_frame(spark.read.option("snapshot-id", str(base)).table(table), types)
        target = before.filter(scope)
        target_count = target.count()
        if target_count:
            if (
                not target.filter(~F.col("source_run_sha256").eqNullSafe(F.lit(run.summary_sha256)))
                .limit(1)
                .count()
            ):
                origin = source_snapshot(metadata, base, run.summary_sha256, summary_json)
                if PROCESSING_TIME in types:
                    value = (
                        processing_time(run.summary)
                        if source_has_processing_time(metadata, origin)
                        else None
                    )
                    incoming = incoming.withColumn(
                        PROCESSING_TIME, F.lit(value).cast(TimestampType())
                    )
                _same_history_rows(target, incoming, "Stored release differs from its source run")
                status = "reused"
            elif not replace:
                raise ValueError(
                    "Release already contains a different run; replacement must be explicit"
                )
            else:
                status = "replaced"
        else:
            if replace:
                raise ValueError("Cannot replace a missing release partition")
            status = "published"
        # A retry after a successful commit is already complete, even with its old expected ID.
        if status != "reused" and expected_snapshot_id is not None and base != expected_snapshot_id:
            raise ValueError(
                "Current snapshot differs from expected_snapshot_id; inspect before retrying"
            )
    elif replace or expected_snapshot_id is not None:
        raise ValueError("Cannot replace or expect a snapshot in a missing history table")

    if status != "reused":
        if PROCESSING_TIME in types:
            incoming = incoming.withColumn(
                PROCESSING_TIME, F.lit(processing_time(run.summary)).cast(TimestampType())
            )
        writer = (
            incoming.writeTo(table)
            .using("iceberg")
            .option("snapshot-property.source-run-sha256", run.summary_sha256)
            .option("snapshot-property.source-run-summary", summary_json)
            .option("snapshot-property.source-release", config["release"])
            .option("snapshot-property.area-id", config["area_id"])
            .option("snapshot-property.analysis-sha256", definition_hash)
        )
        if exists:
            # Explicit filter and conflict validation prevent a whole-table overwrite.
            writer.option("isolation-level", "serializable").option(
                "validate-from-snapshot-id", str(base)
            ).overwrite(scope)
        else:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS local.analytics")
            (
                writer.partitionedBy(F.col("source_release"), F.col("area_id"))
                .tableProperty("format-version", "2")
                .tableProperty("release-history-contract", HISTORY_CONTRACT)
                .tableProperty("analysis-sha256", definition_hash)
                .tableProperty("analysis-definition", definition)
                .create()
            )
    published_metadata = _table_metadata(spark, table)
    current_id = published_metadata.get("current-snapshot-id")
    if current_id is None:
        raise ValueError("History table has no current snapshot")
    current_id = int(current_id)
    after = _history_frame(spark.read.option("snapshot-id", str(current_id)).table(table), types)
    _same_history_rows(after.filter(scope), incoming, "Published release readback differs")
    if before is not None:
        _same_history_rows(
            before.filter(~scope), after.filter(~scope), "Unrelated release partitions changed"
        )
    return {
        "status": status,
        "table": table,
        "format_version": 2,
        "snapshot_id": current_id,
        "schema_id": published_metadata["current-schema-id"],
        "previous_snapshot_id": base,
        "source_release": config["release"],
        "area_id": config["area_id"],
        "rows_in_release": total,
        "source_run_sha256": run.summary_sha256,
        "analysis_sha256": definition_hash,
        "readback_matches": True,
    }


def evolve_history_schema(
    spark: SparkSession,
    *,
    expected_schema_id: int,
    table_name: str = "building_proximity_history",
) -> dict[str, Any]:
    """Explicitly add nullable processing time without rewriting data or creating a snapshot."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
        raise ValueError("Iceberg table_name must be a safe lowercase identifier")
    if expected_schema_id < 0:
        raise ValueError("expected_schema_id must be nonnegative")
    table = "local.analytics." + table_name
    if not spark.catalog.tableExists(table):
        raise ValueError("History table does not exist")
    before = _table_metadata(spark, table)
    _history_table_contract(before)
    types = _history_types(before)
    old_schema = int(before["current-schema-id"])
    if PROCESSING_TIME in types:
        status = "reused"
    else:
        if old_schema != expected_schema_id:
            raise ValueError("Current schema differs from expected_schema_id")
        spark.sql(
            f"ALTER TABLE {table} ADD COLUMN {PROCESSING_TIME} TIMESTAMP_LTZ "
            "COMMENT 'Processing completion in UTC, truncated to milliseconds'"
        )
        status = "evolved"
    after = _table_metadata(spark, table)
    if PROCESSING_TIME not in _history_types(after):
        raise ValueError("Processing-time column was not added")
    old_fields, new_fields = _schema_fields(before), _schema_fields(after)
    if any(new_fields.get(name) != field for name, field in old_fields.items()):
        raise ValueError("Existing history field identities changed")
    if after["current-snapshot-id"] != before["current-snapshot-id"]:
        raise ValueError("Data snapshot changed during schema evolution")
    return {
        "status": status,
        "table": table,
        "previous_schema_id": old_schema,
        "schema_id": after["current-schema-id"],
        "snapshot_id": after["current-snapshot-id"],
        "column": PROCESSING_TIME,
        "nullable": True,
        "snapshot_unchanged": True,
        "existing_field_ids_preserved": True,
    }
