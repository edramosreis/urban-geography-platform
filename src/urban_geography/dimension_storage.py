"""Append-only local Iceberg dimension registries; preflight before any pair writes."""

import re
from dataclasses import dataclass
from typing import Any, Literal

from pyspark.sql import DataFrame, SparkSession

from urban_geography.dimensions import AREA_SCHEMA, RELEASE_SCHEMA, _unique_keys
from urban_geography.lakehouse import _table_metadata

DimensionKind = Literal["releases", "study_areas"]
SCHEMAS = {"releases": RELEASE_SCHEMA, "study_areas": AREA_SCHEMA}
KEYS = {"releases": "source_release", "study_areas": "study_area_sha256"}
MAX_DIMENSION_ROWS = 40_000


@dataclass(frozen=True)
class PreparedDimension:
    table: str
    kind: DimensionKind
    incoming: DataFrame
    new_rows: DataFrame
    before: DataFrame | None
    snapshot_id: int | None


def same_rows(left: DataFrame, right: DataFrame) -> None:
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise ValueError("Dimension readback differs from the expected rows")


def _check_frame(spark: SparkSession, frame: DataFrame, kind: DimensionKind) -> None:
    expected = spark.createDataFrame([], SCHEMAS[kind])
    if dict(frame.dtypes) != dict(expected.dtypes):
        raise ValueError("Dimension schema differs from the supported contract")
    if not 0 < frame.limit(MAX_DIMENSION_ROWS + 1).count() <= MAX_DIMENSION_ROWS:
        raise ValueError("Dimension registry is empty or exceeds the pilot limit")
    _unique_keys(frame, KEYS[kind])


def _metadata(spark: SparkSession, table: str, kind: DimensionKind) -> dict[str, Any]:
    if not re.fullmatch(r"local\.analytics\.[a-z][a-z0-9_]*", table):
        raise ValueError("Dimension table must be a safe local.analytics identifier")
    metadata = _table_metadata(spark, table)
    spec = next(
        s for s in metadata["partition-specs"] if s["spec-id"] == metadata["default-spec-id"]
    )
    if (
        metadata["format-version"] != 2
        or metadata["properties"].get("dimension-contract") != "dimension_registry_v1"
        or metadata["properties"].get("dimension-kind") != kind
        or spec["fields"]
    ):
        raise ValueError("Target is not a supported unpartitioned dimension registry")
    return metadata


def read_dimension_table(
    spark: SparkSession, table: str, kind: DimensionKind, snapshot_id: int | None = None
) -> DataFrame:
    metadata = _metadata(spark, table, kind)
    pinned = snapshot_id if snapshot_id is not None else metadata.get("current-snapshot-id")
    if pinned not in {s["snapshot-id"] for s in metadata["snapshots"]}:
        raise ValueError("Dimension snapshot is not retained")
    frame = spark.read.option("snapshot-id", str(pinned)).table(table)
    _check_frame(spark, frame, kind)
    return frame


def prepare_dimension_table(
    spark: SparkSession, incoming: DataFrame, *, table: str, kind: DimensionKind
) -> PreparedDimension:
    """Reject conflicting key attributes without modifying any table."""
    if not re.fullmatch(r"local\.analytics\.[a-z][a-z0-9_]*", table):
        raise ValueError("Dimension table must be a safe local.analytics identifier")
    _check_frame(spark, incoming, kind)
    before, base = None, None
    new_rows = incoming
    if spark.catalog.tableExists(table):
        metadata = _metadata(spark, table, kind)
        base = int(metadata["current-snapshot-id"])
        before = read_dimension_table(spark, table, kind, base).select(*incoming.columns)
        key = KEYS[kind]
        overlap = before.join(incoming.select(key), key, "left_semi")
        if overlap.exceptAll(incoming).limit(1).count():
            raise ValueError("Existing dimension key has conflicting attributes")
        new_rows = incoming.join(before.select(key), key, "left_anti").select(*incoming.columns)
        if before.count() + new_rows.count() > MAX_DIMENSION_ROWS:
            raise ValueError("Dimension registry would exceed the pilot limit")
    return PreparedDimension(table, kind, incoming, new_rows, before, base)


def write_dimension_table(
    spark: SparkSession, prepared: PreparedDimension, publication_sha256: str
) -> dict[str, Any]:
    """Append only unseen keys, or verify an exact retry without a new snapshot."""
    if not re.fullmatch(r"[0-9a-f]{64}", publication_sha256):
        raise ValueError("Invalid dimension publication hash")
    exists = spark.catalog.tableExists(prepared.table)
    if exists != (prepared.before is not None):
        raise ValueError("Dimension registry changed after preflight")
    if (
        exists
        and _metadata(spark, prepared.table, prepared.kind)["current-snapshot-id"]
        != prepared.snapshot_id
    ):
        raise ValueError("Dimension snapshot changed after preflight")
    count = prepared.new_rows.count()
    if count:
        writer = (
            prepared.new_rows.writeTo(prepared.table)
            .using("iceberg")
            .option("snapshot-property.dimension-publication-sha256", publication_sha256)
        )
        if exists:
            writer.append()
        else:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS local.analytics")
            (
                writer.tableProperty("format-version", "2")
                .tableProperty("dimension-contract", "dimension_registry_v1")
                .tableProperty("dimension-kind", prepared.kind)
                .create()
            )
    metadata = _metadata(spark, prepared.table, prepared.kind)
    pinned = int(metadata["current-snapshot-id"])
    stored = read_dimension_table(spark, prepared.table, prepared.kind, pinned)
    expected = (
        prepared.new_rows
        if prepared.before is None
        else prepared.before.unionByName(prepared.new_rows)
    )
    same_rows(stored.select(*expected.columns), expected)
    return {
        "table": prepared.table,
        "snapshot_id": pinned,
        "status": "reused" if not count else ("appended" if exists else "created"),
    }
