"""Immutable local Iceberg Gold grid runs, with atomic row and lineage publication."""

import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from urban_geography.dimension_publication import (
    BINDING_KEYS,
    _digest,
    _warehouse,
    read_dimension_publication,
)
from urban_geography.gold import load_sql_models, sql_model_hashes
from urban_geography.lakehouse import _table_metadata

GOLD_TYPES = {
    "gold_run_sha256": "string",
    "study_area_sha256": "string",
    "source_release": "string",
    "area_id": "string",
    "source_run_sha256": "string",
    "grid_id": "string",
    "mapped_buildings": "bigint",
    "buildings_within": "bigint",
    "share_within": "double",
}
MAX_GOLD_ROWS = 40_000


@dataclass(frozen=True)
class StoredGold:
    grids: DataFrame
    releases: DataFrame
    lineage: dict[str, Any]
    snapshot_id: int
    analysis_definition: dict[str, Any]


def validate_gold_request(table_name: str, snapshot_id: int | None = None) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
        raise ValueError("Gold table_name must be a safe lowercase identifier")
    if snapshot_id is not None and snapshot_id <= 0:
        raise ValueError("Gold snapshot ID must be positive")


def _source_manifest(spark: SparkSession, manifest: dict[str, Any]) -> None:
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if (
        manifest.get("status") != "complete"
        or manifest["payload"].get("contract_version") != 1
        or manifest.get("manifest_sha256") != _digest(body)
        or manifest.get("publication_sha256") != _digest(manifest["payload"])
    ):
        raise ValueError("Source dimension completion manifest is invalid")
    if manifest["payload"]["warehouse"] != _warehouse(spark):
        raise ValueError("Gold source belongs to a different warehouse")


def _metadata(spark: SparkSession, table: str) -> dict[str, Any]:
    metadata = _table_metadata(spark, table)
    spec = next(
        s for s in metadata["partition-specs"] if s["spec-id"] == metadata["default-spec-id"]
    )
    if (
        metadata["format-version"] != 2
        or metadata["properties"].get("gold-contract") != "grid_proximity_runs_v1"
        or [(f["name"], f["transform"]) for f in spec["fields"]]
        != [("gold_run_sha256", "identity")]
    ):
        raise ValueError("Target is not a supported Gold grid-run table")
    return metadata


def _check_grids(frame: DataFrame) -> None:
    if dict(frame.dtypes) != GOLD_TYPES:
        raise ValueError("Gold grid schema differs from the supported contract")
    total = frame.limit(MAX_GOLD_ROWS + 1).count()
    if not 0 < total <= MAX_GOLD_ROWS:
        raise ValueError("Gold table is empty or exceeds the pilot row limit")
    keys = ["gold_run_sha256", "source_release", "area_id", "grid_id"]
    bad = F.lit(False)
    for name in keys:
        bad = bad | F.col(name).isNull() | (F.col(name) == "")
    for name in ("gold_run_sha256", "study_area_sha256", "source_run_sha256"):
        bad = bad | F.col(name).isNull() | ~F.col(name).rlike("^[0-9a-f]{64}$")
    mapped, matched, share = (
        F.col("mapped_buildings"),
        F.col("buildings_within"),
        F.col("share_within"),
    )
    bad = (
        bad
        | mapped.isNull()
        | matched.isNull()
        | share.isNull()
        | (mapped <= 0)
        | (mapped > 40000)
        | (matched < 0)
        | (matched > mapped)
        | ~share.eqNullSafe(matched.cast("double") / mapped)
    )
    if frame.filter(bad).limit(1).count():
        raise ValueError("Gold grid keys, counts or shares are invalid")
    if frame.select(*keys).distinct().count() != total:
        raise ValueError("Duplicate Gold run/release/area/grid keys")


def _content(frame: DataFrame) -> tuple[str, int, int]:
    _check_grids(frame)
    rows = [row.asDict() for row in frame.select(*GOLD_TYPES).collect()]
    canonical = sorted(json.dumps(row, sort_keys=True, allow_nan=False) for row in rows)
    return _digest(canonical), len(rows), sum(row["mapped_buildings"] for row in rows)


def _origin(metadata: dict[str, Any], snapshot_id: int, run_hash: str) -> dict[str, str]:
    snapshots = {snapshot["snapshot-id"]: snapshot for snapshot in metadata["snapshots"]}
    cursor, visited = snapshot_id, set()
    while cursor not in visited and cursor in snapshots:
        visited.add(cursor)
        snapshot = snapshots[cursor]
        if snapshot["summary"].get("gold-run-sha256") == run_hash:
            return dict(snapshot["summary"])
        cursor = snapshot.get("parent-snapshot-id")
    raise ValueError("Gold run has no retained lineage in this snapshot ancestry")


def read_gold_run(
    spark: SparkSession,
    run_sha256: str,
    *,
    table_name: str = "gold_grid_proximity",
    snapshot_id: int | None = None,
) -> StoredGold:
    """Reopen saved summaries and derive release totals without reading the source fact tables."""
    validate_gold_request(table_name, snapshot_id)
    if not re.fullmatch(r"[0-9a-f]{64}", run_sha256):
        raise ValueError("Invalid Gold run hash")
    table = "local.analytics." + table_name
    metadata = _metadata(spark, table)
    pinned = snapshot_id if snapshot_id is not None else metadata.get("current-snapshot-id")
    if pinned is None or pinned not in {s["snapshot-id"] for s in metadata["snapshots"]}:
        raise ValueError("Gold snapshot is not retained")
    all_rows = spark.read.option("snapshot-id", str(pinned)).table(table)
    _check_grids(all_rows)
    grids = all_rows.filter(F.col("gold_run_sha256") == run_sha256)
    if not grids.limit(1).count():
        raise ValueError("Gold run is not present in the selected snapshot")
    origin = _origin(metadata, int(pinned), run_sha256)
    lineage = json.loads(origin["gold-run-lineage"])
    if lineage.get("contract_version") != 1 or _digest(lineage) != run_sha256:
        raise ValueError("Gold lineage differs from its run identity")
    _source_manifest(spark, lineage["dimension_publication"])
    analysis_definition = json.loads(origin["gold-analysis-definition"])
    analysis_hash = lineage["dimension_publication"]["payload"]["dimensions"]["analysis_sha256"]
    if _digest(analysis_definition) != analysis_hash:
        raise ValueError("Gold analysis definition differs from its source contract")
    models = load_sql_models()
    if lineage["sql_sha256"] != sql_model_hashes(models):
        raise ValueError("Gold SQL version differs; use the recorded model version")
    content_hash, _, input_count = _content(grids)
    if content_hash != origin.get("gold-content-sha256") or input_count != int(
        origin["gold-input-facts"]
    ):
        raise ValueError("Stored Gold content hash or input count differs")
    view = "saved_gold_" + uuid.uuid4().hex
    try:
        grids.createOrReplaceTempView(view)
        releases = spark.sql(models["release_proximity"].format(grids=view))
    finally:
        spark.catalog.dropTempView(view)
    return StoredGold(grids, releases, lineage, int(pinned), analysis_definition)


def _receipt(stored: StoredGold, table: str, run_hash: str, status: str) -> dict[str, Any]:
    source = stored.lineage["dimension_publication"]
    return {
        "status": status,
        "table": table,
        "gold_run_sha256": run_hash,
        "snapshot_id": stored.snapshot_id,
        "grid_rows": stored.grids.count(),
        "dimension_publication_sha256": source["publication_sha256"],
        "fact_snapshot_id": source["payload"]["fact_snapshot_id"],
    }


def publish_gold_run(
    spark: SparkSession,
    dimension_publication: Path,
    *,
    table_name: str = "gold_grid_proximity",
    expected_snapshot_id: int | None = None,
) -> dict[str, Any]:
    """Append one complete immutable calculation run, committing lineage with its grid rows."""
    validate_gold_request(table_name, expected_snapshot_id)
    manifest = json.loads((dimension_publication / "manifest.json").read_text(encoding="utf-8"))
    _source_manifest(spark, manifest)
    if dimension_publication.name != manifest["publication_sha256"]:
        raise ValueError("Source publication directory differs from its identity")
    models = load_sql_models()
    lineage = {
        "contract_version": 1,
        "dimension_publication": manifest,
        "sql_sha256": sql_model_hashes(models),
    }
    run_hash = _digest(lineage)
    table = "local.analytics." + table_name
    before, base = None, None
    if spark.catalog.tableExists(table):
        metadata = _metadata(spark, table)
        base = int(metadata["current-snapshot-id"])
        before = spark.read.option("snapshot-id", str(base)).table(table)
        _check_grids(before)
        if before.filter(F.col("gold_run_sha256") == run_hash).limit(1).count():
            stored = read_gold_run(spark, run_hash, table_name=table_name, snapshot_id=base)
            return _receipt(stored, table, run_hash, "reused")
        if expected_snapshot_id is not None and base != expected_snapshot_id:
            raise ValueError("Gold snapshot differs from expected_snapshot_id")
    elif expected_snapshot_id is not None:
        raise ValueError("Cannot expect a snapshot in a missing Gold table")
    source = read_dimension_publication(spark, dimension_publication)
    source_table = "local.analytics." + manifest["payload"]["fact_table_name"]
    analysis_definition = json.loads(
        _table_metadata(spark, source_table)["properties"]["analysis-definition"]
    )
    if _digest(analysis_definition) != manifest["payload"]["dimensions"]["analysis_sha256"]:
        raise ValueError("Source analysis definition differs from the publication")
    view = "materialize_gold_" + uuid.uuid4().hex
    try:
        source.facts.createOrReplaceTempView(view)
        raw = spark.sql(models["grid_proximity"].format(facts=view))
        bindings = source.facts.select(*BINDING_KEYS, "study_area_sha256").distinct()
        incoming = (
            raw.join(bindings, BINDING_KEYS, "inner")
            .withColumn("gold_run_sha256", F.lit(run_hash))
            .select(*GOLD_TYPES)
        )
        content_hash, row_count, input_count = _content(incoming)
        if input_count != source.facts.count():
            raise ValueError("Gold grid counts do not preserve source fact cardinality")
        if before is not None and before.count() + row_count > MAX_GOLD_ROWS:
            raise ValueError("Gold append would exceed the pilot row limit")
        if before is not None and _metadata(spark, table)["current-snapshot-id"] != base:
            raise ValueError("Gold snapshot changed after preflight")
        writer = (
            incoming.writeTo(table)
            .using("iceberg")
            .option("snapshot-property.gold-run-sha256", run_hash)
            .option("snapshot-property.gold-run-lineage", json.dumps(lineage, sort_keys=True))
            .option(
                "snapshot-property.gold-analysis-definition",
                json.dumps(analysis_definition, sort_keys=True),
            )
            .option("snapshot-property.gold-content-sha256", content_hash)
            .option("snapshot-property.gold-input-facts", str(input_count))
        )
        if before is None:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS local.analytics")
            (
                writer.partitionedBy(F.col("gold_run_sha256"))
                .tableProperty("format-version", "2")
                .tableProperty("gold-contract", "grid_proximity_runs_v1")
                .create()
            )
        else:
            writer.append()
        after = _metadata(spark, table)
        current = int(after["current-snapshot-id"])
        saved = read_gold_run(spark, run_hash, table_name=table_name, snapshot_id=current)
        if before is not None:
            others = (
                spark.read.option("snapshot-id", str(current))
                .table(table)
                .filter(F.col("gold_run_sha256") != run_hash)
            )
            if (
                before.exceptAll(others).limit(1).count()
                or others.exceptAll(before).limit(1).count()
            ):
                raise ValueError("Unrelated saved Gold runs changed")
        return _receipt(saved, table, run_hash, "created" if before is None else "published")
    finally:
        spark.catalog.dropTempView(view)
