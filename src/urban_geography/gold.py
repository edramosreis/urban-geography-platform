"""Read-only, snapshot-pinned local acceptance runner for the Gold SQL models."""

import hashlib
import json
import re
import uuid
from importlib.resources import files
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from urban_geography.lakehouse import (
    HISTORY_TYPES,
    _history_table_contract,
    _history_types,
    _table_metadata,
)

MAX_REPORT_FACTS = 40_000


def load_sql_models() -> dict[str, str]:
    """Load the same versioned SQL for reports and materialized summaries."""
    return {
        name: files("urban_geography").joinpath("sql", name + ".sql").read_text(encoding="utf-8")
        for name in ("grid_proximity", "release_proximity")
    }


def sql_model_hashes(models: dict[str, str]) -> dict[str, str]:
    return {name: hashlib.sha256(sql.encode("utf-8")).hexdigest() for name, sql in models.items()}


def validate_report_request(table_name: str, snapshot_id: int | None) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
        raise ValueError("History table_name must be a safe lowercase identifier")
    if snapshot_id is not None and snapshot_id <= 0:
        raise ValueError("snapshot_id must be positive")


def grid_report(
    spark: SparkSession,
    *,
    table_name: str = "building_proximity_history",
    snapshot_id: int | None = None,
) -> dict[str, Any]:
    """Evaluate both SQL models against one retained snapshot, without writing any table."""
    validate_report_request(table_name, snapshot_id)
    table = "local.analytics." + table_name
    if not spark.catalog.tableExists(table):
        raise ValueError("History table does not exist")
    metadata = _table_metadata(spark, table)
    _history_table_contract(metadata)
    _history_types(metadata)
    pinned = snapshot_id if snapshot_id is not None else metadata.get("current-snapshot-id")
    if pinned is None or pinned not in {s["snapshot-id"] for s in metadata["snapshots"]}:
        raise ValueError("Requested history snapshot is not retained")
    properties = metadata["properties"]
    definition = json.loads(properties["analysis-definition"])
    canonical = json.dumps(definition, sort_keys=True, separators=(",", ":"))
    analysis_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if analysis_hash != properties.get("analysis-sha256"):
        raise ValueError("History analysis definition hash differs")
    facts = spark.read.option("snapshot-id", str(pinned)).table(table).select(*HISTORY_TYPES)
    total = facts.limit(MAX_REPORT_FACTS + 1).count()
    if not 0 < total <= MAX_REPORT_FACTS:
        raise ValueError("Gold report requires 1..40000 input facts; pilot limit exceeded")
    keys = ("source_release", "area_id", "building_id")
    invalid = F.col("within_threshold").isNull()
    for name in (*keys, "grid_id"):
        invalid = invalid | F.col(name).isNull() | (F.col(name) == "")
    invalid = (
        invalid
        | F.col("source_run_sha256").isNull()
        | ~F.col("source_run_sha256").rlike("^[0-9a-f]{64}$")
    )
    boundary = definition["boundary"] or {}
    expected = {
        name: definition[name]
        for name in ("area_id", "metric_version", "threshold_m", "selection_version")
    }
    expected.update(
        boundary_id=boundary.get("area_id"),
        boundary_release=boundary.get("source_release"),
        boundary_geometry_sha256=boundary.get("geometry_sha256"),
    )
    for name, value in expected.items():
        invalid = invalid | ~F.col(name).eqNullSafe(F.lit(value))
    if facts.filter(invalid).limit(1).count():
        raise ValueError("History facts have invalid reporting keys, flags or analysis metadata")
    if facts.select(*keys).distinct().count() != total:
        raise ValueError("Duplicate release/area/building keys; aggregation blocked")
    sources = facts.select("source_release", "area_id", "source_run_sha256").distinct()
    if sources.count() != sources.select("source_release", "area_id").distinct().count():
        raise ValueError("History partition contains multiple source runs")

    suffix = uuid.uuid4().hex
    fact_view, grid_view = "gold_facts_" + suffix, "gold_grids_" + suffix
    models = load_sql_models()
    try:
        facts.createOrReplaceTempView(fact_view)
        grids = spark.sql(models["grid_proximity"].format(facts=fact_view))
        grids.createOrReplaceTempView(grid_view)
        grid_rows = [row.asDict() for row in grids.collect()]
        release_rows = [
            row.asDict()
            for row in spark.sql(models["release_proximity"].format(grids=grid_view)).collect()
        ]
    finally:
        spark.catalog.dropTempView(grid_view)
        spark.catalog.dropTempView(fact_view)
    return {
        "contract_version": 1,
        "table": table,
        "snapshot_id": int(pinned),
        "analysis_sha256": analysis_hash,
        "analysis_definition": definition,
        "sql_sha256": sql_model_hashes(models),
        "input_facts": total,
        "grids": grid_rows,
        "releases": release_rows,
        "interpretation": (
            "Occupied cells of the selected pilot building cohort, not full-cell or city coverage; "
            "mapped building straight-line proximity, not population or walking access. "
            "No match means unknown distance beyond the threshold."
        ),
    }
