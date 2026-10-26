"""Offline Gold SQL acceptance on actual temporary Iceberg snapshots."""

import json
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F

import urban_geography.gold as gold
from urban_geography.ingest import digest
from urban_geography.lakehouse import (
    evolve_history_schema,
    load_completed_run,
    publish_facts,
    publish_release,
)


def uneven_run(run):
    """Six buildings in cells of sizes 3, 1 and 2; two matches overall."""
    path = run.path / "building_proximity" / "part-00000.parquet"
    original = pq.read_table(path)
    template = original.to_pylist()[0]
    rows = [
        {
            **template,
            "building_id": "b" + str(index),
            "grid_id": grid,
            "within_threshold": matched,
            "park_id": "park" if matched else None,
            "distance_m": 50.0 if matched else None,
        }
        for index, (grid, matched) in enumerate(
            [("g0", True), ("g0", False), ("g0", False), ("g1", True), ("g2", False), ("g2", False)]
        )
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=original.schema), path)
    summary_path = run.path / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update(mapped_buildings=6, buildings_within_500m=2, share_within=2 / 6)
    summary["files"][path.relative_to(run.path).as_posix()] = digest(path)
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    return load_completed_run(run.path)


def warehouse_hashes(spark):
    from pathlib import Path
    from urllib.parse import unquote, urlparse

    root = Path(unquote(urlparse(spark.conf.get("spark.sql.catalog.local.warehouse")).path))
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


@pytest.mark.integration
def test_sql_weighting_releases_replacement_and_snapshot_pinning(
    history_spark, history_run_factory
):
    spark = history_spark
    first = uneven_run(history_run_factory())
    a = publish_release(spark, first, table_name="gold_history")
    first_report = gold.grid_report(spark, table_name="gold_history")
    assert first_report["snapshot_id"] == a["snapshot_id"]
    assert first_report["analysis_definition"]["boundary"] is None
    assert [
        (row["mapped_buildings"], row["buildings_within"]) for row in first_report["grids"]
    ] == [(3, 1), (1, 1), (2, 0)]
    assert first_report["grids"][2]["share_within"] == 0.0
    assert first_report["releases"][0]["share_within"] == 2 / 6
    assert first_report["releases"][0]["source_run_sha256"] == first.summary_sha256
    assert set(first_report["sql_sha256"]) == {"grid_proximity", "release_proximity"}
    second = history_run_factory("2026-10-21.0")
    b = publish_release(spark, second, table_name="gold_history")
    evolve_history_schema(spark, table_name="gold_history", expected_schema_id=0)
    before = warehouse_hashes(spark)
    combined = gold.grid_report(spark, table_name="gold_history")
    assert combined["input_facts"] == 8 and len(combined["grids"]) == 4
    assert [row["mapped_buildings"] for row in combined["releases"]] == [6, 2]
    assert [row["share_within"] for row in combined["releases"]] == [2 / 6, 0.5]
    assert (
        gold.grid_report(spark, table_name="gold_history", snapshot_id=a["snapshot_id"])
        == first_report
    )
    assert warehouse_hashes(spark) == before
    assert not any(
        t.name.startswith("gold_facts_") or t.name.startswith("gold_grids_")
        for t in spark.catalog.listTables()
    )
    corrected = history_run_factory(revision=1)
    publish_release(
        spark,
        corrected,
        table_name="gold_history",
        replace=True,
        expected_snapshot_id=b["snapshot_id"],
    )
    before = warehouse_hashes(spark)
    assert (
        gold.grid_report(spark, table_name="gold_history", snapshot_id=b["snapshot_id"]) == combined
    )
    current = gold.grid_report(spark, table_name="gold_history")
    assert [row["mapped_buildings"] for row in current["releases"]] == [2, 2]
    assert current["releases"][1] == combined["releases"][1]
    assert current["releases"][0]["source_run_sha256"] == corrected.summary_sha256
    assert warehouse_hashes(spark) == before


@pytest.mark.integration
@pytest.mark.parametrize("defect", ["duplicate", "null_flag", "mixed_runs", "analysis_drift"])
def test_report_rejects_invalid_history_without_writing(history_spark, history_run_factory, defect):
    spark = history_spark
    name = "gold_bad_" + defect
    receipt = publish_release(spark, history_run_factory(), table_name=name)
    facts = spark.table(receipt["table"])
    if defect == "duplicate":
        changed = facts.withColumn("building_id", F.lit("duplicate"))
        message = "Duplicate release/area/building"
    elif defect == "null_flag":
        changed = facts.withColumn("within_threshold", F.lit(None).cast("boolean"))
        message = "invalid reporting keys, flags or analysis"
    elif defect == "mixed_runs":
        changed = facts.withColumn(
            "source_run_sha256",
            F.when(F.col("building_id") == "b2", "0" * 64).otherwise(F.col("source_run_sha256")),
        )
        message = "multiple source runs"
    else:
        changed = facts.withColumn("threshold_m", F.lit(400.0))
        message = "invalid reporting keys, flags or analysis"
    changed.writeTo(receipt["table"]).overwrite(F.lit(True))
    before = warehouse_hashes(spark)
    with pytest.raises(ValueError, match=message):
        gold.grid_report(spark, table_name=name)
    assert warehouse_hashes(spark) == before


@pytest.mark.integration
def test_report_rejects_missing_snapshot_legacy_table_and_excess_rows(
    history_spark, history_run_factory, monkeypatch
):
    spark, run = history_spark, history_run_factory()
    publish_release(spark, run, table_name="gold_limits")
    with pytest.raises(ValueError, match="snapshot is not retained"):
        gold.grid_report(spark, table_name="gold_limits", snapshot_id=1)
    legacy = publish_facts(spark, run, table_name="gold_legacy")
    with pytest.raises(ValueError, match="not a supported release-history table"):
        gold.grid_report(spark, table_name="gold_legacy")
    with pytest.raises(ValueError, match="does not exist"):
        gold.grid_report(spark, table_name="gold_missing")
    before = warehouse_hashes(spark)
    monkeypatch.setattr(gold, "MAX_REPORT_FACTS", 1)
    with pytest.raises(ValueError, match="pilot limit exceeded"):
        gold.grid_report(spark, table_name="gold_limits")
    assert warehouse_hashes(spark) == before
    assert legacy["rows"] == 2


@pytest.mark.parametrize("defect", ["table", "snapshot", "existing_output"])
def test_report_cli_rejects_invalid_requests_before_spark(tmp_path, defect):
    output = tmp_path / "report.json"
    events = tmp_path / "events"
    command = [
        sys.executable,
        "scripts/report_grids.py",
        "--output",
        str(output),
        "--events-root",
        str(events),
    ]
    if defect == "table":
        command.extend(["--table-name", "history; DROP TABLE x"])
        message = "safe lowercase identifier"
    elif defect == "snapshot":
        command.extend(["--snapshot-id", "0"])
        message = "snapshot_id must be positive"
    else:
        output.write_text("keep me", encoding="utf-8")
        message = "output already exists"
    result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode != 0 and message in result.stderr
    assert not events.exists()
    if output.exists():
        assert output.read_text() == "keep me"
