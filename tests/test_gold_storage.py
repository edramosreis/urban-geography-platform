"""Offline materialized Gold acceptance: saved counts, lineage, retries and failed writes."""

import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameReader

import urban_geography.gold_storage as storage
from urban_geography.dimension_publication import publish_dimensions
from urban_geography.ingest import digest
from urban_geography.lakehouse import publish_release


def make_source(spark, factory, root, name):
    first = factory()
    args = {
        "publication_root": root / "dimensions",
        "table_prefix": "gold_dim_" + name,
        "fact_table_name": "gold_source_" + name,
    }
    publish_release(spark, first, table_name=args["fact_table_name"])
    receipt = publish_dimensions(spark, [first], **args)
    return first, args, Path(receipt["path"])


def warehouse_hashes(spark):
    root = Path(unquote(urlparse(spark.conf.get("spark.sql.catalog.local.warehouse")).path))
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


@pytest.mark.integration
def test_saved_grid_runs_release_totals_old_snapshots_and_replay(
    history_spark, history_run_factory, tmp_path
):
    spark = history_spark
    first, args, source = make_source(spark, history_run_factory, tmp_path, "timeline")
    a = storage.publish_gold_run(spark, source, table_name="saved_timeline")
    initial = storage.read_gold_run(spark, a["gold_run_sha256"], table_name="saved_timeline")
    assert initial.analysis_definition["threshold_m"] == 500.0
    assert initial.analysis_definition["metric_version"] == "park_proximity_v1"
    assert a["status"] == "created" and initial.grids.count() == 1
    assert initial.releases.first().mapped_buildings == 2
    assert initial.releases.first().buildings_within == 1
    assert initial.releases.first().share_within == 0.5
    second = history_run_factory("2026-10-21.0")
    publish_release(spark, second, table_name=args["fact_table_name"])
    later_source = publish_dimensions(spark, [first, second], **args)
    b = storage.publish_gold_run(
        spark,
        Path(later_source["path"]),
        table_name="saved_timeline",
        expected_snapshot_id=a["snapshot_id"],
    )
    current = storage.read_gold_run(spark, b["gold_run_sha256"], table_name="saved_timeline")
    assert b["status"] == "published" and current.grids.count() == 2
    assert [row.mapped_buildings for row in current.releases.collect()] == [2, 2]
    assert [row.share_within for row in current.releases.collect()] == [0.5, 0.5]
    assert spark.table(a["table"]).count() == 3
    assert (
        storage.read_gold_run(
            spark, a["gold_run_sha256"], table_name="saved_timeline"
        ).grids.count()
        == 1
    )
    assert (
        storage.read_gold_run(
            spark, a["gold_run_sha256"], table_name="saved_timeline", snapshot_id=a["snapshot_id"]
        ).grids.count()
        == 1
    )
    with pytest.raises(ValueError, match="not present in the selected snapshot"):
        storage.read_gold_run(
            spark, b["gold_run_sha256"], table_name="saved_timeline", snapshot_id=a["snapshot_id"]
        )
    before = warehouse_hashes(spark)
    replay = storage.publish_gold_run(
        spark,
        Path(later_source["path"]),
        table_name="saved_timeline",
        expected_snapshot_id=a["snapshot_id"],
    )
    assert replay["status"] == "reused" and replay["snapshot_id"] == b["snapshot_id"]
    assert warehouse_hashes(spark) == before


@pytest.mark.integration
def test_saved_reads_and_replays_do_not_scan_source_tables(
    history_spark, history_run_factory, tmp_path, monkeypatch
):
    spark = history_spark
    _, _, source = make_source(spark, history_run_factory, tmp_path, "standalone")
    receipt = storage.publish_gold_run(spark, source, table_name="saved_standalone")
    allowed = receipt["table"]
    original_session, original_reader = SparkSession.table, DataFrameReader.table

    def checked_session(session, name):
        assert name == allowed or name.startswith(allowed + "."), "Source table scan attempted"
        return original_session(session, name)

    def checked_reader(reader, name):
        assert name == allowed, "Source table scan attempted"
        return original_reader(reader, name)

    def source_read_forbidden(*args, **kwargs):
        raise AssertionError("Completed Gold run reopened its source publication")

    before = warehouse_hashes(spark)
    with monkeypatch.context() as guard:
        guard.setattr(SparkSession, "table", checked_session)
        guard.setattr(DataFrameReader, "table", checked_reader)
        guard.setattr(storage, "read_dimension_publication", source_read_forbidden)
        opened = storage.read_gold_run(
            spark, receipt["gold_run_sha256"], table_name="saved_standalone"
        )
        assert opened.grids.count() == 1 and opened.releases.first().mapped_buildings == 2
        assert (
            storage.publish_gold_run(spark, source, table_name="saved_standalone")["status"]
            == "reused"
        )
    assert warehouse_hashes(spark) == before


@pytest.mark.integration
def test_failed_gold_append_preserves_prior_run_and_retries(
    history_spark, history_run_factory, tmp_path, monkeypatch
):
    spark = history_spark
    first, args, source = make_source(spark, history_run_factory, tmp_path, "failure")
    a = storage.publish_gold_run(spark, source, table_name="saved_failure")
    second = history_run_factory("2026-10-21.0")
    publish_release(spark, second, table_name=args["fact_table_name"])
    later = Path(publish_dimensions(spark, [first, second], **args)["path"])
    original = DataFrame.writeTo

    def failing_write(frame, table):
        if table == a["table"]:
            frame = frame.withColumn(
                "mapped_buildings",
                F.expr("CAST(raise_error('injected Gold write failure') AS BIGINT)"),
            )
        return original(frame, table)

    with monkeypatch.context() as injection:
        injection.setattr(DataFrame, "writeTo", failing_write)
        with pytest.raises(Exception, match="injected Gold write failure"):
            storage.publish_gold_run(spark, later, table_name="saved_failure")
    assert spark.table(a["table"] + ".snapshots").count() == 1
    assert (
        storage.read_gold_run(spark, a["gold_run_sha256"], table_name="saved_failure")
        .releases.first()
        .mapped_buildings
        == 2
    )
    retry = storage.publish_gold_run(spark, later, table_name="saved_failure")
    assert retry["status"] == "published" and retry["grid_rows"] == 2
    assert spark.table(a["table"] + ".snapshots").count() == 2


@pytest.mark.integration
def test_stale_expected_snapshot_blocks_new_gold_run(history_spark, history_run_factory, tmp_path):
    spark = history_spark
    first, args, source = make_source(spark, history_run_factory, tmp_path, "stale")
    a = storage.publish_gold_run(spark, source, table_name="saved_stale")
    second = history_run_factory("2026-10-21.0")
    publish_release(spark, second, table_name=args["fact_table_name"])
    later = Path(publish_dimensions(spark, [first, second], **args)["path"])
    before = warehouse_hashes(spark)
    with pytest.raises(ValueError, match="differs from expected_snapshot_id"):
        storage.publish_gold_run(
            spark, later, table_name="saved_stale", expected_snapshot_id=a["snapshot_id"] + 1
        )
    assert warehouse_hashes(spark) == before


@pytest.mark.integration
def test_content_fingerprint_rejects_validly_shaped_changed_counts(
    history_spark, history_run_factory, tmp_path
):
    spark = history_spark
    _, _, source = make_source(spark, history_run_factory, tmp_path, "content")
    a = storage.publish_gold_run(spark, source, table_name="saved_content")
    changed = (
        spark.table(a["table"])
        .withColumn("mapped_buildings", F.lit(4).cast("long"))
        .withColumn("share_within", F.lit(0.25))
    )
    changed.writeTo(a["table"]).overwrite(F.lit(True))
    with pytest.raises(ValueError, match="content hash or input count differs"):
        storage.read_gold_run(spark, a["gold_run_sha256"], table_name="saved_content")
    original = storage.read_gold_run(
        spark, a["gold_run_sha256"], table_name="saved_content", snapshot_id=a["snapshot_id"]
    )
    assert original.releases.first().mapped_buildings == 2


@pytest.mark.integration
def test_fact_table_cannot_be_used_as_gold_target(history_spark, history_run_factory, tmp_path):
    spark = history_spark
    _, args, source = make_source(spark, history_run_factory, tmp_path, "wrong_target")
    before = warehouse_hashes(spark)
    with pytest.raises(ValueError, match="not a supported Gold"):
        storage.publish_gold_run(spark, source, table_name=args["fact_table_name"])
    assert warehouse_hashes(spark) == before


@pytest.mark.integration
def test_read_requires_the_recorded_sql_version(
    history_spark, history_run_factory, tmp_path, monkeypatch
):
    spark = history_spark
    _, _, source = make_source(spark, history_run_factory, tmp_path, "sql_version")
    a = storage.publish_gold_run(spark, source, table_name="saved_sql_version")
    original = storage.load_sql_models

    def changed_models():
        models = original()
        models["release_proximity"] += "\n-- another model version\n"
        return models

    monkeypatch.setattr(storage, "load_sql_models", changed_models)
    with pytest.raises(ValueError, match="SQL version differs"):
        storage.read_gold_run(spark, a["gold_run_sha256"], table_name="saved_sql_version")


@pytest.mark.parametrize("defect", ["identifier", "snapshot", "missing_publication"])
def test_gold_cli_rejects_invalid_requests_before_spark(tmp_path, defect):
    events = tmp_path / "events"
    command = [
        sys.executable,
        "scripts/publish_gold.py",
        "--dimension-publication",
        str(tmp_path / "missing"),
        "--events-root",
        str(events),
    ]
    if defect == "identifier":
        command.extend(["--table-name", "gold; DROP TABLE x"])
        message = "safe lowercase identifier"
    elif defect == "snapshot":
        command.extend(["--expected-snapshot-id", "0"])
        message = "Gold snapshot ID must be positive"
    else:
        message = "completion is missing"
    result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode != 0 and message in result.stderr
    assert not events.exists()
