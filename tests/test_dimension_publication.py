"""Snapshot-pinned dimension-pair publication, interrupted writes and exact retries."""

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from urban_geography.dimension_publication import publish_dimensions, read_dimension_publication
from urban_geography.ingest import digest
from urban_geography.lakehouse import publish_release


def warehouse_hashes(spark):
    root = Path(unquote(urlparse(spark.conf.get("spark.sql.catalog.local.warehouse")).path))
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


@pytest.mark.integration
def test_pair_publication_pins_old_rows_and_exact_retries(
    history_spark, history_run_factory, tmp_path
):
    spark = history_spark
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    publish_release(spark, first, table_name="pair_timeline")
    args = {
        "publication_root": tmp_path / "publications",
        "table_prefix": "pair_timeline",
        "fact_table_name": "pair_timeline",
    }
    a = publish_dimensions(spark, [first], **args)
    old = Path(a["path"])
    old_hash = digest(old / "manifest.json")
    publish_release(spark, second, table_name="pair_timeline")
    b = publish_dimensions(spark, [first, second], **args)
    current = read_dimension_publication(spark, Path(b["path"]))
    assert current.releases.count() == 2 and current.study_areas.count() == 1
    assert current.facts.count() == 4 and current.facts.filter("within_threshold").count() == 2
    historical = read_dimension_publication(spark, old)
    assert historical.releases.count() == 1 and historical.facts.count() == 2
    assert historical.manifest["payload"]["fact_snapshot_id"] == a["fact_snapshot_id"]
    assert digest(old / "manifest.json") == old_hash
    before = warehouse_hashes(spark)
    replay = publish_dimensions(spark, [second, first, first], **args)
    assert replay["status"] == "reused" and replay["path"] == b["path"]
    assert warehouse_hashes(spark) == before


@pytest.mark.integration
def test_mismatched_fact_run_blocks_publication_before_writes(
    history_spark, history_run_factory, tmp_path
):
    spark = history_spark
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    publish_release(spark, first, table_name="pair_missing")
    publish_release(spark, second, table_name="pair_missing")
    before = warehouse_hashes(spark)
    with pytest.raises(ValueError, match="readback differs"):
        publish_dimensions(
            spark,
            [first],
            publication_root=tmp_path / "publications",
            table_prefix="pair_missing",
            fact_table_name="pair_missing",
        )
    assert warehouse_hashes(spark) == before
    assert not (tmp_path / "publications").exists()


@pytest.mark.integration
def test_both_tables_are_preflighted_before_any_append(
    history_spark, history_run_factory, tmp_path
):
    spark = history_spark
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    publish_release(spark, first, table_name="pair_conflict")
    args = {
        "publication_root": tmp_path / "publications",
        "table_prefix": "pair_conflict",
        "fact_table_name": "pair_conflict",
    }
    old = publish_dimensions(spark, [first], **args)
    publish_release(spark, second, table_name="pair_conflict")
    area_table = "local.analytics.pair_conflict_study_areas"
    spark.table(area_table).withColumn("west", F.lit(-8.434)).writeTo(area_table).overwrite(
        F.lit(True)
    )
    before = warehouse_hashes(spark)
    with pytest.raises(ValueError, match="conflicting attributes"):
        publish_dimensions(spark, [first, second], **args)
    assert warehouse_hashes(spark) == before
    assert spark.table("local.analytics.pair_conflict_releases.snapshots").count() == 1
    assert read_dimension_publication(spark, Path(old["path"])).facts.count() == 2


@pytest.mark.integration
def test_failed_second_table_write_keeps_previous_publication_readable(
    history_spark, history_run_factory, tmp_path, monkeypatch
):
    spark = history_spark
    first = history_run_factory()
    publish_release(spark, first, table_name="pair_failure_old")
    root = tmp_path / "publications"
    old = publish_dimensions(
        spark,
        [first],
        publication_root=root,
        table_prefix="pair_failure",
        fact_table_name="pair_failure_old",
    )
    old_path = Path(old["path"])
    old_hash = digest(old_path / "manifest.json")
    changed_bbox = (-8.434, 40.195, -8.415, 40.215)
    newer = [
        history_run_factory(bbox=changed_bbox),
        history_run_factory("2026-10-21.0", bbox=changed_bbox),
    ]
    for run in newer:
        publish_release(spark, run, table_name="pair_failure_new")
    args = {
        "publication_root": root,
        "table_prefix": "pair_failure",
        "fact_table_name": "pair_failure_new",
    }
    original = DataFrame.writeTo

    def failing_write(frame, table):
        if table == "local.analytics.pair_failure_study_areas":
            frame = frame.withColumn(
                "study_area_sha256",
                F.expr("CAST(raise_error('injected dimension write failure') AS STRING)"),
            )
        return original(frame, table)

    with monkeypatch.context() as injection:
        injection.setattr(DataFrame, "writeTo", failing_write)
        with pytest.raises(Exception, match="injected dimension write failure"):
            publish_dimensions(spark, newer, **args)
    assert list(root.rglob("manifest.json")) == [old_path / "manifest.json"]
    assert digest(old_path / "manifest.json") == old_hash
    assert read_dimension_publication(spark, old_path).facts.count() == 2
    assert spark.table("local.analytics.pair_failure_releases.snapshots").count() == 2
    assert spark.table("local.analytics.pair_failure_study_areas.snapshots").count() == 1
    retry = publish_dimensions(spark, newer, **args)
    assert retry["status"] == "published"
    assert read_dimension_publication(spark, Path(retry["path"])).facts.count() == 4
    assert spark.table("local.analytics.pair_failure_releases.snapshots").count() == 2
    assert spark.table("local.analytics.pair_failure_study_areas.snapshots").count() == 2


@pytest.mark.integration
def test_completion_rename_failure_retries_without_extra_snapshots(
    history_spark, history_run_factory, tmp_path, monkeypatch
):
    spark, run = history_spark, history_run_factory()
    publish_release(spark, run, table_name="pair_completion")
    root = tmp_path / "publications"
    args = {
        "publication_root": root,
        "table_prefix": "pair_completion",
        "fact_table_name": "pair_completion",
    }
    original = Path.rename

    def failing_rename(path, target):
        if path.parent == root and path.name.startswith(".staging-"):
            raise RuntimeError("injected completion failure")
        return original(path, target)

    with monkeypatch.context() as injection:
        injection.setattr(Path, "rename", failing_rename)
        with pytest.raises(RuntimeError, match="injected completion failure"):
            publish_dimensions(spark, [run], **args)
    staged = next(root.glob(".staging-*/manifest.json"))
    manifest = json.loads(staged.read_text())
    final = root / manifest["publication_sha256"]
    with pytest.raises(FileNotFoundError):
        read_dimension_publication(spark, final)
    before = warehouse_hashes(spark)
    retry = publish_dimensions(spark, [run], **args)
    assert retry["status"] == "published" and Path(retry["path"]) == final
    assert warehouse_hashes(spark) == before
    assert read_dimension_publication(spark, final).facts.count() == 2


@pytest.mark.integration
def test_reader_rejects_changed_manifest(history_spark, history_run_factory, tmp_path):
    spark, run = history_spark, history_run_factory()
    publish_release(spark, run, table_name="pair_manifest")
    receipt = publish_dimensions(
        spark,
        [run],
        publication_root=tmp_path / "publications",
        table_prefix="pair_manifest",
        fact_table_name="pair_manifest",
    )
    path = Path(receipt["path"])
    manifest = json.loads((path / "manifest.json").read_text())
    manifest["tables"]["releases"]["snapshot_id"] = 1
    bad = tmp_path / "bad" / path.name
    bad.mkdir(parents=True)
    (bad / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="manifest is invalid"):
        read_dimension_publication(spark, bad)


@pytest.mark.parametrize("defect", ["identifier", "snapshot", "missing_run"])
def test_publication_cli_validates_before_spark(tmp_path, defect):
    events = tmp_path / "events"
    command = [
        sys.executable,
        "scripts/publish_dimensions.py",
        "--run",
        str(tmp_path / "missing"),
        "--events-root",
        str(events),
    ]
    if defect == "identifier":
        command.extend(["--table-prefix", "dim; DROP TABLE x"])
        message = "safe lowercase identifiers"
    elif defect == "snapshot":
        command.extend(["--fact-snapshot-id", "0"])
        message = "Fact snapshot ID must be positive"
    else:
        message = "summary.json"
    result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode != 0 and message in result.stderr
    assert not events.exists()


def test_retained_input_cap_is_checked_before_spark(history_run_factory, tmp_path):
    run = history_run_factory()
    oversized = replace(run, summary={**run.summary, "mapped_buildings": 40001})
    with pytest.raises(ValueError, match="Retained source runs exceed"):
        publish_dimensions(None, [oversized], publication_root=tmp_path / "publications")
    assert not (tmp_path / "publications").exists()
