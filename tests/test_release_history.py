"""Analysis comparability and real offline Iceberg release-history acceptance."""

import copy
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from urban_geography.config import RunConfig
from urban_geography.ingest import digest
from urban_geography.lakehouse import (
    FACT_TYPES,
    CompletedRun,
    analysis_definition,
    analysis_sha256,
    load_completed_run,
    publish_facts,
    publish_release,
)
from urban_geography.spark import create_session


def example_summary():
    config = RunConfig("offline_history", "2026-09-23.1", (-8.435, 40.195, -8.415, 40.215))
    return {
        "config": config.as_dict(),
        "metric_version": "park_proximity_v1",
        "selection": {"version": "point_on_surface_bbox_v1", "boundary": None},
    }


def definition_digest(summary):
    return analysis_sha256(CompletedRun(Path("unused"), summary, "0" * 64))


def test_analysis_comparability_ignores_release_and_operational_details():
    original = example_summary()
    later = copy.deepcopy(original)
    later["config"]["release"] = "2026-10-21.0"
    later["config"]["max_download_bytes"] *= 2
    later["config"]["max_features"] *= 2
    later["completed_at_utc"] = "2026-10-25T12:00:00Z"
    later["raw_manifest_sha256"] = "1" * 64
    later["files"] = {"new.parquet": "2" * 64}
    assert definition_digest(original) == definition_digest(later)


@pytest.mark.parametrize("change", ["area", "bbox", "threshold", "metric", "selection", "boundary"])
def test_changed_analysis_meaning_has_a_different_definition(change):
    original = example_summary()
    changed = copy.deepcopy(original)
    if change == "area":
        changed["config"]["area_id"] = "another_area"
    elif change == "bbox":
        changed["config"]["bbox"] = (-8.434, 40.195, -8.415, 40.215)
    elif change == "threshold":
        changed["config"]["threshold_m"] = 400
    elif change == "metric":
        changed["metric_version"] = "park_proximity_v2"
    elif change == "selection":
        changed["selection"]["version"] = "another_selection_v1"
    else:
        changed["selection"]["boundary"] = {
            "area_id": "municipality",
            "source_release": "CAOP2025",
            "geometry_sha256": "3" * 64,
        }
    assert definition_digest(original) != definition_digest(changed)


def test_boundary_provenance_refresh_is_not_an_analysis_change():
    original = example_summary()
    original["selection"]["boundary"] = {
        "area_id": "municipality",
        "source_release": "CAOP2025",
        "geometry_sha256": "3" * 64,
        "manifest_sha256": "4" * 64,
        "acquired_at_utc": "2026-10-24T00:00:00Z",
    }
    refreshed = copy.deepcopy(original)
    refreshed["selection"]["boundary"]["manifest_sha256"] = "5" * 64
    refreshed["selection"]["boundary"]["acquired_at_utc"] = "2026-10-25T00:00:00Z"
    assert definition_digest(original) == definition_digest(refreshed)
    refreshed["selection"]["boundary"]["geometry_sha256"] = "6" * 64
    assert definition_digest(original) != definition_digest(refreshed)


def test_numeric_representation_does_not_change_analysis_identity():
    first = example_summary()
    second = copy.deepcopy(first)
    second["config"]["threshold_m"] = 500
    assert definition_digest(first) == definition_digest(second)
    assert (
        analysis_definition(CompletedRun(Path("unused"), first, "0" * 64))["threshold_m"] == 500.0
    )


@pytest.fixture(scope="module")
def history_spark(tmp_path_factory):
    root = tmp_path_factory.mktemp("release-history")
    spark = create_session(root / "events", warehouse=root / "warehouse")
    yield spark
    spark.stop()


@pytest.fixture
def history_run_factory(tmp_path):
    made = []

    def make(release="2026-09-23.1", revision=0, *, bbox=None):
        root = tmp_path / ("run-" + str(len(made)))
        config = RunConfig("offline_history", release, bbox or (-8.435, 40.195, -8.415, 40.215))
        common = {
            "grid_id": "epsg3763_250m_0_0",
            "source_release": release,
            "area_id": config.area_id,
            "metric_version": "park_proximity_v1",
            "threshold_m": 500.0,
            "selection_version": "point_on_surface_bbox_v1",
            "boundary_id": None,
            "boundary_release": None,
            "boundary_geometry_sha256": None,
        }
        rows = [
            {
                **common,
                "building_id": "b1",
                "park_id": "park",
                "distance_m": 50.0 + 25 * revision,
                "within_threshold": True,
            },
            {
                **common,
                "building_id": "b3" if revision else "b2",
                "park_id": None,
                "distance_m": None,
                "within_threshold": False,
            },
        ]
        arrow_types = {"string": pa.string(), "double": pa.float64(), "boolean": pa.bool_()}
        schema = pa.schema([(name, arrow_types[kind]) for name, kind in FACT_TYPES.items()])
        facts = root / "building_proximity" / "part-00000.parquet"
        grid = root / "grid_proximity" / "part-00000.parquet"
        facts.parent.mkdir(parents=True)
        grid.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), facts)
        pq.write_table(pa.Table.from_pylist([{"mapped_buildings": 2, "buildings_within": 1}]), grid)
        summary = {
            "status": "complete",
            "config": config.as_dict(),
            "metric_version": "park_proximity_v1",
            "mapped_buildings": 2,
            "candidate_parks": 1,
            "buildings_within_500m": 1,
            "share_within": 0.5,
            "completed_at_utc": "2026-10-25T00:00:00Z",
            "source_url": "https://example.invalid/synthetic-release",
            "attribution": "Informação geográfica cedida pela Direção-Geral do Território",
            "selection": {
                "version": "point_on_surface_bbox_v1",
                "scope": "pilot_bbox",
                "bbox": list(config.bbox),
                "boundary": None,
            },
            "files": {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*.parquet")},
        }
        (root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
        made.append(root)
        return load_completed_run(root)

    return make


def canonical_rows(rows):
    return sorted(
        json.dumps(row.asDict() if hasattr(row, "asDict") else row, sort_keys=True) for row in rows
    )


def history_rows(spark, table, snapshot=None):
    reader = spark.read
    if snapshot is not None:
        reader = reader.option("snapshot-id", str(snapshot))
    return canonical_rows(reader.table(table).collect())


def expected_history_rows(*runs):
    return canonical_rows(
        [
            {**row, "source_run_sha256": run.summary_sha256}
            for run in runs
            for row in pq.read_table(run.path / "building_proximity").to_pylist()
        ]
    )


@pytest.mark.integration
def test_release_publication_replacement_replay_and_old_snapshots(
    history_spark, history_run_factory
):
    spark = history_spark
    first = history_run_factory()
    second = history_run_factory("2026-10-21.0")
    corrected = history_run_factory(revision=1)
    before_files = {
        str(p): digest(p)
        for run in (first, second, corrected)
        for p in run.path.rglob("*")
        if p.is_file()
    }
    a = publish_release(spark, first, table_name="timeline")
    b = publish_release(spark, second, table_name="timeline")
    table = a["table"]
    assert a["status"] == "created" and b["status"] == "published"
    assert history_rows(spark, table) == expected_history_rows(first, second)
    assert history_rows(spark, table, a["snapshot_id"]) == expected_history_rows(first)
    replay = publish_release(spark, first, table_name="timeline")
    assert replay["status"] == "reused" and replay["snapshot_id"] == b["snapshot_id"]
    assert spark.table(table + ".snapshots").count() == 2
    with pytest.raises(ValueError, match="replacement must be explicit"):
        publish_release(spark, corrected, table_name="timeline")
    c = publish_release(
        spark, corrected, table_name="timeline", replace=True, expected_snapshot_id=b["snapshot_id"]
    )
    assert c["status"] == "replaced" and c["previous_snapshot_id"] == b["snapshot_id"]
    assert c["rows_in_release"] == 2
    assert history_rows(spark, table) == expected_history_rows(corrected, second)
    assert history_rows(spark, table, a["snapshot_id"]) == expected_history_rows(first)
    assert history_rows(spark, table, b["snapshot_id"]) == expected_history_rows(first, second)
    assert {
        row.building_id
        for row in spark.table(table).filter("source_release = '2026-09-23.1'").collect()
    } == {"b1", "b3"}
    commits = {row.snapshot_id: row.summary for row in spark.table(table + ".snapshots").collect()}
    for receipt, run in ((a, first), (b, second), (c, corrected)):
        assert commits[receipt["snapshot_id"]]["source-run-sha256"] == run.summary_sha256
        assert json.loads(commits[receipt["snapshot_id"]]["source-run-summary"]) == run.summary
        assert commits[receipt["snapshot_id"]]["analysis-sha256"] == analysis_sha256(run)
    retry = publish_release(
        spark, corrected, table_name="timeline", replace=True, expected_snapshot_id=b["snapshot_id"]
    )
    assert retry["status"] == "reused" and retry["snapshot_id"] == c["snapshot_id"]
    assert spark.table(table + ".snapshots").count() == 3
    assert {
        str(p): digest(p)
        for run in (first, second, corrected)
        for p in run.path.rglob("*")
        if p.is_file()
    } == before_files


@pytest.mark.integration
def test_stale_expected_snapshot_rejects_changed_release(history_spark, history_run_factory):
    spark = history_spark
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    a = publish_release(spark, first, table_name="stale")
    b = publish_release(spark, second, table_name="stale")
    with pytest.raises(ValueError, match="Current snapshot differs"):
        publish_release(
            spark,
            history_run_factory(revision=1),
            table_name="stale",
            replace=True,
            expected_snapshot_id=a["snapshot_id"],
        )
    assert history_rows(spark, a["table"]) == expected_history_rows(first, second)
    current = spark.table(a["table"] + ".refs").filter("name = 'main'").first()
    assert current is not None and current.snapshot_id == b["snapshot_id"]
    assert spark.table(a["table"] + ".snapshots").count() == 2


@pytest.mark.integration
def test_analysis_change_is_rejected_without_a_snapshot(history_spark, history_run_factory):
    spark = history_spark
    first = history_run_factory()
    a = publish_release(spark, first, table_name="analysis_guard")
    different = history_run_factory("2026-10-21.0", bbox=(-8.434, 40.195, -8.415, 40.215))
    with pytest.raises(ValueError, match="Analysis definition differs"):
        publish_release(spark, different, table_name="analysis_guard")
    assert history_rows(spark, a["table"]) == expected_history_rows(first)
    assert spark.table(a["table"] + ".snapshots").count() == 1


@pytest.mark.integration
def test_replacement_requires_snapshot_and_existing_partition(
    history_spark, history_run_factory, tmp_path
):
    spark, run = history_spark, history_run_factory()
    with pytest.raises(ValueError, match="requires an expected_snapshot_id"):
        publish_release(spark, run, replace=True, table_name="missing")
    with pytest.raises(ValueError, match="missing history table"):
        publish_release(spark, run, replace=True, expected_snapshot_id=1, table_name="missing")
    a = publish_release(spark, run, table_name="replace_guard")
    with pytest.raises(ValueError, match="missing release partition"):
        publish_release(
            spark,
            history_run_factory("2026-10-21.0"),
            table_name="replace_guard",
            replace=True,
            expected_snapshot_id=a["snapshot_id"],
        )
    result = subprocess.run(
        [
            sys.executable,
            "scripts/publish_release.py",
            "--replace",
            "--run",
            str(tmp_path / "missing-input"),
            "--events-root",
            str(tmp_path / "events"),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode != 0
    assert "--replace requires --expected-snapshot-id" in result.stderr
    assert not (tmp_path / "events").exists()


@pytest.mark.integration
def test_history_mode_does_not_migrate_the_single_run_table(history_spark, history_run_factory):
    spark, run = history_spark, history_run_factory()
    legacy = publish_facts(spark, run, table_name="legacy")
    before = history_rows(spark, legacy["table"])
    with pytest.raises(ValueError, match="not a supported release-history table"):
        publish_release(spark, run, table_name="legacy")
    assert history_rows(spark, legacy["table"]) == before
    assert spark.table(legacy["table"] + ".snapshots").count() == 1


@pytest.mark.integration
def test_failed_spark_write_preserves_committed_history(
    history_spark, history_run_factory, monkeypatch
):
    spark = history_spark
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    a = publish_release(spark, first, table_name="failed_write")
    b = publish_release(spark, second, table_name="failed_write")
    corrected = history_run_factory(revision=1)
    original_write_to = DataFrame.writeTo

    def failing_write_to(frame, table):
        # Alter only the writer input after the production quality checks have completed.
        # The real Iceberg writer and commit path still execute.
        failing = frame.withColumn(
            "distance_m",
            F.expr(
                "CAST(raise_error(concat('injected release write failure ', "
                "spark_partition_id())) AS DOUBLE)"
            ),
        )
        return original_write_to(failing, table)

    group = "release-write-failure"
    spark.sparkContext.setJobGroup(group, "Exercise a real failed Iceberg write")
    try:
        with monkeypatch.context() as injection:
            injection.setattr(DataFrame, "writeTo", failing_write_to)
            with pytest.raises(Exception, match="injected release write failure"):
                publish_release(
                    spark,
                    corrected,
                    table_name="failed_write",
                    replace=True,
                    expected_snapshot_id=b["snapshot_id"],
                )
        tracker = spark.sparkContext.statusTracker()
        jobs = [tracker.getJobInfo(job) for job in tracker.getJobIdsForGroup(group)]
        assert any(job is not None and job.status == "FAILED" for job in jobs)
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)
    assert history_rows(spark, a["table"]) == expected_history_rows(first, second)
    assert history_rows(spark, a["table"], a["snapshot_id"]) == expected_history_rows(first)
    assert spark.table(a["table"] + ".snapshots").count() == 2
    current = spark.table(a["table"] + ".refs").filter("name = 'main'").first()
    assert current is not None and current.snapshot_id == b["snapshot_id"]
    successful_retry = publish_release(
        spark,
        corrected,
        table_name="failed_write",
        replace=True,
        expected_snapshot_id=b["snapshot_id"],
    )
    assert successful_retry["status"] == "replaced"
    assert history_rows(spark, a["table"]) == expected_history_rows(corrected, second)
    assert spark.table(a["table"] + ".snapshots").count() == 3
