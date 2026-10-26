"""Real offline Iceberg schema migration and publication compatibility acceptance."""

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow.parquet as pq
import pytest

from urban_geography.ingest import digest
from urban_geography.lakehouse import (
    HISTORY_TYPES,
    evolve_history_schema,
    load_completed_run,
    publish_facts,
    publish_release,
)

pytestmark = pytest.mark.integration


def metadata(spark, table):
    entry = (
        spark.table(table + ".metadata_log_entries").orderBy("timestamp", ascending=False).first()
    )
    assert entry is not None
    return json.loads(Path(unquote(urlparse(entry.file).path)).read_text(encoding="utf-8"))


def fields(info):
    schema = next(
        item for item in info["schemas"] if item["schema-id"] == info["current-schema-id"]
    )
    return {field["name"]: field for field in schema["fields"]}


def file_hashes(root, *, payload_only=False):
    return {
        p.relative_to(root).as_posix(): digest(p)
        for p in root.rglob("*")
        if p.is_file() and (not payload_only or p.suffix in {".parquet", ".avro"})
    }


def core_rows(spark, table, snapshot=None):
    reader = spark.read
    if snapshot is not None:
        reader = reader.option("snapshot-id", str(snapshot))
    return sorted(
        json.dumps(row.asDict(), sort_keys=True)
        for row in reader.table(table).select(*HISTORY_TYPES).collect()
    )


def expected_rows(*runs):
    return sorted(
        json.dumps({**row, "source_run_sha256": run.summary_sha256}, sort_keys=True)
        for run in runs
        for row in pq.read_table(run.path / "building_proximity").to_pylist()
    )


def change_completion(run, value):
    path = run.path / "summary.json"
    summary = json.loads(path.read_text())
    summary["completed_at_utc"] = value
    path.write_text(json.dumps(summary))
    return load_completed_run(run.path)


def release_times(spark, table, release):
    return [
        row.ms
        for row in spark.table(table)
        .filter(f"source_release = '{release}'")
        .selectExpr("unix_millis(processed_at_utc) AS ms")
        .collect()
    ]


def test_migration_preserves_files_ids_snapshots_and_supports_future_publication(
    history_spark, history_run_factory
):
    spark = history_spark
    first = history_run_factory()
    initial = publish_release(spark, first, table_name="evolution")
    table = initial["table"]
    before = metadata(spark, table)
    root = Path(unquote(urlparse(before["location"]).path))
    payload_before = file_hashes(root, payload_only=True)
    old_fields = fields(before)
    result = evolve_history_schema(
        spark, table_name="evolution", expected_schema_id=before["current-schema-id"]
    )
    after = metadata(spark, table)
    assert result["status"] == "evolved"
    assert result["snapshot_id"] == initial["snapshot_id"]
    assert after["current-schema-id"] != before["current-schema-id"]
    assert {key: fields(after)[key] for key in old_fields} == old_fields
    added = fields(after)["processed_at_utc"]
    assert added["required"] is False and added["type"] == "timestamptz"
    assert added["id"] > max(field["id"] for field in old_fields.values())
    assert file_hashes(root, payload_only=True) == payload_before
    assert core_rows(spark, table) == expected_rows(first)
    assert release_times(spark, table, first.summary["config"]["release"]) == [None, None]
    assert spark.table(table + ".snapshots").count() == 1
    historical = spark.sql(f"SELECT * FROM {table} VERSION AS OF {initial['snapshot_id']}")
    assert set(historical.columns) == set(HISTORY_TYPES)
    assert historical.count() == 2

    files_before_retry = file_hashes(root)
    assert (
        evolve_history_schema(
            spark, table_name="evolution", expected_schema_id=before["current-schema-id"]
        )["status"]
        == "reused"
    )
    old_replay = publish_release(spark, first, table_name="evolution")
    assert (
        old_replay["status"] == "reused" and old_replay["schema_id"] == after["current-schema-id"]
    )
    assert old_replay["snapshot_id"] == initial["snapshot_id"]
    assert file_hashes(root) == files_before_retry

    second = change_completion(
        history_run_factory("2026-10-21.0"), "2026-10-25T03:04:05.123987+01:00"
    )
    second_result = publish_release(spark, second, table_name="evolution")
    expected_ms = int(datetime(2026, 10, 25, 2, 4, 5, tzinfo=UTC).timestamp()) * 1000 + 123
    assert release_times(spark, table, "2026-10-21.0") == [expected_ms, expected_ms]
    assert release_times(spark, table, "2026-09-23.1") == [None, None]
    assert core_rows(spark, table) == expected_rows(first, second)
    assert publish_release(spark, second, table_name="evolution")["status"] == "reused"
    assert publish_release(spark, first, table_name="evolution")["status"] == "reused"
    assert spark.table(table + ".snapshots").count() == 2

    corrected = history_run_factory(revision=1)
    correction = publish_release(
        spark,
        corrected,
        table_name="evolution",
        replace=True,
        expected_snapshot_id=second_result["snapshot_id"],
    )
    assert all(value is not None for value in release_times(spark, table, "2026-09-23.1"))
    assert core_rows(spark, table) == expected_rows(corrected, second)
    assert core_rows(spark, table, initial["snapshot_id"]) == expected_rows(first)
    assert release_times(spark, table, "2026-10-21.0") == [expected_ms, expected_ms]
    # Restoring the exact old source now creates a write under the new schema.
    restored = publish_release(
        spark,
        first,
        table_name="evolution",
        replace=True,
        expected_snapshot_id=correction["snapshot_id"],
    )
    assert all(value is not None for value in release_times(spark, table, "2026-09-23.1"))
    replay = publish_release(spark, first, table_name="evolution")
    assert replay["status"] == "reused" and replay["snapshot_id"] == restored["snapshot_id"]
    assert core_rows(spark, table) == expected_rows(first, second)
    assert spark.table(table + ".snapshots").count() == 4


def test_bad_processing_time_fails_before_an_extended_write(history_spark, history_run_factory):
    spark, first = history_spark, history_run_factory()
    initial = publish_release(spark, first, table_name="bad_completion")
    evolve_history_schema(
        spark, table_name="bad_completion", expected_schema_id=initial["schema_id"]
    )
    before = metadata(spark, initial["table"])
    root = Path(unquote(urlparse(before["location"]).path))
    files_before = file_hashes(root)
    bad = change_completion(history_run_factory("2026-10-21.0"), "2026-10-25T01:02:03")
    with pytest.raises(ValueError, match="timezone-aware completed_at_utc"):
        publish_release(spark, bad, table_name="bad_completion")
    assert core_rows(spark, initial["table"]) == expected_rows(first)
    assert file_hashes(root) == files_before
    assert spark.table(initial["table"] + ".snapshots").count() == 1


@pytest.mark.parametrize("change", ["unknown_column", "wrong_type"])
def test_unapproved_schema_is_rejected_without_an_additional_commit(
    history_spark, history_run_factory, change
):
    spark, first = history_spark, history_run_factory()
    name = "unsupported_" + change
    initial = publish_release(spark, first, table_name=name)
    table = initial["table"]
    extra = "unexpected STRING" if change == "unknown_column" else "processed_at_utc STRING"
    spark.sql(f"ALTER TABLE {table} ADD COLUMN {extra}")
    before = metadata(spark, table)
    root = Path(unquote(urlparse(before["location"]).path))
    files_before = file_hashes(root)
    message = "schema differs" if change == "unknown_column" else "nullable UTC timestamp"
    with pytest.raises(ValueError, match=message):
        evolve_history_schema(
            spark, table_name=name, expected_schema_id=before["current-schema-id"]
        )
    with pytest.raises(ValueError, match=message):
        publish_release(spark, history_run_factory("2026-10-21.0"), table_name=name)
    assert file_hashes(root) == files_before
    assert core_rows(spark, table) == expected_rows(first)


def test_migration_guards_missing_legacy_and_stale_schema(
    history_spark, history_run_factory, tmp_path
):
    spark, first = history_spark, history_run_factory()
    initial = publish_release(spark, first, table_name="schema_guard")
    with pytest.raises(ValueError, match="Current schema differs"):
        evolve_history_schema(
            spark, table_name="schema_guard", expected_schema_id=initial["schema_id"] + 1
        )
    with pytest.raises(ValueError, match="must be nonnegative"):
        evolve_history_schema(spark, table_name="schema_guard", expected_schema_id=-1)
    with pytest.raises(ValueError, match="does not exist"):
        evolve_history_schema(spark, table_name="missing_schema_table", expected_schema_id=0)
    legacy = publish_facts(spark, first, table_name="schema_legacy")
    with pytest.raises(ValueError, match="not a supported release-history table"):
        evolve_history_schema(spark, table_name="schema_legacy", expected_schema_id=0)
    assert spark.table(legacy["table"] + ".snapshots").count() == 1
    assert "processed_at_utc" not in spark.table(initial["table"]).columns
    result = subprocess.run(
        [
            sys.executable,
            "scripts/evolve_history_schema.py",
            "--expected-schema-id",
            "-1",
            "--events-root",
            str(tmp_path / "operator-events"),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode != 0 and "--expected-schema-id must be nonnegative" in result.stderr
    assert not (tmp_path / "operator-events").exists()


def test_unsafe_type_change_is_rejected_by_iceberg(history_spark, history_run_factory):
    spark, first = history_spark, history_run_factory()
    initial = publish_release(spark, first, table_name="type_guard")
    before = metadata(spark, initial["table"])
    with pytest.raises(Exception, match="Cannot update|cannot change|Unsupported|not supported"):
        spark.sql(f"ALTER TABLE {initial['table']} ALTER COLUMN distance_m TYPE STRING")
    assert metadata(spark, initial["table"]) == before
    assert core_rows(spark, initial["table"]) == expected_rows(first)


def test_new_schema_null_processing_time_is_not_treated_as_legacy(
    history_spark, history_run_factory
):
    spark, first = history_spark, history_run_factory()
    initial = publish_release(spark, first, table_name="timestamp_integrity")
    table = initial["table"]
    evolve_history_schema(
        spark, table_name="timestamp_integrity", expected_schema_id=initial["schema_id"]
    )
    second = history_run_factory("2026-10-21.0")
    valid = publish_release(spark, second, table_name="timestamp_integrity")
    spark.sql(f"UPDATE {table} SET processed_at_utc = NULL WHERE source_release = '2026-10-21.0'")
    before = metadata(spark, table)
    with pytest.raises(ValueError, match="Stored release differs from its source run"):
        publish_release(spark, second, table_name="timestamp_integrity")
    assert metadata(spark, table) == before
    assert core_rows(spark, table) == expected_rows(first, second)
    old = spark.read.option("snapshot-id", str(valid["snapshot_id"])).table(table)
    assert old.filter("source_release = '2026-10-21.0' AND processed_at_utc IS NULL").count() == 0
