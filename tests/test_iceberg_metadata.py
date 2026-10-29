"""Real offline metadata inspection through Iceberg, independently checked against disk."""

import json
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException

from urban_geography.ingest import digest
from urban_geography.lakehouse import (
    _table_metadata,
    evolve_history_schema,
    publish_release,
)

pytestmark = pytest.mark.integration


def hashes(root):
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


def committed_file(spark, table_name):
    # Independent oracle for this test's Hadoop catalog, not production metadata resolution.
    warehouse = Path(unquote(urlparse(spark.conf.get("spark.sql.catalog.local.warehouse")).path))
    directory = warehouse / "analytics" / table_name / "metadata"
    version = int((directory / "version-hint.text").read_text(encoding="utf-8"))
    return json.loads((directory / f"v{version}.metadata.json").read_text(encoding="utf-8"))


def warehouse_root(spark):
    return Path(unquote(urlparse(spark.conf.get("spark.sql.catalog.local.warehouse")).path))


def test_metadata_matches_disk_and_observes_schema_only_commit(history_spark, history_run_factory):
    spark = history_spark
    run = history_run_factory()
    published = publish_release(spark, run, table_name="metadata_inspection")
    table = published["table"]
    root = warehouse_root(spark)
    before = hashes(root)
    initial = _table_metadata(spark, table)
    assert initial == committed_file(spark, "metadata_inspection")
    assert json.loads(initial["properties"]["analysis-definition"])["area_id"] == "offline_history"
    source = initial["snapshots"][0]["summary"]
    assert source["source-run-sha256"] == run.summary_sha256
    assert json.loads(source["source-run-summary"]) == run.summary
    assert "Direção-Geral" in json.loads(source["source-run-summary"])["attribution"]
    assert initial["current-snapshot-id"] == published["snapshot_id"]
    assert isinstance(initial["current-snapshot-id"], int)
    # Full metadata equality also verifies exact integer snapshot IDs.
    assert _table_metadata(spark, table) == initial
    assert hashes(root) == before

    evolve_history_schema(
        spark,
        table_name="metadata_inspection",
        expected_schema_id=initial["current-schema-id"],
    )
    before = hashes(root)
    updated = _table_metadata(spark, table)
    assert updated == committed_file(spark, "metadata_inspection")
    assert updated["current-schema-id"] != initial["current-schema-id"]
    assert updated["current-snapshot-id"] == initial["current-snapshot-id"]
    assert updated["snapshots"] == initial["snapshots"]
    assert _table_metadata(spark, table) == updated
    assert hashes(root) == before


def test_empty_table_metadata_is_available_without_a_data_snapshot(history_spark):
    spark = history_spark
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.analytics")
    spark.sql(
        "CREATE TABLE local.analytics.empty_metadata (id BIGINT) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    root = warehouse_root(spark)
    before = hashes(root)
    actual = _table_metadata(spark, "local.analytics.empty_metadata")
    assert actual == committed_file(spark, "empty_metadata")
    assert actual["current-snapshot-id"] == -1
    assert actual["snapshots"] == []
    assert actual["schemas"][0]["fields"][0]["type"] == "long"
    assert hashes(root) == before


@pytest.mark.parametrize("target", ["local.analytics.missing_metadata", "non_iceberg_metadata"])
def test_missing_and_non_iceberg_targets_fail_without_writes(history_spark, target):
    spark = history_spark
    spark.createDataFrame([(1,)], "id long").createOrReplaceTempView("non_iceberg_metadata")
    root = warehouse_root(spark)
    before = hashes(root)
    with pytest.raises((AnalysisException, Py4JJavaError)):
        _table_metadata(spark, target)
    assert hashes(root) == before
    assert not spark.catalog.tableExists("local.analytics.missing_metadata")
