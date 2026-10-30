"""Real local writer sessions: verified publication/replay and wrong-warehouse refusal."""

import uuid

import pytest
from pyspark import SparkContext
from pyspark.sql.readwriter import DataFrameReader

from urban_geography.glue_input import stage_input
from urban_geography.glue_writer import run_writer
from urban_geography.ingest import digest
from urban_geography.spark import create_session

pytestmark = pytest.mark.integration


def settings(warehouse):
    return {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": warehouse.as_uri(),
        "spark.sql.catalog.local.table-override.format-version": "2",
    }


def test_writer_creates_then_reuses_across_owned_sessions(
    history_run_factory, tmp_path, monkeypatch
):
    source = history_run_factory()
    staged = tmp_path / "staged"
    stage_input(source.path, source.summary_sha256, staged)
    before = {str(p): digest(p) for p in staged.rglob("*") if p.is_file()}
    warehouse = tmp_path / "warehouse"

    def start():
        return create_session(tmp_path / "events" / uuid.uuid4().hex, warehouse=warehouse)

    options = {
        "catalog_name": "local",
        "namespace": "writer_test",
        "expected_settings": settings(warehouse),
        "session_factory": start,
    }
    # A cloud executor cannot assume the driver's local Parquet paths exist there.
    monkeypatch.setattr(
        DataFrameReader,
        "parquet",
        lambda *args, **kwargs: pytest.fail("Writer must distribute driver rows"),
    )
    first = run_writer(staged, source.summary_sha256, **options)
    assert first["status"] == "created" and first["rows"] == 2
    assert first["table"] == "local.writer_test.building_proximity"
    assert first["readback_matches"] and SparkContext._active_spark_context is None
    again = run_writer(staged, source.summary_sha256, **options)
    assert again == first | {"status": "reused"}
    assert SparkContext._active_spark_context is None
    check = start()
    try:
        assert check.table(first["table"] + ".snapshots").count() == 1
        unmatched = check.table(first["table"]).filter("NOT within_threshold").first()
        assert unmatched.distance_m is None and unmatched.park_id is None
    finally:
        check.stop()
    assert {str(p): digest(p) for p in staged.rglob("*") if p.is_file()} == before


def test_wrong_actual_warehouse_is_refused_before_any_table_write(history_run_factory, tmp_path):
    source = history_run_factory()
    staged = tmp_path / "staged"
    stage_input(source.path, source.summary_sha256, staged)
    actual = tmp_path / "actual-warehouse"
    expected = tmp_path / "expected-warehouse"
    with pytest.raises(ValueError, match="catalog.local.warehouse"):
        run_writer(
            staged,
            source.summary_sha256,
            catalog_name="local",
            namespace="writer_test",
            expected_settings=settings(expected),
            session_factory=lambda: create_session(tmp_path / "events", warehouse=actual),
        )
    assert SparkContext._active_spark_context is None
    assert not (actual / "writer_test").exists()
    assert not expected.exists()
