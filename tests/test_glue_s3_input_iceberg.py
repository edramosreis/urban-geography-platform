"""Downloaded prepared input feeds the real local writer; the object store is in memory."""

import uuid

import pytest
from pyspark import SparkContext
from s3_input_fixtures import MemoryS3

from urban_geography.glue_input import stage_input
from urban_geography.glue_s3_input import download_prepared_input
from urban_geography.glue_writer import run_writer
from urban_geography.ingest import digest
from urban_geography.spark import create_session

pytestmark = pytest.mark.integration


def test_download_then_write_and_replay(history_run_factory, tmp_path):
    run = history_run_factory()
    prepared, downloaded = tmp_path / "prepared", tmp_path / "downloaded"
    stage_input(run.path, run.summary_sha256, prepared)
    client = MemoryS3(prepared, run.summary_sha256)
    transfer = download_prepared_input(
        client,
        deployment_name="urban-geo-test",
        account_id="123456789012",
        expected_summary_sha256=run.summary_sha256,
        expected_manifest_sha256=digest(prepared / "input-manifest.json"),
        output=downloaded,
    )
    warehouse = tmp_path / "warehouse"
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": warehouse.as_uri(),
        "spark.sql.catalog.local.table-override.format-version": "2",
    }
    options = {
        "catalog_name": "local",
        "namespace": "transfer_test",
        "expected_settings": settings,
        "session_factory": lambda: create_session(
            tmp_path / "events" / uuid.uuid4().hex, warehouse=warehouse
        ),
    }
    first = run_writer(downloaded, run.summary_sha256, **options)
    assert first["status"] == "created" and first["rows"] == 2 and first["readback_matches"]
    assert run_writer(downloaded, run.summary_sha256, **options) == first | {"status": "reused"}
    assert SparkContext._active_spark_context is None
    assert transfer["get_requests"] == 4 and all(body.closed for body in client.bodies)
