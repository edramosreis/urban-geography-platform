"""Whole S3 bootstrap with real local Iceberg; the object client is in memory."""

import uuid

import pytest
from pyspark import SparkContext
from s3_input_fixtures import MemoryS3

from urban_geography.glue_input import stage_input
from urban_geography.glue_s3_bootstrap import run_s3_writer
from urban_geography.glue_writer import run_writer
from urban_geography.ingest import digest
from urban_geography.spark import create_session

pytestmark = pytest.mark.integration


def test_bootstrap_downloads_writes_replays_and_cleans_owned_resources(
    history_run_factory, tmp_path
):
    run = history_run_factory()
    prepared = tmp_path / "prepared"
    stage_input(run.path, run.summary_sha256, prepared)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    warehouse = tmp_path / "warehouse"
    clients, inputs = [], []
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": warehouse.as_uri(),
        "spark.sql.catalog.local.table-override.format-version": "2",
    }

    def factory():
        client = MemoryS3(prepared, run.summary_sha256)
        clients.append(client)
        return client

    def write(path):
        assert clients[-1].closed
        inputs.append(path)
        return run_writer(
            path,
            run.summary_sha256,
            catalog_name="local",
            namespace="bootstrap_test",
            expected_settings=settings,
            session_factory=lambda: create_session(
                tmp_path / "events" / uuid.uuid4().hex, warehouse=warehouse
            ),
        )

    options = {
        "deployment_name": "urban-geo-test",
        "account_id": "123456789012",
        "expected_summary_sha256": run.summary_sha256,
        "expected_manifest_sha256": digest(prepared / "input-manifest.json"),
        "scratch_root": scratch,
        "client_factory": factory,
        "write": write,
    }
    first = run_s3_writer(**options)
    replay = run_s3_writer(**options)
    assert first["publication"]["status"] == "created"
    assert first["publication"]["rows"] == 2 and first["publication"]["readback_matches"]
    assert replay["publication"] == first["publication"] | {"status": "reused"}
    assert first["scratch_cleaned"] and replay["scratch_cleaned"]
    assert all(client.closed and all(body.closed for body in client.bodies) for client in clients)
    assert all(not path.exists() for path in inputs) and not list(scratch.iterdir())
    assert SparkContext._active_spark_context is None
