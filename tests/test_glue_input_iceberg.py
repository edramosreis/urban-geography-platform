"""The staged input contract feeds real local Iceberg publication and exact replay."""

import pytest

from urban_geography.glue_input import inspect_input, stage_input
from urban_geography.ingest import digest
from urban_geography.lakehouse import publish_facts

pytestmark = pytest.mark.integration


def test_staged_input_publishes_and_replays_in_a_separate_catalog(
    history_run_factory, history_spark, tmp_path
):
    source = history_run_factory()
    staged = tmp_path / "staged"
    stage_input(source.path, source.summary_sha256, staged)
    run = inspect_input(staged, source.summary_sha256).run
    before = {str(p): digest(p) for p in staged.rglob("*") if p.is_file()}
    prefix = "spark.sql.catalog.staged_m4"
    history_spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    history_spark.conf.set(prefix + ".type", "hadoop")
    history_spark.conf.set(prefix + ".warehouse", (tmp_path / "warehouse").as_uri())
    options = {"catalog_name": "staged_m4", "namespace": "input_test"}
    first = publish_facts(history_spark, run, **options)
    assert first["status"] == "created" and first["rows"] == 2
    assert first["table"] == "staged_m4.input_test.building_proximity"
    assert first["readback_matches"]
    assert publish_facts(history_spark, run, **options) == {**first, "status": "reused"}
    assert history_spark.table(first["table"] + ".snapshots").count() == 1
    assert {str(p): digest(p) for p in staged.rglob("*") if p.is_file()} == before
