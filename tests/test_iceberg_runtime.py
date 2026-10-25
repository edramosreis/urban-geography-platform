"""Real offline Iceberg/Sedona interoperability and persistent local catalog checks."""

import pytest
from pyspark.sql import functions as F

from urban_geography.spark import create_session

pytestmark = pytest.mark.integration


def test_local_v2_catalog_snapshots_wkb_and_session_reopen(tmp_path):
    warehouse = tmp_path / "warehouse"
    table = "local.smoke.observations"
    spark = create_session(tmp_path / "first-events", warehouse=warehouse)
    try:
        spark.sql("CREATE NAMESPACE local.smoke")
        first = spark.createDataFrame(
            [("b1", "2026-09-23.1", None)], "id string, source_release string, distance_m double"
        ).withColumn("geometry_wkb", F.expr("ST_AsBinary(ST_GeomFromWKT('POINT(-8.425 40.205)'))"))
        expected_row = first.select("geometry_wkb").first()
        assert expected_row is not None
        expected_wkb = bytes(expected_row.geometry_wkb)
        first.writeTo(table).using("iceberg").partitionedBy("source_release").create()
        properties = {
            row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        assert properties["format-version"] == "2"
        snapshot = spark.table(f"{table}.snapshots").first()
        assert snapshot is not None
        first_snapshot = snapshot.snapshot_id
        stored = spark.table(table).first()
        assert stored is not None
        assert stored.distance_m is None
        assert bytes(stored.geometry_wkb) == expected_wkb
        spatial = (
            spark.table(table).selectExpr("ST_X(ST_GeomFromWKB(geometry_wkb)) AS longitude").first()
        )
        assert spatial is not None
        assert spatial.longitude == -8.425

        second = first.withColumn("id", F.lit("b2")).withColumn(
            "source_release", F.lit("synthetic-next")
        )
        second.writeTo(table).append()
        assert spark.table(table).count() == 2
        assert spark.table(f"{table}.snapshots").count() == 2
        historic = spark.read.option("snapshot-id", str(first_snapshot)).table(table).collect()
        assert len(historic) == 1 and historic[0].id == "b1"
    finally:
        spark.stop()

    reopened = create_session(tmp_path / "reopened-events", warehouse=warehouse)
    try:
        assert {row.id for row in reopened.table(table).collect()} == {"b1", "b2"}
        historic = reopened.read.option("snapshot-id", str(first_snapshot)).table(table).collect()
        assert len(historic) == 1 and historic[0].id == "b1"
    finally:
        reopened.stop()
