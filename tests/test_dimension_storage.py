"""Real Iceberg acceptance for append-only dimension registries."""

import pytest
from pyspark.sql import functions as F

from urban_geography.dimension_storage import (
    prepare_dimension_table,
    read_dimension_table,
    write_dimension_table,
)
from urban_geography.dimensions import build_dimension_contract, dimension_frames

pytestmark = pytest.mark.integration


def test_dimension_registry_create_append_replay_and_conflict(history_spark, history_run_factory):
    spark = history_spark
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    a, _ = dimension_frames(spark, build_dimension_contract([first]))
    both, _ = dimension_frames(spark, build_dimension_contract([first, second]))
    table = "local.analytics.registry_releases"
    initial = write_dimension_table(
        spark, prepare_dimension_table(spark, a, table=table, kind="releases"), "0" * 64
    )
    appended = write_dimension_table(
        spark, prepare_dimension_table(spark, both, table=table, kind="releases"), "1" * 64
    )
    assert initial["status"] == "created" and appended["status"] == "appended"
    assert read_dimension_table(spark, table, "releases").count() == 2
    assert read_dimension_table(spark, table, "releases", initial["snapshot_id"]).count() == 1
    replay = write_dimension_table(
        spark, prepare_dimension_table(spark, both, table=table, kind="releases"), "1" * 64
    )
    assert replay["status"] == "reused" and replay["snapshot_id"] == appended["snapshot_id"]
    assert spark.table(table + ".snapshots").count() == 2
    with pytest.raises(ValueError, match="conflicting attributes"):
        prepare_dimension_table(
            spark, both.withColumn("source_name", F.lit("changed")), table=table, kind="releases"
        )
    assert spark.table(table + ".snapshots").count() == 2


@pytest.mark.parametrize("defect", ["duplicate", "schema", "stale", "wrong_kind"])
def test_dimension_registry_guards(history_spark, history_run_factory, defect):
    spark = history_spark
    release, _ = dimension_frames(spark, build_dimension_contract([history_run_factory()]))
    table = "local.analytics.registry_guard_" + defect
    if defect == "duplicate":
        with pytest.raises(ValueError, match="Duplicate dimension key"):
            prepare_dimension_table(
                spark, release.unionByName(release), table=table, kind="releases"
            )
    elif defect == "schema":
        with pytest.raises(ValueError, match="schema differs"):
            prepare_dimension_table(
                spark, release.drop("source_name"), table=table, kind="releases"
            )
    else:
        prepared = prepare_dimension_table(spark, release, table=table, kind="releases")
        write_dimension_table(spark, prepared, "0" * 64)
        if defect == "stale":
            with pytest.raises(ValueError, match="changed after preflight"):
                write_dimension_table(spark, prepared, "0" * 64)
        else:
            with pytest.raises(ValueError, match="not a supported"):
                read_dimension_table(spark, table, "study_areas")
