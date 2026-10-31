"""Real local Iceberg readback and rejection, with all catalog files unchanged by reading."""

import json
import uuid

import pytest
from pyspark import SparkContext
from pyspark.sql import functions as F
from writer_readback_fixtures import READBACK as reader
from writer_readback_fixtures import make_case

from urban_geography.glue_writer import _driver_facts, run_writer
from urban_geography.ingest import digest
from urban_geography.lakehouse import publish_facts
from urban_geography.spark import create_session

pytestmark = pytest.mark.integration


def hashes(root):
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("scenario", ["correct", "different_rows", "wrong_property"])
def test_real_snapshot_readback_is_exact_and_read_only(history_run_factory, tmp_path, scenario):
    case = make_case(history_run_factory, tmp_path)

    def start():
        return create_session(tmp_path / "events" / uuid.uuid4().hex, warehouse=case.warehouse)

    if scenario == "different_rows":
        spark = start()
        try:
            changed = _driver_facts(spark, case.plan).withColumn(
                "distance_m", F.when(F.col("within_threshold"), F.col("distance_m") + 1.0)
            )
            claim = publish_facts(
                spark, case.plan.run, namespace="readback_test", source_facts=changed
            )
            case.claim.update(claim)
        finally:
            spark.stop()
    else:
        case.claim = run_writer(
            case.path,
            case.run.summary_sha256,
            catalog_name="local",
            namespace="readback_test",
            expected_settings=case.settings,
            session_factory=start,
        )
        if scenario == "wrong_property":
            spark = start()
            try:
                spark.sql(
                    f"ALTER TABLE {case.table} SET TBLPROPERTIES ('source-run-sha256'='{'b' * 64}')"
                )
            finally:
                spark.stop()
    case.receipt.write_text(json.dumps(case.claim), encoding="utf-8")
    before = hashes(case.warehouse)
    kwargs = dict(
        catalog_name="local",
        namespace="readback_test",
        expected_settings=case.settings,
        expected_location=case.location,
        session_factory=start,
    )
    if scenario == "correct":
        result = reader.verify_writer_output(
            case.path, case.run.summary_sha256, case.receipt, digest(case.receipt), **kwargs
        )
        assert result["binding"] == {
            "source_schema": "readback_test",
            "snapshot_id": case.claim["snapshot_id"],
            "source_run_sha256": case.run.summary_sha256,
        }
        assert result["readback_matches"] and result["metadata_stable"] and result["rows"] == 2
        assert (
            result["athena_verified"] is False and result["writer_job_identity_verified"] is False
        )
    else:
        with pytest.raises(
            ValueError,
            match="snapshot rows" if scenario == "different_rows" else "metadata differs",
        ):
            reader.verify_writer_output(
                case.path, case.run.summary_sha256, case.receipt, digest(case.receipt), **kwargs
            )
    assert SparkContext._active_spark_context is None
    assert hashes(case.warehouse) == before
