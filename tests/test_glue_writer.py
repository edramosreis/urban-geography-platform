"""Writer preflight ordering, runtime guards and cleanup without starting Java."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, sentinel

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import urban_geography.glue_writer as writer
from urban_geography.glue_input import stage_input
from urban_geography.ingest import digest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def prepared(history_run_factory, tmp_path):
    run = history_run_factory()
    path = tmp_path / "staged"
    stage_input(run.path, run.summary_sha256, path)
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.warehouse": (tmp_path / "warehouse").as_uri(),
        "spark.sql.catalog.local.table-override.format-version": "2",
        "spark.sql.catalog.local.type": "hadoop",
    }
    return path, run.summary_sha256, settings


def session(settings):
    return SimpleNamespace(
        version="3.5.6",
        conf=SimpleNamespace(get=lambda key, default: settings.get(key, default)),
        stop=Mock(),
        createDataFrame=Mock(return_value=sentinel.frame),
    )


def execute(prepared, factory, **changes):
    path, pin, settings = prepared
    options = {
        "catalog_name": "local",
        "namespace": "writer_test",
        "expected_settings": settings,
        "session_factory": factory,
    }
    options.update(changes)
    return writer.run_writer(path, pin, **options)


def test_writer_reports_verified_input_and_target_and_stops_its_session(prepared, monkeypatch):
    spark = session(prepared[2])
    calls = []

    def publish(observed, run, **target):
        calls.append((observed, run.path, target))
        return {"status": "created", "snapshot_id": 17, "source_run_sha256": run.summary_sha256}

    monkeypatch.setattr(writer, "publish_facts", publish)
    result = execute(prepared, lambda: spark)
    assert calls == [
        (
            spark,
            prepared[0],
            {
                "catalog_name": "local",
                "namespace": "writer_test",
                "table_name": "building_proximity",
                "source_facts": sentinel.frame,
            },
        )
    ]
    assert result["input_files"] == 3 and result["input_payload_bytes"] > 0
    assert len(result["input_manifest_sha256"]) == 64
    assert result["verified_spark_settings"] == prepared[2]
    assert result["status"] == "created"
    rows, schema = spark.createDataFrame.call_args.args
    assert len(rows) == 2 and set(schema.fieldNames()) == set(rows[0])
    assert next(row for row in rows if not row["within_threshold"])["distance_m"] is None
    spark.stop.assert_called_once()


@pytest.mark.parametrize("defect", ["target", "identity", "manifest"])
def test_invalid_preflight_never_starts_a_session(prepared, defect):
    path, pin, settings = prepared
    options = {}
    if defect == "target":
        options["namespace"] = "bad.namespace"
    elif defect == "identity":
        pin = "0" * 64
    else:
        (path / "input-manifest.json").unlink()

    def forbidden():
        pytest.fail("Invalid input/target must fail before Spark startup")

    with pytest.raises(ValueError):
        execute((path, pin, settings), forbidden, **options)


@pytest.mark.parametrize("defect", ["empty", "timezone"])
def test_session_contract_must_be_explicit_before_startup(prepared, defect):
    settings = (
        {} if defect == "empty" else prepared[2] | {"spark.sql.session.timeZone": "Europe/Lisbon"}
    )
    with pytest.raises(ValueError, match="explicit UTC"):
        execute(
            prepared,
            lambda: pytest.fail("No startup for a bad session contract"),
            expected_settings=settings,
        )


@pytest.mark.parametrize("defect", ["version", "timezone", "warehouse"])
def test_wrong_runtime_never_publishes_and_is_stopped(prepared, monkeypatch, defect):
    values = dict(prepared[2])
    if defect == "timezone":
        values["spark.sql.session.timeZone"] = "Europe/Lisbon"
    elif defect == "warehouse":
        values["spark.sql.catalog.local.warehouse"] = "file:///wrong-warehouse"
    spark = session(values)
    if defect == "version":
        spark.version = "unexpected"
    monkeypatch.setattr(writer, "publish_facts", lambda *a, **k: pytest.fail("No publication"))
    with pytest.raises(ValueError, match="Spark"):
        execute(prepared, lambda: spark)
    spark.stop.assert_called_once()


def test_publication_failure_propagates_and_stops_the_session(prepared, monkeypatch):
    spark = session(prepared[2])

    def failed(*args, **kwargs):
        raise ValueError("synthetic publication failure")

    monkeypatch.setattr(writer, "publish_facts", failed)
    with pytest.raises(ValueError, match="synthetic publication failure"):
        execute(prepared, lambda: spark)
    spark.stop.assert_called_once()


@pytest.mark.parametrize("when", ["startup", "publication"])
def test_payload_drift_aborts_before_success_and_stops(prepared, monkeypatch, when):
    path = prepared[0]
    fact = next((path / "building_proximity").glob("*.parquet"))
    spark = session(prepared[2])

    def start():
        if when == "startup":
            fact.write_bytes(b"changed")
        return spark

    def publish(*args, **kwargs):
        assert when == "publication", "Startup drift must fail before publication"
        fact.write_bytes(b"changed")
        return {"status": "created"}

    monkeypatch.setattr(writer, "publish_facts", publish)
    with pytest.raises(ValueError, match="checksum differs"):
        execute(prepared, start)
    spark.stop.assert_called_once()


def test_managed_entry_point_rejects_bad_input_before_a_java_runtime(prepared):
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/run_glue_writer.py"),
            "--input",
            str(prepared[0]),
            "--summary-sha256",
            "0" * 64,
            "--deployment-name",
            "urban-geo-test",
            "--account-id",
            "123456789012",
            "--JOB_NAME",
            "synthetic-job",
            "--JOB_RUN_ID",
            "synthetic-run",
            "--enable-metrics",
            "true",
        ],
        cwd=ROOT,
        env=os.environ | {"JAVA_HOME": "/missing-java"},
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 and "checksum differs" in result.stderr
    assert "JAVA_GATEWAY_EXITED" not in result.stderr


def test_driver_reader_refuses_schema_coercion_and_stops(
    history_run_factory, tmp_path, monkeypatch
):
    run = history_run_factory()
    fact = next((run.path / "building_proximity").glob("*.parquet"))
    table = pq.read_table(fact)
    index = table.schema.get_field_index("distance_m")
    pq.write_table(
        table.set_column(index, "distance_m", pa.array([50, None], type=pa.int64())), fact
    )
    summary_path = run.path / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary["files"][fact.relative_to(run.path).as_posix()] = digest(fact)
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    output = tmp_path / "staged"
    pin = digest(summary_path)
    stage_input(run.path, pin, output)
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.warehouse": (tmp_path / "warehouse").as_uri(),
        "spark.sql.catalog.local.table-override.format-version": "2",
    }
    spark = session(settings)
    monkeypatch.setattr(writer, "publish_facts", lambda *a, **k: pytest.fail("No publication"))
    with pytest.raises(ValueError, match="Arrow fact schema"):
        execute((output, pin, settings), lambda: spark)
    spark.stop.assert_called_once()
