"""Offline first-snapshot publication, replay and rejection tests with real Iceberg."""

import json
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from urban_geography.config import RunConfig
from urban_geography.ingest import digest
from urban_geography.lakehouse import FACT_TYPES, _table_metadata, load_completed_run, publish_facts
from urban_geography.spark import create_session

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    root = tmp_path_factory.mktemp("iceberg-publication")
    session = create_session(root / "events", warehouse=root / "warehouse")
    yield session
    session.stop()


@pytest.fixture
def processed_run(tmp_path):
    config = RunConfig("offline_iceberg", "2026-09-23.1", (-8.435, 40.195, -8.415, 40.215))
    root = tmp_path / "completed"
    facts_path = root / "building_proximity" / "part-00000.parquet"
    grid_path = root / "grid_proximity" / "part-00000.parquet"
    facts_path.parent.mkdir(parents=True)
    grid_path.parent.mkdir(parents=True)
    common = {
        "grid_id": "epsg3763_250m_0_0",
        "source_release": config.release,
        "area_id": config.area_id,
        "metric_version": "park_proximity_v1",
        "threshold_m": 500.0,
        "selection_version": "point_on_surface_bbox_v1",
        "boundary_id": None,
        "boundary_release": None,
        "boundary_geometry_sha256": None,
    }
    rows = [
        {
            **common,
            "building_id": "b1",
            "park_id": "park",
            "distance_m": 50.0,
            "within_threshold": True,
        },
        {
            **common,
            "building_id": "b2",
            "park_id": None,
            "distance_m": None,
            "within_threshold": False,
        },
    ]
    arrow_types = {"string": pa.string(), "double": pa.float64(), "boolean": pa.bool_()}
    schema = pa.schema([(name, arrow_types[kind]) for name, kind in FACT_TYPES.items()])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), facts_path)
    pq.write_table(
        pa.Table.from_pylist([{"mapped_buildings": 2, "buildings_within": 1}]), grid_path
    )
    summary = {
        "source_url": "https://example.invalid/synthetic-input",
        "attribution": "Informação geográfica cedida pela Direção-Geral do Território",
        "status": "complete",
        "config": json.loads(json.dumps(config.as_dict())),
        "metric_version": "park_proximity_v1",
        "mapped_buildings": 2,
        "candidate_parks": 1,
        "buildings_within_500m": 1,
        "share_within": 0.5,
        "selection": {
            "version": "point_on_surface_bbox_v1",
            "scope": "pilot_bbox",
            "bbox": list(config.bbox),
            "boundary": None,
        },
        "completed_at_utc": "2026-10-25T00:00:00+00:00",
        "files": {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*.parquet")},
    }
    (root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    return root


def file_hashes(root):
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


def test_publish_readback_exact_replay_and_refuse_another_run(spark, processed_run):
    run = load_completed_run(processed_run)
    before = file_hashes(processed_run)
    result = publish_facts(spark, run)
    assert result["status"] == "created"
    assert result["format_version"] == 2 and result["rows"] == 2
    assert result["readback_matches"] is True
    assert result["source_run_sha256"] == digest(processed_run / "summary.json")
    table = result["table"]
    actual = {row.building_id: row.asDict() for row in spark.table(table).collect()}
    expected = {
        row["building_id"]: row
        for row in pq.read_table(processed_run / "building_proximity").to_pylist()
    }
    assert actual == expected
    snapshot = spark.table(table + ".snapshots").first()
    assert snapshot is not None
    assert snapshot.snapshot_id == result["snapshot_id"]
    assert snapshot.summary["source-run-sha256"] == run.summary_sha256
    properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    # URL-bearing metadata is redacted by Spark's display command, but remains intact on disk.
    assert properties["source-run-summary"].endswith("(redacted)")
    entry = spark.table(table + ".metadata_log_entries").select("file").first()
    assert entry is not None
    metadata = json.loads(Path(unquote(urlparse(entry.file).path)).read_text(encoding="utf-8"))
    assert json.loads(metadata["properties"]["source-run-summary"]) == run.summary
    assert properties["format-version"] == "2"
    assert publish_facts(spark, run) == {**result, "status": "reused"}
    assert spark.table(table + ".snapshots").count() == 1
    assert file_hashes(processed_run) == before

    summary_path = processed_run / "summary.json"
    changed = json.loads(summary_path.read_text())
    changed["completed_at_utc"] = "2026-10-25T01:00:00+00:00"
    summary_path.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="already contains a different run"):
        publish_facts(spark, load_completed_run(processed_run))
    assert spark.table(table + ".snapshots").count() == 1
    assert {row.building_id: row.asDict() for row in spark.table(table).collect()} == expected


@pytest.mark.parametrize("defect", ["duplicates", "metadata", "schema"])
def test_invalid_facts_fail_before_table_creation(spark, processed_run, defect):
    path = processed_run / "building_proximity" / "part-00000.parquet"
    original = pq.read_table(path)
    rows = original.to_pylist()
    if defect == "duplicates":
        rows[1]["building_id"] = rows[0]["building_id"]
        rewritten = pa.Table.from_pylist(rows, schema=original.schema)
        message = "Duplicate processing building IDs"
    elif defect == "metadata":
        rows[1]["area_id"] = "wrong_area"
        rewritten = pa.Table.from_pylist(rows, schema=original.schema)
        message = "disagree with metric or source metadata"
    else:
        rewritten = original.drop(["grid_id"])
        message = "fact schema differs"
    pq.write_table(rewritten, path)
    summary_path = processed_run / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary["files"][path.relative_to(processed_run).as_posix()] = digest(path)
    summary_path.write_text(json.dumps(summary))
    with pytest.raises(ValueError, match=message):
        publish_facts(spark, load_completed_run(processed_run), table_name="invalid_" + defect)
    assert not spark.catalog.tableExists("local.analytics.invalid_" + defect)


def test_corrupt_processed_file_is_rejected(processed_run):
    (processed_run / "building_proximity" / "part-00000.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="file checksum differs"):
        load_completed_run(processed_run)


@pytest.mark.parametrize("defect", ["extra", "escape"])
def test_inventory_changes_and_path_escape_are_rejected(processed_run, defect):
    if defect == "extra":
        pq.write_table(
            pa.Table.from_pylist([{"id": "unexpected"}]), processed_run / "extra.parquet"
        )
        message = "file inventory differs"
    else:
        summary_path = processed_run / "summary.json"
        summary = json.loads(summary_path.read_text())
        summary["files"]["../outside.parquet"] = "0" * 64
        summary_path.write_text(json.dumps(summary))
        message = "path escapes"
    with pytest.raises(ValueError, match=message):
        load_completed_run(processed_run)


def test_replay_detects_modified_iceberg_rows_without_writing(spark, processed_run):
    run = load_completed_run(processed_run)
    result = publish_facts(spark, run, table_name="modified")
    table = result["table"]
    spark.read.parquet(str(processed_run / "building_proximity")).select(*FACT_TYPES).limit(
        1
    ).writeTo(table).append()
    assert spark.table(table + ".snapshots").count() == 2
    with pytest.raises(ValueError, match="readback differs"):
        publish_facts(spark, run, table_name="modified")
    assert spark.table(table + ".snapshots").count() == 2
    assert spark.table(table).count() == 3


def test_explicit_catalog_and_namespace_isolate_publication_and_replay(
    spark, processed_run, tmp_path
):
    run = load_completed_run(processed_run)
    input_hashes = file_hashes(processed_run)
    control = publish_facts(spark, run, table_name="configured_target")
    control_metadata = _table_metadata(spark, control["table"])

    warehouse = tmp_path / "separate-warehouse"
    prefix = "spark.sql.catalog.prepared_m4"
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(prefix + ".type", "hadoop")
    spark.conf.set(prefix + ".warehouse", warehouse.as_uri())
    options = {
        "catalog_name": "prepared_m4",
        "namespace": "urban_geo_test_m4",
        "table_name": "configured_target",
    }
    result = publish_facts(spark, run, **options)
    table = "prepared_m4.urban_geo_test_m4.configured_target"
    assert result == {
        "status": "created",
        "table": table,
        "format_version": 2,
        "snapshot_id": _table_metadata(spark, table)["current-snapshot-id"],
        "rows": 2,
        "source_run_sha256": run.summary_sha256,
        "readback_matches": True,
    }
    metadata = _table_metadata(spark, table)
    assert metadata["location"] == warehouse.as_uri() + "/urban_geo_test_m4/configured_target"
    assert metadata["format-version"] == 2
    assert metadata["properties"]["source-run-sha256"] == run.summary_sha256
    expected = pq.read_table(processed_run / "building_proximity").to_pylist()
    assert {row.building_id: row.asDict() for row in spark.table(table).collect()} == {
        row["building_id"]: row for row in expected
    }
    assert publish_facts(spark, run, **options) == {**result, "status": "reused"}
    assert spark.table(table + ".snapshots").count() == 1

    summary_path = processed_run / "summary.json"
    original_summary = summary_path.read_bytes()
    changed = json.loads(original_summary)
    changed["completed_at_utc"] = "2026-10-29T00:00:00Z"
    summary_path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError, match="already contains a different run"):
        publish_facts(spark, load_completed_run(processed_run), **options)
    assert spark.table(table + ".snapshots").count() == 1
    summary_path.write_bytes(original_summary)

    # Detect external changes on the selected target without writing another snapshot.
    spark.read.parquet(str(processed_run / "building_proximity")).select(*FACT_TYPES).limit(
        1
    ).writeTo(table).append()
    with pytest.raises(ValueError, match="readback differs"):
        publish_facts(spark, run, **options)
    assert spark.table(table + ".snapshots").count() == 2
    assert _table_metadata(spark, control["table"]) == control_metadata
    assert spark.table(control["table"]).count() == 2
    assert file_hashes(processed_run) == input_hashes
