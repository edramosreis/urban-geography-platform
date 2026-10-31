"""Shared synthetic receipt/metadata contracts, without a JVM or network."""

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from urban_geography.glue_input import load_prepared_input, stage_input
from urban_geography.ingest import digest
from urban_geography.lakehouse import FACT_TYPES

ROOT = Path(__file__).resolve().parents[1]
for name in ("prepare_dbt_project", "writer_readback"):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
READBACK = sys.modules["writer_readback"]


def settings(warehouse):
    return {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": warehouse.resolve().as_uri(),
        "spark.sql.catalog.local.table-override.format-version": "2",
    }


def make_case(history_run_factory, tmp_path):
    run = history_run_factory()
    prepared_path = tmp_path / "prepared"
    stage_input(run.path, run.summary_sha256, prepared_path)
    plan = load_prepared_input(prepared_path, run.summary_sha256)
    warehouse = tmp_path / "warehouse"
    conf = settings(warehouse)
    location = (warehouse / "readback_test" / "building_proximity").resolve().as_uri()
    table = "local.readback_test.building_proximity"
    claim = {
        "status": "created",
        "table": table,
        "format_version": 2,
        "snapshot_id": 123,
        "rows": 2,
        "source_run_sha256": run.summary_sha256,
        "readback_matches": True,
        "input_files": len(plan.files),
        "input_payload_bytes": plan.payload_bytes,
        "input_manifest_sha256": digest(prepared_path / "input-manifest.json"),
        "verified_spark_settings": conf,
    }
    receipt = tmp_path / "writer.json"
    receipt.write_text(json.dumps(claim), encoding="utf-8")
    metadata = {
        "format-version": 2,
        "table-uuid": "b6309a11-cc67-422d-8789-374651d36e67",
        "location": location,
        "current-snapshot-id": 123,
        "current-schema-id": 0,
        "schemas": [
            {"schema-id": 0, "fields": [{"name": k, "type": v} for k, v in FACT_TYPES.items()]}
        ],
        "properties": {
            "source-run-sha256": run.summary_sha256,
            "source-run-summary": json.dumps(run.summary, sort_keys=True),
        },
        "snapshots": [
            {
                "snapshot-id": 123,
                "summary": {
                    "source-run-sha256": run.summary_sha256,
                    "operation": "append",
                    "total-records": "2",
                    "total-data-files": "1",
                    "total-files-size": "2048",
                    "total-delete-files": "0",
                },
            }
        ],
    }
    return SimpleNamespace(
        run=run,
        path=prepared_path,
        plan=plan,
        warehouse=warehouse,
        settings=conf,
        location=location,
        table=table,
        claim=claim,
        receipt=receipt,
        pin=digest(receipt),
        metadata=metadata,
    )
