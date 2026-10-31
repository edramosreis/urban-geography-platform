"""Demonstrate writer-to-snapshot-to-dbt binding on fresh local Iceberg only."""

import argparse
import json
import uuid
from pathlib import Path
from typing import Any

from prepare_dbt_project import ARTIFACTS, prepare_project
from pyspark.sql import SparkSession
from writer_readback import verify_writer_output

from urban_geography.glue_input import load_prepared_input
from urban_geography.glue_writer import run_writer
from urban_geography.ingest import digest
from urban_geography.spark import create_session


def inventory(root: Path) -> dict[str, str]:
    return {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}


def save(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as target:
        target.write(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def check_local(input_path: Path, pin: str, output: Path) -> dict[str, Any]:
    if output.is_symlink():
        raise ValueError("Local-check output must not be a symlink")
    input_path, output = input_path.resolve(), output.resolve()
    if (
        output.exists()
        or output == ARTIFACTS.resolve()
        or not output.is_relative_to(ARTIFACTS.resolve())
        or output.is_relative_to(input_path)
        or input_path.is_relative_to(output)
    ):
        raise ValueError("Use fresh local-check output under artifacts, separate from the input")
    prepared = load_prepared_input(input_path, pin)
    output.mkdir(parents=True)
    warehouse = output / "warehouse"
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": warehouse.as_uri(),
        "spark.sql.catalog.local.table-override.format-version": "2",
    }
    namespace = "readback_demo"

    def start() -> SparkSession:
        return create_session(output / "events" / uuid.uuid4().hex, warehouse=warehouse)

    publication = run_writer(
        input_path,
        pin,
        catalog_name="local",
        namespace=namespace,
        expected_settings=settings,
        session_factory=start,
    )
    writer_path = output / "writer.json"
    save(writer_path, publication)
    before = inventory(warehouse)
    verified = verify_writer_output(
        input_path,
        pin,
        writer_path,
        digest(writer_path),
        catalog_name="local",
        namespace=namespace,
        expected_settings=settings,
        expected_location=(warehouse / namespace / "building_proximity").as_uri(),
        session_factory=start,
    )
    if inventory(warehouse) != before:
        raise ValueError("Local reader changed the Iceberg warehouse")
    readback_path = output / "readback.json"
    save(readback_path, verified)
    # Bind the fixture project directly to the observed snapshot, with no hand-copied ID.
    project = output / "dbt-project"
    preparation = prepare_project(project, **verified["binding"])
    if preparation["binding"] != verified["binding"]:
        raise ValueError("Prepared dbt binding differs from verified readback")
    after = load_prepared_input(input_path, pin)
    if after.files != prepared.files or after.payload_bytes != prepared.payload_bytes:
        raise ValueError("Retained input changed during the local demonstration")
    report = {
        "scope": "Local HadoopCatalog fixture only; the resulting binding is not an AWS table",
        "writer_receipt_sha256": digest(writer_path),
        "readback_sha256": digest(readback_path),
        "dbt_preparation_sha256": digest(project / "preparation.json"),
        "binding": verified["binding"],
        "rows": verified["rows"],
        "warehouse_files_unchanged_by_reader": len(before),
        "dbt_binding_matches_readback": True,
        "live_aws_binding_verified": False,
        "athena_queries_executed": 0,
    }
    save(output / "check.json", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--summary-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check_local(args.input, args.summary_sha256, args.output), indent=2))


if __name__ == "__main__":
    main()
