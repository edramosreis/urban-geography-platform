"""Run the immutable fact publisher from prepared input and an explicit session contract."""

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from urban_geography.glue_input import InputPlan, load_prepared_input
from urban_geography.ingest import digest
from urban_geography.lakehouse import FACT_TYPES, publish_facts


def run_writer(
    input_path: Path,
    expected_summary_sha256: str,
    *,
    catalog_name: str,
    namespace: str,
    expected_settings: dict[str, str],
    session_factory: Callable[[], SparkSession],
    table_name: str = "building_proximity",
) -> dict[str, Any]:
    """Preflight input, own one Spark session, publish/read back, and always stop it."""
    input_path = input_path.resolve()
    for name, value in (
        ("catalog_name", catalog_name),
        ("namespace", namespace),
        ("table_name", table_name),
    ):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
            raise ValueError(f"Writer {name} must be a safe lowercase identifier")
    settings = dict(expected_settings)
    prefix = "spark.sql.catalog." + catalog_name
    required = {
        "spark.sql.session.timeZone": "UTC",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        prefix + ".table-override.format-version": "2",
    }
    if any(settings.get(key) != value for key, value in required.items()) or not settings.get(
        prefix + ".warehouse"
    ):
        raise ValueError("Writer requires explicit UTC, Iceberg v2 catalog and warehouse settings")
    prepared = load_prepared_input(input_path, expected_summary_sha256)
    spark = session_factory()
    try:
        if spark.version != "3.5.6":
            raise ValueError("Writer requires the pinned Spark 3.5.6 runtime")
        mismatches = [key for key, value in settings.items() if spark.conf.get(key, "") != value]
        if mismatches:
            raise ValueError("Writer Spark settings differ: " + ", ".join(sorted(mismatches)))
        # Startup can take time: verify again before any catalog lookup or write.
        current = load_prepared_input(input_path, expected_summary_sha256)
        if current.files != prepared.files or current.payload_bytes != prepared.payload_bytes:
            raise ValueError("Writer input changed after preflight")
        result = publish_facts(
            spark,
            current.run,
            catalog_name=catalog_name,
            namespace=namespace,
            table_name=table_name,
            source_facts=_driver_facts(spark, current),
        )
        after = load_prepared_input(input_path, expected_summary_sha256)
        if after.files != prepared.files or after.payload_bytes != prepared.payload_bytes:
            raise ValueError("Writer input changed during publication")
        return result | {
            "input_files": len(prepared.files),
            "input_payload_bytes": prepared.payload_bytes,
            "input_manifest_sha256": digest(input_path / "input-manifest.json"),
            "verified_spark_settings": settings,
        }
    finally:
        spark.stop()


def _driver_facts(spark: SparkSession, prepared: InputPlan) -> DataFrame:
    """Distribute bounded pilot rows, so remote executors need no driver-local files."""
    arrow_types = {"string": pa.string(), "double": pa.float64(), "boolean": pa.bool_()}
    expected = {name: arrow_types[kind] for name, kind in FACT_TYPES.items()}
    rows = []
    for name in sorted(prepared.files):
        if name.startswith("building_proximity/"):
            table = pq.ParquetFile(prepared.run.path / name).read()
            if (
                len(table.schema) != len(expected)
                or {field.name: field.type for field in table.schema} != expected
            ):
                raise ValueError("Driver Arrow fact schema differs from the supported contract")
            rows.extend(table.to_pylist())
            if len(rows) > prepared.run.summary["mapped_buildings"]:
                raise ValueError("Driver fact rows exceed the verified count")
    if len(rows) != prepared.run.summary["mapped_buildings"]:
        raise ValueError("Driver fact rows differ from the verified count")
    spark_types = {"string": StringType, "double": DoubleType, "boolean": BooleanType}
    schema = StructType(
        [StructField(name, spark_types[kind]()) for name, kind in FACT_TYPES.items()]
    )
    return spark.createDataFrame(rows, schema)
