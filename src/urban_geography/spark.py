"""Pinned local Spark session; no runtime Maven downloads."""

from pathlib import Path

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def create_session(event_dir: Path, *, warehouse: Path | None = None) -> SparkSession:
    jars = sorted(Path("/opt/spark-jars").glob("*.jar"))
    expected = {
        "sedona-spark-shaded-3.5_2.12-1.8.0.jar",
        "geotools-wrapper-1.8.0-33.1.jar",
        "iceberg-spark-runtime-3.5_2.12-1.10.2.jar",
    }
    if {jar.name for jar in jars} != expected:
        raise RuntimeError("Rebuild the Docker image to install the pinned Spark runtime JARs")
    event_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SedonaContext.builder()
        .master("local[2]")
        .appName("urban-geography-pilot")
        .config("spark.jars", ",".join(str(jar) for jar in jars))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.dir", event_dir.resolve().as_uri())
    )
    if warehouse is not None:
        builder = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.resolve().as_uri())
            .config("spark.sql.catalog.local.table-override.format-version", "2")
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return SedonaContext.create(session)
