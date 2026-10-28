"""Pinned local Spark session; no runtime Maven downloads."""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def execution_settings(profile: str) -> dict[str, str]:
    """Only known local execution profiles; no arbitrary external master URI."""
    if profile == "local":
        return {"spark.master": "local[2]"}
    if profile != "two_executor":
        raise ValueError("Unknown Spark execution profile")
    return {
        "spark.master": "spark://master:7077",
        "spark.driver.host": "driver",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.port": "7090",
        "spark.blockManager.port": "7091",
        "spark.cores.max": "2",
        "spark.executor.cores": "1",
        "spark.executor.memory": "768m",
        "spark.dynamicAllocation.enabled": "false",
        "spark.task.maxFailures": "1",
        "spark.scheduler.minRegisteredResourcesRatio": "1.0",
        "spark.scheduler.maxRegisteredResourcesWaitingTime": "45s",
        "spark.rpc.askTimeout": "30s",
        "spark.rpc.lookupTimeout": "30s",
        "spark.network.timeout": "60s",
    }


def executor_ids(spark: SparkSession) -> list[str]:
    ids = spark.sparkContext._jsc.sc().getExecutorIds()
    return sorted(str(ids.apply(index)) for index in range(ids.size()))


def wait_for_executors(spark: SparkSession, timeout_seconds: float = 45) -> list[str]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        ids = executor_ids(spark)
        if len(ids) == 2:
            return ids
        if len(ids) > 2 or time.monotonic() >= deadline:
            raise RuntimeError("Expected exactly two registered executors within the deadline")
        time.sleep(0.25)


def create_session(
    event_dir: Path, *, warehouse: Path | None = None, execution_profile: str = "local"
) -> SparkSession:
    settings = execution_settings(execution_profile)
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
    for key, value in settings.items():
        builder = builder.config(key, value)
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
    try:
        if execution_profile == "two_executor":
            wait_for_executors(session)
        return SedonaContext.create(session)
    except Exception:
        session.stop()
        raise
