"""Pinned local Spark session; no runtime Maven downloads."""

from pathlib import Path

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def create_session(event_dir: Path) -> SparkSession:
    jars = sorted(Path("/opt/sedona-jars").glob("*.jar"))
    if len(jars) != 2:
        raise RuntimeError("Build the Docker image to install the pinned Sedona and GeoTools JARs")
    event_dir.mkdir(parents=True, exist_ok=True)
    session = (
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
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return SedonaContext.create(session)
