"""Check proposed Glue JARs offline using a temporary local Iceberg catalog only."""

import argparse
import hashlib
import json
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from urllib.parse import unquote

from pyproj import Transformer
from sedona.spark import SedonaContext

from urban_geography.glue_runtime import JARS, runtime_plan
from urban_geography.lakehouse import _table_metadata
from urban_geography.spark import create_session

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def projection_rows(spark) -> list[dict]:
    """Measure two fixed points with Sedona and independent PROJ."""
    transform = Transformer.from_crs(4326, 3763, always_xy=True)
    rows = []
    for x, y in [(-8.425, 40.205), (-8.435, 40.195)]:
        row = spark.sql(
            "SELECT ST_X(p) AS x, ST_Y(p) AS y FROM "
            f"(SELECT ST_Transform(ST_GeomFromWKT('POINT({x} {y})'), "
            "'EPSG:4326', 'EPSG:3763') AS p)"
        ).first()
        assert row is not None
        expected = transform.transform(x, y)
        rows.append(
            {
                "lon_lat": [x, y],
                "sedona_xy": [row.x, row.y],
                "proj_xy": list(expected),
                "proj_delta_m": [row.x - expected[0], row.y - expected[1]],
            }
        )
    return rows


def projection_control(output: Path) -> None:
    """Separate process and JVM: the ordinary local runtime, with its three JARs."""
    if output.exists() or output.resolve().is_relative_to((PROJECT_ROOT / "data").resolve()):
        raise ValueError("Control output requires a fresh directory outside retained data")
    output.mkdir(parents=True)
    spark = create_session(output / "spark-events")
    try:
        rows = projection_rows(spark)
    finally:
        spark.stop()
    with (output / "projection.json").open("x", encoding="utf-8") as target:
        target.write(json.dumps(rows, indent=2) + "\n")


def check_runtime(aws_bundle: Path, output: Path) -> dict:
    """Load AWS classes without initialization; never resolve the configured Glue catalog."""
    if output.exists():
        raise ValueError("Runtime check output already exists; use a fresh directory")
    if output.resolve().is_relative_to((PROJECT_ROOT / "data").resolve()):
        raise ValueError("Runtime check output must be outside retained data")
    jars = {}
    for artifact, expected in JARS:
        name = artifact.rsplit("/", 1)[1]
        path = (
            aws_bundle
            if name == "iceberg-aws-bundle-1.10.2.jar"
            else Path("/opt/spark-jars") / name
        )
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Runtime JAR checksum differs: {name}")
        jars[name] = path.resolve()
    plan = runtime_plan(deployment_name="urban-geo-test", account_id="123456789012")
    output.mkdir(parents=True)
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--control-only",
            "--output",
            str(output / "control"),
        ],
        check=True,
    )
    control = json.loads((output / "control/projection.json").read_text(encoding="utf-8"))
    events = output / "spark-events"
    events.mkdir()
    warehouse = output / "warehouse"
    builder = (
        SedonaContext.builder()
        .master("local[2]")
        .appName("urban-geography-glue-runtime-check")
        .config("spark.jars", ",".join(str(path) for path in jars.values()))
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.dir", events.resolve().as_uri())
        .config("spark.sql.warehouse.dir", (output / "session-warehouse").resolve().as_uri())
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse.resolve().as_uri())
        .config("spark.sql.catalog.local.table-override.format-version", "2")
    )
    for key, value in plan["spark_settings"].items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        SedonaContext.create(spark)
        jvm = spark._jvm
        assert jvm is not None
        observed = {
            "spark": spark.version,
            "python": f"{sys.version_info.major}.{sys.version_info.minor}",
            "scala": jvm.scala.util.Properties.versionNumberString(),
            "java": jvm.java.lang.System.getProperty("java.specification.version"),
            "sedona": version("apache-sedona"),
            "iceberg": jvm.org.apache.iceberg.IcebergBuild.version(),
            "iceberg_format": 2,
        }
        if observed != plan["runtime"]:
            raise ValueError(f"Observed runtime versions differ: {observed}")
        assert all(spark.conf.get(key) == value for key, value in plan["spark_settings"].items())
        assert jvm.org.apache.iceberg.aws.AwsClientProperties.CLIENT_REGION == "client.region"
        assert jvm.org.apache.iceberg.aws.s3.S3FileIOProperties.SSE_TYPE == "s3.sse.type"

        classes = {}
        loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
        for name in (
            "org.apache.iceberg.aws.glue.GlueCatalog",
            "org.apache.iceberg.aws.s3.S3FileIO",
            "software.amazon.awssdk.services.glue.GlueClient",
            "software.amazon.awssdk.services.s3.S3Client",
        ):
            loaded = jvm.java.lang.Class.forName(name, False, loader)
            location = loaded.getProtectionDomain().getCodeSource().getLocation().toString()
            jar_name = unquote(location).rsplit("/", 1)[-1]
            assert jar_name in jars, (name, jar_name)
            classes[name] = jar_name

        projected = projection_rows(spark)
        # This is a library compatibility check, not an absolute datum-accuracy claim.
        assert projected == control, {"control": control, "candidate": projected}

        spark.sql("CREATE NAMESPACE local.runtime_check")
        table = "local.runtime_check.observations"
        spark.sql(
            "SELECT 'b1' AS id, CAST(NULL AS DOUBLE) AS distance_m, "
            "ST_AsBinary(ST_GeomFromWKT('POINT(-8.425 40.205)')) AS geometry_wkb, "
            "TIMESTAMP '2026-10-29 00:00:00.123' AS processed_at_utc"
        ).writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
        initial = _table_metadata(spark, table)
        assert initial["format-version"] == 2
        snapshot = initial["current-snapshot-id"]
        stored = spark.table(table).first()
        assert stored is not None and stored.distance_m is None
        assert stored.processed_at_utc.microsecond == 123000
        point = spark.table(table).selectExpr("ST_X(ST_GeomFromWKB(geometry_wkb)) AS x").first()
        assert point is not None and point.x == -8.425
        spark.sql(
            f"INSERT INTO {table} SELECT 'b2', distance_m, geometry_wkb, "
            f"processed_at_utc FROM {table}"
        )
        assert spark.table(table).count() == 2
        assert spark.read.option("snapshot-id", str(snapshot)).table(table).count() == 1
        updated = _table_metadata(spark, table)
        assert len(updated["snapshots"]) == 2
        assert updated["current-snapshot-id"] != snapshot
        report = {
            "status": "complete_offline_check",
            "runtime": observed,
            "jar_sha256": {
                name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in jars.items()
            },
            "aws_classes": classes,
            "proposed_settings_preserved": True,
            "projection_matches_fresh_local_control": True,
            "projection_measurements": projected,
            "wkb_null_and_millisecond_readback": True,
            "local_v2_snapshots_and_historical_readback": True,
            "glue_catalog_queried": False,
            "aws_api_or_upload_or_provisioning": False,
            "glue_service_runtime_validated": False,
        }
    finally:
        spark.stop()
    with (output / "report.json").open("x", encoding="utf-8") as target:
        target.write(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws-bundle", type=Path)
    parser.add_argument(
        "--control-only",
        action="store_true",
        help="Internal subprocess mode: ordinary local projection baseline",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.control_only:
        projection_control(args.output)
        return
    if args.aws_bundle is None:
        parser.error("--aws-bundle is required for the complete offline check")
    report = check_runtime(args.aws_bundle, args.output)
    print(json.dumps({"status": report["status"], "report": str(args.output / "report.json")}))


if __name__ == "__main__":
    main()
