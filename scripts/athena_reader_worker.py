"""Run one bounded readback in the existing Spark interpreter; no query submission."""

import argparse
import hashlib
import json
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from athena_control import canonical, write_once
from athena_read_plan import ARTIFACTS, ROOT, same

from urban_geography.glue_runtime import JARS, runtime_plan

MAX_RECORD_BYTES = 64 * 1024
BUNDLE = ROOT / ".private/tools/glue-runtime/iceberg-aws-bundle-1.10.2.jar"


def validate_request(request: dict[str, Any]) -> None:
    keys = {
        "contract_version",
        "local_fixture",
        "target",
        "input_path",
        "source_run_sha256",
        "writer_receipt",
        "writer_receipt_sha256",
        "catalog_name",
        "namespace",
        "expected_settings",
        "expected_location",
        "output",
    }
    if (
        set(request) != keys
        or type(request["contract_version"]) is not int
        or request["contract_version"] != 1
        or type(request["local_fixture"]) is not bool
    ):
        raise ValueError("Unsupported reader request")
    output = Path(request["output"])
    if (
        output.is_symlink()
        or not output.resolve().is_relative_to(ARTIFACTS.resolve())
        or output.resolve() == ARTIFACTS.resolve()
    ):
        raise ValueError("Reader output must stay under ignored artifacts")
    recipe = runtime_plan(**request["target"])
    if request["local_fixture"]:
        settings = request["expected_settings"]
        warehouse = urlsplit(settings.get("spark.sql.catalog.local.warehouse", ""))
        location = urlsplit(request["expected_location"])
        if (
            warehouse.scheme != "file"
            or warehouse.netloc
            or location.scheme != "file"
            or location.netloc
        ):
            raise ValueError("Local reader fixture requires local file storage")
        base = Path(unquote(warehouse.path)).resolve()
        expected = {
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": base.as_uri(),
            "spark.sql.catalog.local.table-override.format-version": "2",
        }
        if (
            request["catalog_name"] != "local"
            or not same(settings, expected)
            or not base.is_relative_to(ARTIFACTS.resolve())
            or Path(unquote(location.path)).resolve()
            != base / request["namespace"] / "building_proximity"
        ):
            raise ValueError("Local fixture catalog or warehouse differs")
    elif not same(
        {
            key: request[key]
            for key in ("catalog_name", "namespace", "expected_settings", "expected_location")
        },
        {
            "catalog_name": recipe["catalog"],
            "namespace": recipe["database"],
            "expected_settings": recipe["spark_settings"],
            "expected_location": recipe["warehouse"] + "building_proximity",
        },
    ):
        raise ValueError("Cloud reader target/settings differ from the fixed recipe")


def verified_jars() -> list[str]:
    paths = []
    for artifact, pin in JARS:
        name = artifact.rsplit("/", 1)[-1]
        path = BUNDLE if name == BUNDLE.name else Path("/opt/spark-jars") / name
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != pin:
                raise ValueError("Reader JAR checksum differs: " + name)
        paths.append(str(path))
    return paths


def start_session(request: dict[str, Any], observed: dict[str, Any]) -> Any:
    from pyspark import SparkContext
    from sedona.spark import SedonaContext

    if sys.version_info[:2] != (3, 11) or SparkContext._active_spark_context is not None:
        raise ValueError("Use a fresh pinned Python 3.11 Spark worker process")
    jars = verified_jars()
    output = Path(request["output"])
    events = output / "events"
    events.mkdir()
    builder = (
        SedonaContext.builder()
        .master("local[2]")
        .appName("urban-geography-readback")
        .config("spark.jars", ",".join(jars))
        .config("spark.driver.memory", "1g")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.dir", events.as_uri())
        .config("spark.sql.warehouse.dir", (output / "session-warehouse").as_uri())
    )
    for key, value in request["expected_settings"].items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        jvm = spark._jvm
        observed.update(
            {
                "python": "3.11",
                "spark": spark.version,
                "scala": jvm.scala.util.Properties.versionNumberString(),
                "java": jvm.java.lang.System.getProperty("java.specification.version"),
                "sedona": version("apache-sedona"),
                "iceberg": jvm.org.apache.iceberg.IcebergBuild.version(),
                "pyarrow": version("pyarrow"),
            }
        )
        if observed != {
            "python": "3.11",
            "spark": "3.5.6",
            "scala": "2.12.18",
            "java": "17",
            "sedona": "1.8.0",
            "iceberg": "1.10.2",
            "pyarrow": "19.0.1",
        }:
            raise ValueError("Reader runtime pins differ")
        return SedonaContext.create(spark)
    except BaseException:
        spark.stop()
        raise


def close_gateway() -> None:
    from pyspark import SparkContext

    gateway = SparkContext._gateway
    process = getattr(gateway, "proc", None)
    if process is not None:
        # This interpreter created its own gateway; reap it before reporting success.
        if process.stdin is not None:
            process.stdin.close()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def run(request_path: Path, pin: str) -> None:
    with request_path.open("rb") as stream:
        content = stream.read(MAX_RECORD_BYTES + 1)
    if len(content) > MAX_RECORD_BYTES or hashlib.sha256(content).hexdigest() != pin:
        raise ValueError("Reader request checksum/size differs")
    request = json.loads(content)
    validate_request(request)
    output = Path(request["output"])
    if output / "report.json" == request_path or (output / "report.json").exists():
        raise ValueError("Use a fresh reader report")
    from writer_readback import verify_writer_output

    observed: dict[str, Any] = {}
    try:
        result = verify_writer_output(
            Path(request["input_path"]),
            request["source_run_sha256"],
            Path(request["writer_receipt"]),
            request["writer_receipt_sha256"],
            catalog_name=request["catalog_name"],
            namespace=request["namespace"],
            expected_settings=request["expected_settings"],
            expected_location=request["expected_location"],
            session_factory=lambda: start_session(request, observed),
        )
    finally:
        close_gateway()
    write_once(
        output / "report.json",
        canonical({"request_sha256": pin, "runtime": observed, "readback": result}),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    args = parser.parse_args()
    run(args.request, args.request_sha256)


if __name__ == "__main__":
    main()
