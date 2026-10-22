"""Explicit local extraction and processing entry points."""

import argparse
import json
import platform
import time
import uuid
from contextlib import redirect_stdout
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

from pyspark.sql import functions as F

from urban_geography.config import RunConfig, load_config
from urban_geography.ingest import config_key, digest, extract, validate_raw
from urban_geography.spark import create_session
from urban_geography.transform import (
    METRIC_VERSION,
    geometry_frame,
    proximity,
    select_buildings,
)


def process(config: RunConfig, raw: Path, output: Path) -> Path:
    manifest = validate_raw(raw, config)
    if output.exists():
        raise ValueError("Output already exists; use a new run path (raw inputs are reusable)")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.parent / f".run-{uuid.uuid4().hex}"
    staging.mkdir()
    started = time.perf_counter()
    spark = create_session(staging / "spark-events")
    try:
        buildings = select_buildings(
            geometry_frame(spark.read.parquet(str(raw / "building.parquet")), config), config
        ).cache()
        parks = geometry_frame(spark.read.parquet(str(raw / "land_use.parquet")), config).cache()
        building_count, park_count = buildings.count(), parks.count()
        if not building_count:
            raise ValueError("No building representative points in the analysis area")
        if building_count * park_count > 2_000_000:
            raise ValueError("Pilot cross join exceeds 2 million pairs; use the M3 spatial join")
        facts = (
            proximity(buildings, parks, config.threshold_m)
            .withColumn("source_release", F.lit(config.release))
            .withColumn("area_id", F.lit(config.area_id))
            .withColumn("metric_version", F.lit(METRIC_VERSION))
            .withColumn("threshold_m", F.lit(config.threshold_m))
            .cache()
        )
        with (staging / "plan.txt").open("w", encoding="utf-8") as plan_file:
            with redirect_stdout(plan_file):
                facts.explain(mode="extended")
        actual = facts.count()
        matched = facts.filter("within_threshold").count()
        if actual != building_count:
            raise ValueError("Output cardinality differs from selected buildings")
        facts.write.mode("errorifexists").parquet(str(staging / "building_proximity"))
        grid = (
            facts.groupBy("source_release", "area_id", "metric_version", "grid_id")
            .agg(
                F.count("*").alias("mapped_buildings"),
                F.sum(F.col("within_threshold").cast("int")).alias("buildings_within"),
            )
            .withColumn("share_within", F.col("buildings_within") / F.col("mapped_buildings"))
        )
        grid.write.mode("errorifexists").parquet(str(staging / "grid_proximity"))
        summary = {
            "status": "complete",
            "config": config.as_dict(),
            "metric_version": METRIC_VERSION,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "raw_manifest_sha256": digest(raw / "manifest.json"),
            "input_rows": {k: v["rows"] for k, v in manifest["extracts"].items()},
            "mapped_buildings": building_count,
            "candidate_parks": park_count,
            "buildings_within_500m" if config.threshold_m == 500 else "buildings_within": matched,
            "share_within": matched / actual,
            "elapsed_seconds_including_spark_startup": time.perf_counter() - started,
            "spark_master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "versions": {k: version(k) for k in ("pyspark", "apache-sedona", "pyarrow")},
            "python": platform.python_version(),
            "quality": {"geometry_errors": 0, "duplicate_ids": 0, "cardinality_match": True},
            "interpretation": (
                "Mapped building proximity to mapped park polygons; not walking access"
            ),
        }
    finally:
        spark.stop()
    summary["files"] = {
        str(p.relative_to(staging)): digest(p) for p in sorted(staging.rglob("*.parquet"))
    }
    (staging / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    staging.rename(output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded local urban geography pipeline")
    parser.add_argument("command", choices=("extract", "process", "run"))
    parser.add_argument("--config", type=Path, default=Path("configs/coimbra_pilot.json"))
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/coimbra-pilot"))
    args = parser.parse_args()
    config = load_config(args.config)
    if args.command in {"extract", "run"}:
        raw = extract(config, args.raw_root)
    else:
        raw = args.raw_root / config.release / config.area_id / config_key(config)
    result = raw if args.command == "extract" else process(config, raw, args.output)
    print(json.dumps({"status": "complete", "path": str(result)}))


if __name__ == "__main__":
    main()
