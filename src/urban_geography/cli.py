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

from urban_geography.boundary import RetainedBoundary, load_boundary_spec, load_retained_boundary
from urban_geography.config import RunConfig, load_config
from urban_geography.ingest import config_key, digest, extract, validate_raw
from urban_geography.spark import create_session
from urban_geography.transform import (
    METRIC_VERSION,
    geometry_frame,
    proximity_distance_join,
    select_buildings,
)


def process(
    config: RunConfig, raw: Path, output: Path, *, boundary: RetainedBoundary | None = None
) -> Path:
    manifest = validate_raw(raw, config)
    if output.exists():
        raise ValueError("Output already exists; use a new run path (raw inputs are reusable)")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.parent / f".run-{uuid.uuid4().hex}"
    staging.mkdir()
    selection_version = (
        "point_on_surface_bbox_boundary_v1" if boundary else "point_on_surface_bbox_v1"
    )
    boundary_columns = {
        "boundary_id": boundary.spec.area_id if boundary else None,
        "boundary_release": boundary.spec.source_release if boundary else None,
        "boundary_geometry_sha256": boundary.spec.expected_geometry_sha256 if boundary else None,
    }
    started = time.perf_counter()
    spark = create_session(staging / "spark-events")
    try:
        buildings = select_buildings(
            geometry_frame(spark.read.parquet(str(raw / "building.parquet")), config),
            config,
            boundary.geometry_wkb if boundary else None,
        ).cache()
        parks = geometry_frame(spark.read.parquet(str(raw / "land_use.parquet")), config).cache()
        building_count, park_count = buildings.count(), parks.count()
        if not building_count:
            raise ValueError("No building representative points in the analysis area")
        if building_count * park_count > 2_000_000:
            raise ValueError(
                "Pilot logical pair budget exceeds 2 million pairs; review the workload limits"
            )
        facts = (
            proximity_distance_join(buildings, parks, config.threshold_m)
            .withColumn("source_release", F.lit(config.release))
            .withColumn("area_id", F.lit(config.area_id))
            .withColumn("metric_version", F.lit(METRIC_VERSION))
            .withColumn("threshold_m", F.lit(config.threshold_m))
            .withColumn("selection_version", F.lit(selection_version))
            .cache()
        )
        for name, value in boundary_columns.items():
            facts = facts.withColumn(name, F.lit(value).cast("string"))
        with (staging / "plan.txt").open("w", encoding="utf-8") as plan_file:
            with redirect_stdout(plan_file):
                facts.explain(mode="extended")
        actual = facts.count()
        matched = facts.filter("within_threshold").count()
        if actual != building_count:
            raise ValueError("Output cardinality differs from selected buildings")
        facts.write.mode("errorifexists").parquet(str(staging / "building_proximity"))
        grid = (
            facts.groupBy(
                "source_release",
                "area_id",
                "metric_version",
                "selection_version",
                *boundary_columns,
                "grid_id",
            )
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
            "processing_implementation": "exact_distance_join_v1",
            "selection": {
                "version": selection_version,
                "scope": "pilot_bbox_intersection_boundary" if boundary else "pilot_bbox",
                "bbox": list(config.bbox),
                "boundary": boundary.as_dict() if boundary else None,
            },
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
    parser.add_argument(
        "--boundary-config",
        type=Path,
        help="Optionally intersect the pilot with an already retained, pinned municipality",
    )
    parser.add_argument("--boundary-root", type=Path, default=Path("data/boundaries"))
    args = parser.parse_args()
    if args.boundary_config and args.command == "extract":
        parser.error("--boundary-config applies to process or run, not extraction")
    config = load_config(args.config)
    boundary = (
        load_retained_boundary(load_boundary_spec(args.boundary_config), args.boundary_root)
        if args.boundary_config
        else None
    )
    if args.command in {"extract", "run"}:
        raw = extract(config, args.raw_root)
    else:
        raw = args.raw_root / config.release / config.area_id / config_key(config)
    result = (
        raw if args.command == "extract" else process(config, raw, args.output, boundary=boundary)
    )
    print(json.dumps({"status": "complete", "path": str(result)}))


if __name__ == "__main__":
    main()
