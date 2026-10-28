"""Bounded descriptions of retained geometry and logical candidate workloads."""

import json
import math
import statistics
import time
from collections import Counter
from collections.abc import Sequence
from importlib.metadata import version
from typing import Any

import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from urban_geography.benchmark import BaselinePlan
from urban_geography.demo import _hash_paths
from urban_geography.spark import create_session
from urban_geography.transform import envelope_candidates, geometry_frame, select_buildings
from urban_geography.workload_limits import PILOT_LIMITS, WorkloadLimits


def distribution(values: Sequence[int]) -> dict[str, Any]:
    """Describe observed nonnegative counts; an empty population has no quantiles."""
    if any(not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in values):
        raise ValueError("Profile values must be nonnegative integer counts")
    ordered = sorted(values)
    return {
        "count": len(values),
        "total": sum(values),
        "min": min(values) if values else None,
        "median": statistics.median(values) if values else None,
        "p95_nearest_rank": ordered[math.ceil(0.95 * len(values)) - 1] if values else None,
        "max": max(values) if values else None,
        "mean": statistics.mean(values) if values else None,
    }


def geometry_profile(frame: DataFrame) -> dict[str, Any]:
    rows = frame.selectExpr(
        "ST_NPoints(geom) AS vertices",
        "ST_NumGeometries(geom) AS parts",
        "ST_NRings(geom) AS rings",
        "length(ST_AsBinary(geom)) AS projected_wkb_bytes",
    ).collect()
    return {
        name: distribution([row[name] for row in rows])
        for name in (
            "vertices",
            "parts",
            "rings",
            "projected_wkb_bytes",
        )
    }


def input_partition_rows(frame: DataFrame) -> dict[str, Any]:
    partitions = frame.rdd.getNumPartitions()
    counts = {
        row.partition_id: row["count"]
        for row in frame.select(F.spark_partition_id().alias("partition_id"))
        .groupBy("partition_id")
        .count()
        .collect()
    }
    rows = [counts.get(index, 0) for index in range(partitions)]
    return {"partitions": partitions, "row_counts": rows, "distribution": distribution(rows)}


def profile_frames(
    footprints: DataFrame,
    points: DataFrame,
    parks: DataFrame,
    threshold_m: float,
    *,
    limits: WorkloadLimits = PILOT_LIMITS,
) -> dict[str, Any]:
    """Describe validated projected frames; never collect the candidate pair relation."""
    raw_count, building_count, park_count = footprints.count(), points.count(), parks.count()
    if (
        not 0 <= building_count <= raw_count <= limits.max_features
        or not 0 <= park_count <= limits.max_features
    ):
        raise ValueError("Profile rows exceed the retained pilot limit or selection differs")
    if building_count * park_count > limits.max_pairs:
        raise ValueError("Profile exceeds the pilot logical pair limit")
    selected = footprints.join(points.select("id"), "id", "left_semi")
    buildings = geometry_profile(selected)
    if buildings["vertices"]["count"] != building_count:
        raise ValueError("Selected point IDs differ from retained footprint IDs")
    park_geometry = geometry_profile(parks)
    candidates = (
        envelope_candidates(points, parks, threshold_m).select("building_id", "park_id").cache()
    )
    try:
        candidate_count = candidates.count()
        building_degrees = (
            points.select(F.col("id").alias("building_id"))
            .join(candidates.groupBy("building_id").count(), "building_id", "left")
            .fillna(0, ["count"])
        )
        park_degrees = (
            parks.select(F.col("id").alias("park_id"))
            .join(candidates.groupBy("park_id").count(), "park_id", "left")
            .fillna(0, ["count"])
        )
        per_building = [row["count"] for row in building_degrees.select("count").collect()]
        per_park = [row["count"] for row in park_degrees.select("count").collect()]
        if sum(per_building) != candidate_count or sum(per_park) != candidate_count:
            raise ValueError("Profile candidate degree totals differ")
    finally:
        candidates.unpersist()
    possible = building_count * park_count
    return {
        "raw_buildings": raw_count,
        "selected_buildings": building_count,
        "parks": park_count,
        "selected_building_footprints": buildings,
        "full_park_polygons": park_geometry,
        "logical_candidates": {
            "all_pairs_upper_bound": possible,
            "envelope_candidate_rows": candidate_count,
            "retained_share": candidate_count / possible if possible else None,
            "parks_per_building": distribution(per_building),
            "buildings_per_park": distribution(per_park),
            "buildings_with_zero_candidates": per_building.count(0),
            "parks_with_zero_candidates": per_park.count(0),
            "actual_distance_evaluations": None,
        },
        "input_partitions": {
            "selected_points": input_partition_rows(points),
            "parks": input_partition_rows(parks),
        },
    }


def run_profile(plan: BaselinePlan) -> dict[str, Any]:
    """Write a fresh profile only after source/selection/hash checks pass."""
    plan.output.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    spark = create_session(plan.output / "spark-events")
    try:
        footprints = geometry_frame(
            spark.read.parquet(str(plan.raw / "building.parquet")), plan.config
        ).cache()
        points = select_buildings(
            footprints, plan.config, plan.boundary.geometry_wkb if plan.boundary else None
        ).cache()
        parks = geometry_frame(
            spark.read.parquet(str(plan.raw / "land_use.parquet")), plan.config
        ).cache()
        if {row.id for row in points.select("id").collect()} != set(plan.expected):
            raise ValueError("Profile selection differs from the completed reference")
        profile = profile_frames(
            footprints, points, parks, plan.config.threshold_m, limits=plan.config.limits
        )
        settings = {
            "master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "note": "input partition rows only; not runtime shuffle/task skew",
        }
    finally:
        spark.stop()
    if _hash_paths(plan.inputs) != plan.hashes:
        raise ValueError("Profile inputs changed; no complete report written")
    occupied_grids = Counter(row["grid_id"] for row in plan.expected.values())
    report = {
        "status": "complete",
        "scope": "retained workload description; no speedup or scaling conclusion",
        "profile": profile,
        "occupied_grid_buildings": distribution(list(occupied_grids.values())),
        "reference_matches": sum(row["within_threshold"] for row in plan.expected.values()),
        "reference_summary_sha256": plan.reference.summary_sha256,
        "config": plan.config.as_dict(),
        "workload_limits": plan.config.limits.as_dict(),
        "selection": plan.reference.summary["selection"],
        "raw_parquet": {
            kind: {
                "bytes": (plan.raw / f"{kind}.parquet").stat().st_size,
                "rows": pq.read_metadata(plan.raw / f"{kind}.parquet").num_rows,
            }
            for kind in ("building", "land_use")
        },
        "input_hashes": plan.hashes,
        "inputs_unchanged": True,
        "versions": {name: version(name) for name in ("pyspark", "apache-sedona", "pyarrow")},
        "spark_settings": settings,
        "profile_elapsed_seconds": time.perf_counter() - started,
        "metric_notes": {
            "vertices": "coordinate positions including ring closure, not distinct vertices",
            "projected_wkb_bytes": "uncompressed binary geometry size, not executor heap usage",
            "candidates": "logical envelope join rows; not actual distance calls or exact matches",
            "grids": "occupied logical 250m cells; not Spark execution partitions",
        },
    }
    (plan.output / "profile.json").write_text(
        json.dumps(report, indent=2, allow_nan=False), encoding="utf-8"
    )
    return report
