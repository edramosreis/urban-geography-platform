"""Spatial SQL semantics shared by the pilot and its independent correctness tests."""

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from urban_geography.config import RunConfig

METRIC_VERSION = "park_proximity_v1"


def geometry_frame(frame: DataFrame, config: RunConfig) -> DataFrame:
    decoded = frame.select("id", F.expr("ST_GeomFromWKB(geometry)").alias("wgs84"))
    total = decoded.count()
    if not total or decoded.filter("id IS NULL OR id = ''").count():
        raise ValueError("Empty input or null/blank feature IDs")
    if decoded.select("id").distinct().count() != total:
        raise ValueError("Duplicate source IDs")
    bad = decoded.filter("""
        wgs84 IS NULL OR ST_IsEmpty(wgs84) OR NOT ST_IsValid(wgs84)
        OR ST_GeometryType(wgs84) NOT IN ('ST_Polygon', 'ST_MultiPolygon')
        OR NOT (ST_XMin(wgs84) >= -180 AND ST_XMax(wgs84) <= 180
                AND ST_YMin(wgs84) >= -90 AND ST_YMax(wgs84) <= 90)
    """).count()
    if bad:
        raise ValueError(f"Invalid geometry records: {bad} of {total}; publication blocked")
    return decoded.select(
        "id", F.expr(f"ST_Transform(wgs84, 'EPSG:4326', '{config.metric_crs}')").alias("geom")
    )


def select_buildings(
    projected: DataFrame, config: RunConfig, boundary_wkb: bytes | None = None
) -> DataFrame:
    """Select full-footprint representative points in the pilot and optional CRS84 boundary."""
    west, south, east, north = config.bbox
    polygon = (
        f"POLYGON(({west} {south},{east} {south},{east} {north},{west} {north},{west} {south}))"
    )
    points = projected.select("id", F.expr("ST_PointOnSurface(geom)").alias("point"))
    selected = points.withColumn(
        "selection_point", F.expr(f"ST_Transform(point, '{config.metric_crs}', 'EPSG:4326')")
    ).filter(F.expr(f"ST_Covers(ST_GeomFromWKT('{polygon}'), selection_point)"))
    if boundary_wkb is not None:
        # Keep the pilot window even when the retained municipality is much larger.
        selected = selected.withColumn("boundary_wkb", F.lit(boundary_wkb)).filter(
            F.expr("ST_Covers(ST_GeomFromWKB(boundary_wkb), selection_point)")
        )
    return selected.select("id", "point")


def proximity(buildings: DataFrame, parks: DataFrame, threshold_m: float) -> DataFrame:
    # Straightforward bounded pilot baseline. Do not use the cross join at large scale.
    pairs = (
        buildings.alias("b")
        .crossJoin(parks.alias("p"))
        .select(
            F.col("b.id").alias("building_id"),
            F.col("p.id").alias("park_id"),
            F.expr("ST_Distance(b.point, p.geom)").alias("distance_m"),
        )
        .filter(F.col("distance_m") <= threshold_m)
    )
    return _proximity_facts(buildings, pairs)


def _proximity_facts(buildings: DataFrame, pairs: DataFrame) -> DataFrame:
    """Shared exact ranking, unmatched-building preservation and grid assignment."""
    rank = Window.partitionBy("building_id").orderBy("distance_m", "park_id")
    nearest = pairs.withColumn("rank", F.row_number().over(rank)).filter("rank = 1").drop("rank")
    coordinates = buildings.select(
        F.col("id").alias("building_id"),
        F.expr("ST_X(point)").alias("x_m"),
        F.expr("ST_Y(point)").alias("y_m"),
    )
    return (
        coordinates.join(nearest, "building_id", "left")
        .withColumn("within_threshold", F.col("park_id").isNotNull())
        .withColumn(
            "grid_id",
            F.concat_ws(
                "_",
                F.lit("epsg3763_250m"),
                F.floor(F.col("x_m") / 250),
                F.floor(F.col("y_m") / 250),
            ),
        )
        .drop("x_m", "y_m")
    )


# Conservative metre padding broadens only the candidate screen, never the exact threshold.
ENVELOPE_PADDING_M = 1e-6


def envelope_candidates(buildings: DataFrame, parks: DataFrame, threshold_m: float) -> DataFrame:
    """Logical broad phase for projected points and full polygon geometries."""
    if not math.isfinite(threshold_m) or threshold_m < 0:
        raise ValueError("Envelope threshold must be finite and nonnegative")
    reach = threshold_m + ENVELOPE_PADDING_M
    points = buildings.select(
        F.col("id").alias("building_id"),
        "point",
        F.expr("ST_X(point)").alias("point_x"),
        F.expr("ST_Y(point)").alias("point_y"),
    )
    bounds = parks.select(
        F.col("id").alias("park_id"),
        F.col("geom").alias("park_geom"),
        F.expr("ST_XMin(geom)").alias("min_x"),
        F.expr("ST_XMax(geom)").alias("max_x"),
        F.expr("ST_YMin(geom)").alias("min_y"),
        F.expr("ST_YMax(geom)").alias("max_y"),
    )
    screen = (
        (F.col("point_x") >= F.col("min_x") - reach)
        & (F.col("point_x") <= F.col("max_x") + reach)
        & (F.col("point_y") >= F.col("min_y") - reach)
        & (F.col("point_y") <= F.col("max_y") + reach)
    )
    # Catalyst chooses physical predicate ordering; this is not a distance-call counter.
    return points.join(bounds, screen, "inner").select(
        "building_id", "park_id", "point", "park_geom"
    )


def proximity_envelope(buildings: DataFrame, parks: DataFrame, threshold_m: float) -> DataFrame:
    """Conservative envelope candidates followed by the unchanged exact distance test."""
    pairs = (
        envelope_candidates(buildings, parks, threshold_m)
        .select(
            "building_id",
            "park_id",
            F.expr("ST_Distance(point, park_geom)").alias("distance_m"),
        )
        .filter(F.col("distance_m") <= threshold_m)
    )
    return _proximity_facts(buildings, pairs)


def distance_join_pairs(buildings: DataFrame, parks: DataFrame, threshold_m: float) -> DataFrame:
    """Express the exact distance condition as an inner join, with no manual envelope screen."""
    if not math.isfinite(threshold_m) or threshold_m < 0:
        raise ValueError("Distance join threshold must be finite and nonnegative")
    points = buildings.select(F.col("id").alias("building_id"), "point")
    polygons = parks.select(F.col("id").alias("park_id"), F.col("geom").alias("park_geom"))
    return points.join(
        polygons, F.expr("ST_Distance(point, park_geom)") <= threshold_m, "inner"
    ).select("building_id", "park_id", F.expr("ST_Distance(point, park_geom)").alias("distance_m"))


def proximity_distance_join(
    buildings: DataFrame, parks: DataFrame, threshold_m: float
) -> DataFrame:
    """Let the ordinary optimizer plan the exact spatial join and preserve all buildings."""
    return _proximity_facts(buildings, distance_join_pairs(buildings, parks, threshold_m))
