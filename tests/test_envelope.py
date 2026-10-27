"""Exact envelope screening checked against all-pairs and an independent geometry oracle."""

import math

import pytest
from pyspark.sql import functions as F
from shapely.geometry import MultiPolygon, Point, Polygon, box

from urban_geography.spark import create_session
from urban_geography.transform import envelope_candidates, proximity, proximity_envelope


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = create_session(tmp_path_factory.mktemp("envelope-events"))
    yield session
    session.stop()


def frames(spark, points, polygons):
    buildings = spark.createDataFrame(
        [(key, point.wkt) for key, point in points.items()], "id string, wkt string"
    ).select("id", F.expr("ST_GeomFromWKT(wkt)").alias("point"))
    parks = spark.createDataFrame(
        [(key, polygon.wkt) for key, polygon in polygons.items()], "id string, wkt string"
    ).select("id", F.expr("ST_GeomFromWKT(wkt)").alias("geom"))
    return buildings, parks


@pytest.mark.integration
@pytest.mark.parametrize("threshold", [0, 25, 500])
def test_screen_retains_every_qualifying_pair_and_exact_results(spark, threshold):
    points = {
        "inside": Point(50, 50),
        "edge": Point(600, 50),
        "just_inside": Point(600 - 1e-7, 50),
        "just_outside": Point(600 + 1e-7, 50),
        "corner": Point(599, 599),
        "hole": Point(50, 250),
        "negative": Point(-510, -25),
        "part": Point(1505, 5),
        "concave": Point(300, 300),
        "far": Point(10000, 10000),
    }
    polygons = {
        "a": box(0, 0, 100, 100),
        "b": box(0, 0, 100, 100),
        "holed": Polygon(
            [(0, 200), (100, 200), (100, 300), (0, 300)],
            [[(25, 225), (75, 225), (75, 275), (25, 275)]],
        ),
        "multi": MultiPolygon([box(-1000, -50, -900, 50), box(1500, 0, 1510, 10)]),
        "concave": Polygon(
            [(200, 200), (400, 200), (400, 220), (220, 220), (220, 400), (200, 400)]
        ),
        # The full park lies outside the selected points; it must remain a candidate.
        "outside": box(-50, 0, -10, 100),
    }
    buildings, parks = frames(spark, points, polygons)
    candidates = envelope_candidates(buildings, parks, threshold)
    candidate_ids = {(r.building_id, r.park_id) for r in candidates.collect()}
    exact = candidates.select(
        "building_id", "park_id", F.expr("ST_Distance(point, park_geom)").alias("distance_m")
    ).filter(F.col("distance_m") <= threshold)
    actual_pairs = {(r.building_id, r.park_id): r.distance_m for r in exact.collect()}
    oracle = {
        (building_id, park_id): point.distance(polygon)
        for building_id, point in points.items()
        for park_id, polygon in polygons.items()
        if point.distance(polygon) <= threshold
    }
    assert set(oracle) <= candidate_ids
    assert actual_pairs == pytest.approx(oracle, abs=1e-6)
    baseline = {r.building_id: r.asDict() for r in proximity(buildings, parks, threshold).collect()}
    screened = {
        r.building_id: r.asDict() for r in proximity_envelope(buildings, parks, threshold).collect()
    }
    assert set(screened) == set(points) and screened == baseline
    for key, row in screened.items():
        eligible = sorted(
            (distance, park) for (building, park), distance in oracle.items() if building == key
        )
        assert row["park_id"] == (eligible[0][1] if eligible else None)
        assert row["within_threshold"] == bool(eligible)
        assert row["distance_m"] == (pytest.approx(eligible[0][0], abs=1e-6) if eligible else None)
        assert row["grid_id"] == (
            f"epsg3763_250m_{math.floor(points[key].x / 250)}_{math.floor(points[key].y / 250)}"
        )
    if threshold == 500:
        assert ("corner", "a") in candidate_ids and ("corner", "a") not in actual_pairs
        assert ("just_outside", "a") in candidate_ids and ("just_outside", "a") not in actual_pairs
        assert actual_pairs[("edge", "a")] == 500
        assert screened["inside"]["park_id"] == "a"
        assert len(candidate_ids) < len(points) * len(polygons)


@pytest.mark.integration
@pytest.mark.parametrize("empty", ["buildings", "parks"])
def test_empty_inputs_preserve_the_baseline_contract(spark, empty):
    buildings, parks = frames(
        spark,
        {} if empty == "buildings" else {"b": Point(0, 0)},
        {} if empty == "parks" else {"p": box(0, 0, 1, 1)},
    )
    actual = proximity_envelope(buildings, parks, 500).collect()
    assert actual == proximity(buildings, parks, 500).collect()
    assert len(actual) == (0 if empty == "buildings" else 1)
    if actual:
        assert actual[0].distance_m is None and not actual[0].within_threshold


@pytest.mark.parametrize("threshold", [-1, float("nan"), float("inf")])
def test_invalid_screen_threshold_fails_without_spark(threshold):
    with pytest.raises(ValueError, match="finite and nonnegative"):
        envelope_candidates(None, None, threshold)
