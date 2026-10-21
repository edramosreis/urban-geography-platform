from pathlib import Path

import pytest
from pyproj import Transformer
from pyspark.sql import functions as F
from shapely.geometry import Point, Polygon, box
from shapely.ops import transform

from urban_geography.config import load_config
from urban_geography.spark import create_session
from urban_geography.transform import geometry_frame, proximity, select_buildings

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = create_session(tmp_path_factory.mktemp("events"))
    yield session
    session.stop()


def projected_frames(spark, points, polygons):
    buildings = spark.createDataFrame(
        [(key, value.wkt) for key, value in points.items()], "id string, wkt string"
    ).select("id", F.expr("ST_GeomFromWKT(wkt)").alias("point"))
    parks = spark.createDataFrame(
        [(key, value.wkt) for key, value in polygons.items()], "id string, wkt string"
    ).select("id", F.expr("ST_GeomFromWKT(wkt)").alias("geom"))
    return buildings, parks


def test_exact_threshold_inside_hole_ties_and_unmatched(spark):
    points = {
        "inside": Point(50, 50),
        "edge": Point(600, 50),
        "outside": Point(600.001, 50),
        "hole": Point(50, 250),
        "far": Point(5000, 5000),
    }
    parks = {
        "a": box(0, 0, 100, 100),
        "b": box(0, 0, 100, 100),
        "holepark": Polygon(
            [(0, 200), (100, 200), (100, 300), (0, 300)],
            [[(25, 225), (75, 225), (75, 275), (25, 275)]],
        ),
    }
    buildings, polygons = projected_frames(spark, points, parks)
    actual = {row.building_id: row for row in proximity(buildings, polygons, 500).collect()}
    for key, point in points.items():
        candidates = sorted(
            (point.distance(polygon), park_id)
            for park_id, polygon in parks.items()
            if point.distance(polygon) <= 500
        )
        assert actual[key].within_threshold == bool(candidates)
        if candidates:
            assert actual[key].distance_m == pytest.approx(candidates[0][0], abs=1e-6)
            assert actual[key].park_id == candidates[0][1]
        else:
            assert actual[key].distance_m is None
            assert actual[key].park_id is None
    assert actual["edge"].distance_m == 500
    assert actual["inside"].park_id == "a"
    assert actual["hole"].distance_m == 25


def test_empty_parks_preserves_buildings(spark):
    buildings, parks = projected_frames(spark, {"b": Point(0, 0)}, {})
    result = proximity(buildings, parks, 500).collect()
    assert len(result) == 1 and not result[0].within_threshold


def test_projection_and_membership_match_independent_proj(spark):
    config = load_config(Path("configs/coimbra_pilot.json"))
    polygon = box(-8.43, 40.20, -8.429, 40.201)
    raw = spark.createDataFrame([("b", bytes(polygon.wkb))], "id string, geometry binary")
    transformed = geometry_frame(raw, config)
    area = transformed.selectExpr("ST_Area(geom) AS area").first().area
    reference = transform(Transformer.from_crs(4326, 3763, always_xy=True).transform, polygon)
    assert area == pytest.approx(reference.area, rel=1e-6)
    assert select_buildings(transformed, config).count() == 1


def test_duplicate_and_invalid_geometries_block_publication(spark):
    config = load_config(Path("configs/coimbra_pilot.json"))
    good = bytes(box(-8.43, 40.20, -8.429, 40.201).wkb)
    raw = spark.createDataFrame([("b", good), ("b", good)], "id string, geometry binary")
    with pytest.raises(ValueError, match="Duplicate"):
        geometry_frame(raw, config)
    raw = spark.createDataFrame(
        [("b", bytes(Point(-8.43, 40.2).wkb))], "id string, geometry binary"
    )
    with pytest.raises(ValueError, match="Invalid geometry"):
        geometry_frame(raw, config)


def test_park_outside_area_still_matches_inside_building(spark):
    buildings, parks = projected_frames(
        spark, {"inside": Point(10, 10)}, {"outside": box(-100, 0, -10, 100)}
    )
    result = proximity(buildings, parks, 500).first()
    assert result.park_id == "outside"
    assert result.distance_m == 20
