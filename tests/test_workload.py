"""Bounded workload distributions, real geometry profiling and retained-input CLI acceptance."""

import json
import subprocess
import sys

import pytest
from processing_fixtures import make_processing_case
from pyspark.sql import functions as F
from shapely.geometry import MultiPolygon, Point, Polygon, box

from urban_geography.ingest import digest
from urban_geography.spark import create_session
from urban_geography.workload import distribution, profile_frames


def test_distributions_distinguish_empty_populations_from_observed_zero_counts():
    assert distribution([0, 0, 1, 3, 6]) == {
        "count": 5,
        "total": 10,
        "min": 0,
        "median": 1,
        "p95_nearest_rank": 6,
        "max": 6,
        "mean": 2,
    }
    assert distribution([0, 0])["mean"] == 0
    empty = distribution([])
    assert empty["count"] == 0 and empty["total"] == 0
    assert all(empty[key] is None for key in ("min", "median", "p95_nearest_rank", "max", "mean"))


@pytest.mark.parametrize("invalid", [-1, 1.5, None, True])
def test_invalid_observation_counts_are_rejected(invalid):
    with pytest.raises(ValueError, match="nonnegative integer"):
        distribution([invalid])


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = create_session(tmp_path_factory.mktemp("workload-events"))
    yield session
    session.stop()


def frame(spark, geometries, name):
    return spark.createDataFrame(
        [(key, geom.wkt) for key, geom in geometries.items()], "id string, wkt string"
    ).select("id", F.expr("ST_GeomFromWKT(wkt)").alias(name))


@pytest.mark.integration
def test_full_geometry_complexity_zero_candidate_degrees_and_empty_input_partitions(spark):
    footprints = frame(
        spark,
        {
            "near": box(-1, -1, 1, 1),
            "far": box(99, -1, 101, 1),
            "unselected": box(999, -1, 1001, 1),
        },
        "geom",
    )
    points = (
        frame(spark, {"near": Point(0, 0), "far": Point(100, 0)}, "point").repartition(4).cache()
    )
    polygons = {
        "near": box(-1, -1, 1, 1),
        "unused": MultiPolygon(
            [
                Polygon(
                    [(500, 0), (510, 0), (510, 10), (500, 10)],
                    [[(502, 2), (504, 2), (504, 4), (502, 4)]],
                ),
                box(600, 0, 610, 10),
            ]
        ),
    }
    parks = frame(spark, polygons, "geom").repartition(3).cache()
    try:
        report = profile_frames(footprints, points, parks, 5)
        assert report["raw_buildings"] == 3 and report["selected_buildings"] == 2
        assert report["selected_building_footprints"]["vertices"]["total"] == 10
        complexity = report["full_park_polygons"]
        assert complexity["vertices"]["total"] == 20
        assert complexity["parts"]["total"] == 3 and complexity["rings"]["total"] == 4
        assert complexity["projected_wkb_bytes"]["total"] == sum(
            len(p.wkb) for p in polygons.values()
        )
        counts = report["logical_candidates"]
        assert counts["all_pairs_upper_bound"] == 4 and counts["envelope_candidate_rows"] == 1
        assert counts["retained_share"] == 0.25
        assert (
            counts["buildings_with_zero_candidates"] == 1
            and counts["parks_with_zero_candidates"] == 1
        )
        assert counts["parks_per_building"] == distribution([1, 0])
        assert counts["buildings_per_park"] == distribution([1, 0])
        assert counts["actual_distance_evaluations"] is None
        partition_rows = report["input_partitions"]["selected_points"]
        assert partition_rows["partitions"] == 4 and len(partition_rows["row_counts"]) == 4
        assert sum(partition_rows["row_counts"]) == 2 and partition_rows["row_counts"].count(0) >= 2
        assert report["input_partitions"]["parks"]["distribution"]["total"] == 2
    finally:
        points.unpersist()
        parks.unpersist()


@pytest.mark.integration
def test_empty_park_population_preserves_zero_building_degrees(spark):
    footprints = frame(spark, {"b": box(-1, -1, 1, 1)}, "geom")
    points = frame(spark, {"b": Point(0, 0)}, "point")
    parks = frame(spark, {}, "geom")
    report = profile_frames(footprints, points, parks, 5)
    counts = report["logical_candidates"]
    assert counts["envelope_candidate_rows"] == 0 and counts["retained_share"] is None
    assert counts["parks_per_building"] == distribution([0])
    assert counts["buildings_per_park"] == distribution([])
    assert report["full_park_polygons"]["vertices"]["count"] == 0


@pytest.mark.integration
@pytest.mark.parametrize("limit", ["rows", "pairs"])
def test_frame_budgets_fail_before_geometry_or_candidate_collection(spark, limit):
    buildings = spark.range(20001 if limit == "rows" else 2001).selectExpr(
        "cast(id as string) AS id"
    )
    parks = spark.range(1 if limit == "rows" else 1000).selectExpr("cast(id as string) AS id")
    with pytest.raises(
        ValueError, match="pilot limit" if limit == "rows" else "logical pair limit"
    ):
        profile_frames(buildings, buildings, parks, 500)


@pytest.mark.integration
def test_cli_profile_preserves_retained_inputs_reference_and_existing_output(tmp_path):
    case = make_processing_case(tmp_path / "case")
    processed = subprocess.run(
        [
            sys.executable,
            "-m",
            "urban_geography.cli",
            "process",
            "--config",
            str(case.config_path),
            "--raw-root",
            str(case.raw_root),
            "--output",
            str(case.output),
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert processed.returncode == 0, processed.stderr
    original = {p: digest(p) for p in case.config_path.parent.rglob("*") if p.is_file()}
    output = tmp_path / "profile"
    command = [
        sys.executable,
        "scripts/profile_workload.py",
        "--config",
        str(case.config_path),
        "--reference-run",
        str(case.output),
        "--raw-root",
        str(case.raw_root),
        "--output",
        str(output),
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "complete"
    path = output / "profile.json"
    report = json.loads(path.read_text())
    assert report["status"] == "complete" and report["inputs_unchanged"]
    assert report["raw_parquet"]["building"]["rows"] == 3
    assert report["profile"]["selected_buildings"] == 2 and report["reference_matches"] == 1
    assert report["profile"]["logical_candidates"]["envelope_candidate_rows"] == 1
    assert report["occupied_grid_buildings"]["count"] == 2
    assert report["occupied_grid_buildings"]["total"] == 2
    assert all(digest(p) == expected for p, expected in original.items())
    complete_hash = digest(path)
    retry = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    assert retry.returncode != 0 and "output exists" in retry.stderr
    assert digest(path) == complete_hash
