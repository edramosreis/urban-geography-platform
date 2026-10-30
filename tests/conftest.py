"""Shared small retained-run fixtures for offline Iceberg history acceptance."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from urban_geography.config import RunConfig
from urban_geography.ingest import digest
from urban_geography.lakehouse import FACT_TYPES, load_completed_run
from urban_geography.spark import create_session


@pytest.fixture(scope="module")
def history_spark(tmp_path_factory):
    root = tmp_path_factory.mktemp("release-history")
    spark = create_session(root / "events", warehouse=root / "warehouse")
    yield spark
    spark.stop()


@pytest.fixture
def history_run_factory(tmp_path):
    made = []

    def make(release="2026-09-23.1", revision=0, *, bbox=None):
        root = tmp_path / ("run-" + str(len(made)))
        config = RunConfig("offline_history", release, bbox or (-8.435, 40.195, -8.415, 40.215))
        common = {
            "grid_id": "epsg3763_250m_0_0",
            "source_release": release,
            "area_id": config.area_id,
            "metric_version": "park_proximity_v1",
            "threshold_m": 500.0,
            "selection_version": "point_on_surface_bbox_v1",
            "boundary_id": None,
            "boundary_release": None,
            "boundary_geometry_sha256": None,
        }
        rows = [
            {
                **common,
                "building_id": "b1",
                "park_id": "park",
                "distance_m": 50.0 + 25 * revision,
                "within_threshold": True,
            },
            {
                **common,
                "building_id": "b3" if revision else "b2",
                "park_id": None,
                "distance_m": None,
                "within_threshold": False,
            },
        ]
        arrow_types = {"string": pa.string(), "double": pa.float64(), "boolean": pa.bool_()}
        schema = pa.schema([(name, arrow_types[kind]) for name, kind in FACT_TYPES.items()])
        facts = root / "building_proximity" / "part-00000.parquet"
        grid = root / "grid_proximity" / "part-00000.parquet"
        facts.parent.mkdir(parents=True)
        grid.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), facts)
        pq.write_table(pa.Table.from_pylist([{"mapped_buildings": 2, "buildings_within": 1}]), grid)
        summary = {
            "status": "complete",
            "config": config.as_dict(),
            "metric_version": "park_proximity_v1",
            "mapped_buildings": 2,
            "candidate_parks": 1,
            "buildings_within_500m": 1,
            "share_within": 0.5,
            "completed_at_utc": "2026-10-25T00:00:00Z",
            "source_url": "https://example.invalid/synthetic-release",
            "attribution": "Informação geográfica cedida pela Direção-Geral do Território",
            "selection": {
                "version": "point_on_surface_bbox_v1",
                "scope": "pilot_bbox",
                "bbox": list(config.bbox),
                "boundary": None,
            },
            "files": {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*.parquet")},
        }
        (root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
        made.append(root)
        return load_completed_run(root)

    return make


@pytest.fixture
def frozen(history_run_factory, tmp_path, monkeypatch):
    from glue_release_fixtures import make_frozen

    return make_frozen(history_run_factory, tmp_path, monkeypatch)
