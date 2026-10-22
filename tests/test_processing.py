"""Offline CLI integration tests with real Spark/Sedona and synthetic retained raw data."""

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import Transformer
from shapely import from_wkb
from shapely.geometry import Polygon, box
from shapely.ops import transform

from urban_geography.config import RunConfig
from urban_geography.ingest import config_key, digest

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class ProcessingCase:
    config: RunConfig
    config_path: Path
    raw_root: Path
    raw: Path
    output: Path


@pytest.fixture
def processing_case(tmp_path):
    config = RunConfig(
        area_id="offline_processing_test",
        release="2026-09-23.1",
        bbox=(-8.435, 40.195, -8.415, 40.215),
    )
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config.as_dict()), encoding="utf-8")
    raw_root = tmp_path / "raw"
    raw = raw_root / config.release / config.area_id / config_key(config)
    raw.mkdir(parents=True)

    # Define meaningful distances in metres, then serialize upstream-style WGS84 WKB.
    x, y = Transformer.from_crs(4326, 3763, always_xy=True).transform(-8.425, 40.205)
    to_wgs84 = Transformer.from_crs(3763, 4326, always_xy=True).transform
    footprints = {
        "near": box(x - 505, y - 5, x - 495, y + 5),
        "far": box(x + 495, y - 5, x + 505, y + 5),
        "outside": box(x + 1995, y - 5, x + 2005, y + 5),
    }
    buildings = [
        {"id": key, "geometry": bytes(transform(to_wgs84, geometry).wkb)}
        for key, geometry in footprints.items()
    ]
    parks = [
        {
            "id": "park",
            "geometry": bytes(transform(to_wgs84, box(x - 550, y - 50, x - 450, y + 50)).wkb),
        }
    ]
    manifest = {"contract_version": 1, "config": config.as_dict(), "extracts": {}}
    for kind, rows in (("building", buildings), ("land_use", parks)):
        path = raw / f"{kind}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        manifest["extracts"][kind] = {"rows": len(rows), "sha256": digest(path)}
    (raw / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return ProcessingCase(config, config_path, raw_root, raw, tmp_path / "published")


def run_process(case: ProcessingCase) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
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
        env={**os.environ, "PROJ_NETWORK": "OFF"},
    )


def file_hashes(directory: Path) -> dict[str, str]:
    return {
        str(path.relative_to(directory)): digest(path)
        for path in directory.rglob("*")
        if path.is_file()
    }


def test_cli_publishes_complete_results_and_preserves_existing_run(processing_case):
    case = processing_case
    original_raw = file_hashes(case.raw)
    result = run_process(case)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "complete", "path": str(case.output)}

    summary = json.loads((case.output / "summary.json").read_text())
    assert summary["status"] == "complete"
    assert summary["mapped_buildings"] == 2
    assert summary["candidate_parks"] == 1
    assert summary["buildings_within_500m"] == 1
    assert summary["share_within"] == 0.5
    assert summary["input_rows"] == {"building": 3, "land_use": 1}
    assert summary["raw_manifest_sha256"] == digest(case.raw / "manifest.json")

    facts = {
        row["building_id"]: row
        for row in pq.read_table(case.output / "building_proximity").to_pylist()
    }
    assert set(facts) == {"near", "far"}
    assert facts["near"]["park_id"] == "park"
    assert facts["near"]["distance_m"] == pytest.approx(0, abs=1e-6)
    assert facts["near"]["within_threshold"] is True
    assert facts["far"]["park_id"] is None
    assert facts["far"]["distance_m"] is None
    assert facts["far"]["within_threshold"] is False
    for row in facts.values():
        assert row["source_release"] == case.config.release
        assert row["area_id"] == case.config.area_id
        assert row["metric_version"] == "park_proximity_v1"
        assert row["threshold_m"] == 500

    grids = pq.read_table(case.output / "grid_proximity").to_pylist()
    assert len(grids) == 2
    assert sum(row["mapped_buildings"] for row in grids) == 2
    assert sum(row["buildings_within"] for row in grids) == 1
    assert sorted(row["share_within"] for row in grids) == [0.0, 1.0]

    parquet_files = {
        str(path.relative_to(case.output)): digest(path) for path in case.output.rglob("*.parquet")
    }
    assert parquet_files
    assert summary["files"] == parquet_files
    plan = (case.output / "plan.txt").read_text()
    assert "== Parsed Logical Plan ==" in plan
    assert "== Physical Plan ==" in plan
    event_files = [
        path
        for path in (case.output / "spark-events").iterdir()
        if path.is_file() and not path.name.startswith(".")
    ]
    assert event_files
    assert any(
        json.loads(line).get("Event") == "SparkListenerApplicationEnd"
        for path in event_files
        for line in path.read_text().splitlines()
    )
    assert file_hashes(case.raw) == original_raw

    # A retry at the same destination must preserve all published files byte-for-byte.
    published_before = file_hashes(case.output)
    retry = run_process(case)
    assert retry.returncode != 0
    assert "Output already exists" in retry.stderr
    assert file_hashes(case.output) == published_before
    assert file_hashes(case.raw) == original_raw


@pytest.mark.parametrize(
    ("defect", "message"),
    [
        ("duplicate_id", "Duplicate source IDs"),
        ("self_intersection", "Invalid geometry records"),
    ],
)
def test_cli_rejects_invalid_geometry_data_without_publishing(processing_case, defect, message):
    case = processing_case
    building_path = case.raw / "building.parquet"
    rows = pq.read_table(building_path).to_pylist()
    if defect == "duplicate_id":
        rows[1]["id"] = rows[0]["id"]
    else:
        left, bottom, right, top = from_wkb(rows[0]["geometry"]).bounds
        bowtie = Polygon(
            [
                (left, bottom),
                (right, top),
                (left, top),
                (right, bottom),
                (left, bottom),
            ]
        )
        assert not bowtie.is_valid
        rows[0]["geometry"] = bytes(bowtie.wkb)
    pq.write_table(pa.Table.from_pylist(rows), building_path)
    manifest_path = case.raw / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    # Keep the checksum valid so the real Spark geometry gate is exercised.
    manifest["extracts"]["building"]["sha256"] = digest(building_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    original_raw = file_hashes(case.raw)

    result = run_process(case)

    assert result.returncode != 0
    assert message in result.stderr
    assert not case.output.exists()
    assert not list(case.output.parent.rglob("summary.json"))
    assert file_hashes(case.raw) == original_raw


def test_cli_rejects_corrupt_raw_input_without_publishing(processing_case):
    case = processing_case
    (case.raw / "building.parquet").write_bytes(b"corrupted parquet input")
    original_raw = file_hashes(case.raw)

    result = run_process(case)

    assert result.returncode != 0
    assert "Raw checksum mismatch: building" in result.stderr
    assert not case.output.exists()
    assert file_hashes(case.raw) == original_raw
