"""Offline CLI integration tests with real Spark/Sedona and synthetic retained raw data."""

import json
import os
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from processing_fixtures import ProcessingCase, make_processing_case
from pyproj import Transformer
from shapely import from_wkb
from shapely.geometry import Polygon, box, mapping
from shapely.ops import transform

from urban_geography.boundary import (
    ATTRIBUTION,
    LICENSE_URL,
    BoundarySpec,
    geometry_digest,
)
from urban_geography.ingest import digest

pytestmark = pytest.mark.integration


@pytest.fixture
def processing_case(tmp_path):
    return make_processing_case(tmp_path)


def run_process(
    case: ProcessingCase, *options: str, command: str = "process"
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "urban_geography.cli",
            command,
            "--config",
            str(case.config_path),
            "--raw-root",
            str(case.raw_root),
            "--output",
            str(case.output),
            *options,
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
    assert summary["processing_implementation"] == "exact_distance_join_v1"
    assert summary["metric_version"] == "park_proximity_v1"
    assert summary["mapped_buildings"] == 2
    assert summary["candidate_parks"] == 1
    assert summary["buildings_within_500m"] == 1
    assert summary["share_within"] == 0.5
    assert summary["selection"] == {
        "version": "point_on_surface_bbox_v1",
        "scope": "pilot_bbox",
        "bbox": list(case.config.bbox),
        "boundary": None,
    }
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
        assert row["selection_version"] == "point_on_surface_bbox_v1"
        assert row["boundary_id"] is None
        assert row["boundary_release"] is None
        assert row["boundary_geometry_sha256"] is None

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
    assert "BroadcastIndexJoin" in plan
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


@pytest.fixture
def processing_boundary(processing_case, tmp_path):
    """Synthetic retained municipality; the park lies wholly outside its polygon."""
    case = processing_case
    x, y = Transformer.from_crs(4326, 3763, always_xy=True).transform(-8.425, 40.205)
    to_wgs84 = Transformer.from_crs(3763, 4326, always_xy=True).transform
    polygon = transform(to_wgs84, box(x - 525, y - 25, x - 475, y + 25))
    outside_park = transform(to_wgs84, box(x - 650, y - 50, x - 550, y + 50))
    assert polygon.disjoint(outside_park)
    park_path = case.raw / "land_use.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "park", "geometry": bytes(outside_park.wkb)}]), park_path
    )
    raw_manifest_path = case.raw / "manifest.json"
    raw_manifest = json.loads(raw_manifest_path.read_text(encoding="utf-8"))
    raw_manifest["extracts"]["land_use"]["sha256"] = digest(park_path)
    raw_manifest_path.write_text(json.dumps(raw_manifest))

    geometry = mapping(polygon)
    spec = BoundarySpec(
        area_id="synthetic_municipality",
        source_release="CAOP2025",
        municipality_code="0603",
        municipality_name="Coimbra",
        metadata_id="12345678123456781234567812345678",
        expected_geometry_sha256=geometry_digest(geometry),
    )
    spec_path = tmp_path / "boundary_config.json"
    spec_path.write_text(json.dumps(asdict(spec)))
    root = tmp_path / "boundaries"
    retained = root / spec.source_release / spec.area_id / spec.expected_geometry_sha256[:16]
    retained.mkdir(parents=True)
    collection = {
        "id": "municipios",
        "title": "CAOP2025 Municípios",
        "links": [
            {
                "rel": "canonical",
                "href": "https://snig.dgterritorio.gov.pt/rndg/srv/por/catalog.search#/metadata/"
                + spec.metadata_id,
            }
        ],
    }
    metric = transform(Transformer.from_crs(4326, 3763, always_xy=True).transform, polygon)
    feature = {
        "type": "Feature",
        "id": spec.municipality_code,
        "geometry": geometry,
        "properties": {
            "dtmn": spec.municipality_code,
            "municipio": spec.municipality_name,
            "area_ha": metric.area / 10000,
        },
    }
    payloads = {
        "collection_before.json": json.dumps(collection).encode(),
        "collection_after.json": json.dumps(collection).encode(),
        "source_feature.json": json.dumps(feature).encode(),
        "boundary.geojson": json.dumps(feature).encode(),
        "metadata.xml": (
            f"<metadata><fileIdentifier>{spec.metadata_id}</fileIdentifier>"
            "<title>CAOP2025 (Continente)</title>"
            f"<otherConstraints>{LICENSE_URL} {ATTRIBUTION}</otherConstraints></metadata>"
        ).encode(),
    }
    for name, content in payloads.items():
        (retained / name).write_bytes(content)
    manifest = {
        "contract_version": 1,
        "config": asdict(spec),
        "acquired_at_utc": "2026-10-24T00:00:00+00:00",
        "files": {name: digest(retained / name) for name in payloads},
    }
    (retained / "manifest.json").write_text(json.dumps(manifest))
    return spec, spec_path, root, retained


def test_cli_boundary_selection_records_lineage_and_preserves_outside_parks(
    processing_case, processing_boundary
):
    case = processing_case
    spec, spec_path, root, retained = processing_boundary
    inputs_before = file_hashes(case.raw), file_hashes(retained)
    result = run_process(case, "--boundary-config", str(spec_path), "--boundary-root", str(root))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "complete", "path": str(case.output)}
    summary = json.loads((case.output / "summary.json").read_text())
    assert summary["processing_implementation"] == "exact_distance_join_v1"
    assert summary["mapped_buildings"] == summary["buildings_within_500m"] == 1
    assert summary["candidate_parks"] == 1
    assert summary["share_within"] == 1
    assert summary["input_rows"] == {"building": 3, "land_use": 1}
    selection = summary["selection"]
    assert selection["version"] == "point_on_surface_bbox_boundary_v1"
    assert selection["scope"] == "pilot_bbox_intersection_boundary"
    assert selection["bbox"] == list(case.config.bbox)
    lineage = selection["boundary"]
    assert lineage["manifest_sha256"] == digest(retained / "manifest.json")
    assert lineage["geojson_sha256"] == digest(retained / "boundary.geojson")
    assert lineage["geometry_sha256"] == spec.expected_geometry_sha256
    assert lineage["source_release"] == spec.source_release
    assert lineage["metadata_id"] == spec.metadata_id
    assert lineage["license"] == "CC-BY-4.0"
    assert lineage["attribution"] == ATTRIBUTION

    facts = pq.read_table(case.output / "building_proximity").to_pylist()
    assert len(facts) == 1
    assert facts[0]["building_id"] == "near"
    assert facts[0]["park_id"] == "park"
    assert facts[0]["distance_m"] == pytest.approx(50, abs=1e-3)
    assert facts[0]["within_threshold"] is True
    grids = pq.read_table(case.output / "grid_proximity").to_pylist()
    assert len(grids) == 1
    assert grids[0]["mapped_buildings"] == grids[0]["buildings_within"] == 1
    for row in facts + grids:
        assert row["source_release"] == case.config.release
        assert row["area_id"] == case.config.area_id
        assert row["selection_version"] == selection["version"]
        assert row["boundary_id"] == spec.area_id
        assert row["boundary_release"] == spec.source_release
        assert row["boundary_geometry_sha256"] == spec.expected_geometry_sha256
    oracle_command = [
        sys.executable,
        "scripts/verify_pilot.py",
        "--config",
        str(case.config_path),
        "--raw-root",
        str(case.raw_root),
        "--output",
        str(case.output),
    ]
    missing_boundary = subprocess.run(
        oracle_command, capture_output=True, text=True, check=False, timeout=30
    )
    assert missing_boundary.returncode != 0
    assert "Run boundary lineage differs" in missing_boundary.stderr
    oracle = subprocess.run(
        [*oracle_command, "--boundary-config", str(spec_path), "--boundary-root", str(root)],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert oracle.returncode == 0, oracle.stderr
    checked = json.loads(oracle.stdout)
    assert checked["rows_checked"] == checked["matched"] == 1
    assert checked["boundary_geometry_sha256"] == spec.expected_geometry_sha256
    assert checked["grid_totals_match"] is True
    assert (file_hashes(case.raw), file_hashes(retained)) == inputs_before


@pytest.mark.parametrize("defect", ["checksum", "configuration"])
def test_cli_boundary_defects_fail_before_spark_startup(
    processing_case, processing_boundary, defect
):
    case = processing_case
    spec, spec_path, root, retained = processing_boundary
    if defect == "checksum":
        (retained / "boundary.geojson").write_text("{}")
        message = "Retained boundary file checksum differs"
    else:
        spec_path.write_text(json.dumps(asdict(replace(spec, municipality_name="Lisboa"))))
        message = "Retained boundary manifest contract or configuration differs"
    inputs_before = file_hashes(case.raw), file_hashes(retained)
    result = run_process(case, "--boundary-config", str(spec_path), "--boundary-root", str(root))
    assert result.returncode != 0
    assert message in result.stderr
    assert not case.output.exists()
    assert not list(case.output.parent.rglob("spark-events"))
    assert not list(case.output.parent.rglob("summary.json"))
    assert (file_hashes(case.raw), file_hashes(retained)) == inputs_before


def test_cli_extract_rejects_processing_only_boundary_option(processing_case, processing_boundary):
    _, spec_path, root, _ = processing_boundary
    result = run_process(
        processing_case,
        "--boundary-config",
        str(spec_path),
        "--boundary-root",
        str(root),
        command="extract",
    )
    assert result.returncode != 0
    assert "--boundary-config applies to process or run" in result.stderr
    assert not processing_case.output.exists()
