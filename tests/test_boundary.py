import copy
import json
from dataclasses import replace
from unittest.mock import MagicMock

import pytest
import requests
from pyproj import Transformer
from shapely.geometry import Polygon, box, mapping, shape
from shapely.ops import transform

from urban_geography.boundary import (
    ATTRIBUTION,
    BASE_URL,
    CRS84,
    LICENSE_URL,
    BoundarySpec,
    acquire_boundary,
    geometry_digest,
    load_retained_boundary,
    sha256,
    validate_feature,
    validate_metadata,
    validate_retained_boundary,
)


@pytest.fixture
def official_sample():
    polygon = Polygon(
        [(-8.43, 40.20), (-8.42, 40.20), (-8.42, 40.21), (-8.43, 40.21), (-8.43, 40.20)],
        [
            [
                (-8.427, 40.203),
                (-8.427, 40.207),
                (-8.423, 40.207),
                (-8.423, 40.203),
                (-8.427, 40.203),
            ]
        ],
    )
    geometry = mapping(polygon)
    spec = BoundarySpec(
        area_id="test_municipality",
        source_release="CAOP2025",
        municipality_code="0603",
        municipality_name="Coimbra",
        metadata_id="12345678123456781234567812345678",
        expected_geometry_sha256=geometry_digest(geometry),
    )
    metric = transform(Transformer.from_crs(4326, 3763, always_xy=True).transform, polygon)
    feature = {
        "type": "Feature",
        "id": "0603",
        "properties": {"dtmn": "0603", "municipio": "Coimbra", "area_ha": metric.area / 10000},
        "geometry": geometry,
        "next": "live navigation is not retained in derived geometry",
    }
    canonical = (
        "https://snig.dgterritorio.gov.pt/rndg/srv/por/catalog.search#/metadata/" + spec.metadata_id
    )
    collection = {
        "id": "municipios",
        "title": "CAOP2025 Municípios",
        "links": [{"rel": "canonical", "href": canonical}],
    }
    metadata = (
        f"<MD_Metadata><fileIdentifier>{spec.metadata_id}</fileIdentifier>"
        "<title>CAOP2025 (Continente)</title><dateStamp>2026-02-18</dateStamp>"
        f"<otherConstraints>{LICENSE_URL} {ATTRIBUTION}</otherConstraints></MD_Metadata>"
    ).encode()
    return {
        "spec": spec,
        "feature": feature,
        "before": copy.deepcopy(collection),
        "after": copy.deepcopy(collection),
        "metadata": metadata,
        "crs": f"<{CRS84}>",
    }


def mock_official_service(monkeypatch, sample):
    calls = []
    collection_requests = 0

    def get(_session, url, **kwargs):
        nonlocal collection_requests
        calls.append(url)
        if url == BASE_URL + "?f=json":
            key = "before" if collection_requests == 0 else "after"
            collection_requests += 1
            content = json.dumps(sample[key]).encode()
        elif "/formatters/xml" in url:
            content = sample["metadata"]
        elif url == BASE_URL + "/items/0603?f=json":
            content = json.dumps(sample["feature"]).encode()
        else:
            raise AssertionError(f"Unexpected source request: {url}")
        response = MagicMock()
        response.status_code = 200
        response.headers = {"Content-Length": str(len(content)), "Content-Crs": sample["crs"]}
        response.iter_content.return_value = [content]
        response.__enter__.return_value = response
        assert kwargs["allow_redirects"] is False
        return response

    monkeypatch.setattr(requests.Session, "get", get)
    return calls


def test_retention_preserves_geometry_reuses_offline_and_detects_corruption(
    tmp_path, monkeypatch, official_sample
):
    sample = official_sample
    calls = mock_official_service(monkeypatch, sample)
    path = acquire_boundary(sample["spec"], tmp_path)
    manifest = validate_retained_boundary(path, sample["spec"])
    loaded = load_retained_boundary(sample["spec"], tmp_path)
    assert loaded.geometry_wkb == bytes(shape(sample["feature"]["geometry"]).wkb)
    assert loaded.as_dict()["geometry_sha256"] == sample["spec"].expected_geometry_sha256
    assert loaded.manifest_sha256 == sha256((path / "manifest.json").read_bytes())
    assert loaded.geojson_sha256 == sha256((path / "boundary.geojson").read_bytes())
    assert loaded.as_dict()["attribution"] == ATTRIBUTION
    retained = json.loads((path / "boundary.geojson").read_text())

    assert retained["geometry"] == json.loads(json.dumps(sample["feature"]["geometry"]))
    assert "next" not in retained
    assert manifest["license"] == "CC-BY-4.0"
    assert manifest["attribution"] == ATTRIBUTION
    assert manifest["downloaded_bytes"] < sample["spec"].max_download_bytes
    assert len(calls) == 4
    assert calls[0] == calls[-1] == BASE_URL + "?f=json"
    assert calls[2] == BASE_URL + "/items/0603?f=json"
    assert acquire_boundary(sample["spec"], tmp_path) == path
    assert len(calls) == 4

    (path / "boundary.geojson").write_text("{}")
    with pytest.raises(ValueError, match="file checksum"):
        acquire_boundary(sample["spec"], tmp_path)
    assert len(calls) == 4


@pytest.mark.parametrize("when", ["before", "after"])
def test_live_service_release_drift_prevents_publication(
    tmp_path, monkeypatch, official_sample, when
):
    official_sample[when]["title"] = "CAOP2026 Municípios"
    calls = mock_official_service(monkeypatch, official_sample)

    with pytest.raises(ValueError, match="collection release"):
        acquire_boundary(official_sample["spec"], tmp_path)

    assert len(calls) == (1 if when == "before" else 4)
    assert not list(tmp_path.rglob("manifest.json"))
    assert not list(tmp_path.rglob("boundary.geojson"))


def test_unexpected_response_crs_prevents_publication(tmp_path, monkeypatch, official_sample):
    official_sample["crs"] = "<http://www.opengis.net/def/crs/EPSG/0/3763>"
    mock_official_service(monkeypatch, official_sample)

    with pytest.raises(ValueError, match="response CRS"):
        acquire_boundary(official_sample["spec"], tmp_path)

    assert not list(tmp_path.rglob("manifest.json"))


def test_changed_geometry_requires_an_explicit_new_pin(official_sample):
    sample = official_sample
    sample["feature"]["geometry"] = mapping(box(-8.431, 40.20, -8.42, 40.21))
    with pytest.raises(ValueError, match="geometry checksum"):
        validate_feature(sample["spec"], sample["feature"])


def test_wrong_municipality_is_rejected(official_sample):
    sample = official_sample
    sample["feature"]["properties"]["municipio"] = "Lisboa"
    with pytest.raises(ValueError, match="municipality identity"):
        validate_feature(sample["spec"], sample["feature"])


def test_invalid_geometry_is_not_automatically_repaired(official_sample):
    sample = official_sample
    geometry = mapping(
        Polygon(
            [
                (-8.43, 40.20),
                (-8.42, 40.21),
                (-8.43, 40.21),
                (-8.42, 40.20),
                (-8.43, 40.20),
            ]
        )
    )
    sample["feature"]["geometry"] = geometry
    spec = replace(sample["spec"], expected_geometry_sha256=geometry_digest(geometry))
    with pytest.raises(ValueError, match="Invalid boundary geometry"):
        validate_feature(spec, sample["feature"])


def test_missing_licence_evidence_is_rejected(official_sample):
    sample = official_sample
    without_terms = sample["metadata"].replace(LICENSE_URL.encode(), b"no licence evidence")
    with pytest.raises(ValueError, match="licence"):
        validate_metadata(sample["spec"], without_terms)


def test_official_area_disagreement_is_rejected(official_sample):
    sample = official_sample
    sample["feature"]["properties"]["area_ha"] *= 100
    with pytest.raises(ValueError, match="official area"):
        validate_feature(sample["spec"], sample["feature"])


def test_boundary_budget_cannot_expand_beyond_the_small_acquisition(official_sample):
    with pytest.raises(ValueError, match="4 MiB"):
        replace(official_sample["spec"], max_download_bytes=5 * 1024 * 1024)


def test_loading_missing_boundary_does_not_attempt_acquisition(
    tmp_path, monkeypatch, official_sample
):
    def unexpected_download(*args, **kwargs):
        raise AssertionError("Processing must never acquire a missing boundary")

    monkeypatch.setattr(requests.Session, "get", unexpected_download)
    with pytest.raises(FileNotFoundError):
        load_retained_boundary(official_sample["spec"], tmp_path)
