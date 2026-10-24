"""Pin and retain one official DGT municipality boundary independently of pilot extraction."""

import hashlib
import json
import math
import re
import uuid
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform

from urban_geography.http import DownloadBudget

BASE_URL = "https://ogcapi.dgterritorio.gov.pt/collections/municipios"
CRS84 = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"
ATTRIBUTION = "Informação geográfica cedida pela Direção-Geral do Território"
FILES = (
    "collection_before.json",
    "metadata.xml",
    "source_feature.json",
    "collection_after.json",
    "boundary.geojson",
)


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def geometry_digest(geometry: dict[str, Any]) -> str:
    return sha256(
        json.dumps(geometry, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    )


@dataclass(frozen=True)
class BoundarySpec:
    area_id: str
    source_release: str
    municipality_code: str
    municipality_name: str
    metadata_id: str
    expected_geometry_sha256: str
    max_download_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9_]+", self.area_id):
            raise ValueError("Invalid boundary area_id")
        if not re.fullmatch(r"CAOP\d{4}", self.source_release):
            raise ValueError("Pin a specific CAOP release")
        if not re.fullmatch(r"\d{4}", self.municipality_code):
            raise ValueError("Municipality code must retain its four digits")
        if not self.municipality_name.strip():
            raise ValueError("Municipality name is required")
        uuid.UUID(self.metadata_id)
        if not re.fullmatch(r"[0-9a-f]{64}", self.expected_geometry_sha256):
            raise ValueError("Pin a SHA256 geometry digest")
        if not 0 < self.max_download_bytes <= 4 * 1024 * 1024:
            raise ValueError("Boundary transfer budget must be within 4 MiB")


def load_boundary_spec(path: Path) -> BoundarySpec:
    return BoundarySpec(**json.loads(path.read_text(encoding="utf-8")))


def validate_collection(spec: BoundarySpec, collection: dict[str, Any]) -> None:
    expected = f"{spec.source_release} Municípios"
    if collection.get("id") != "municipios" or collection.get("title") != expected:
        raise ValueError("DGT collection release differs from the pinned release")
    canonical = (
        f"https://snig.dgterritorio.gov.pt/rndg/srv/por/catalog.search#/metadata/{spec.metadata_id}"
    )
    if not any(
        link.get("rel") == "canonical" and link.get("href") == canonical
        for link in collection.get("links", [])
    ):
        raise ValueError("DGT collection metadata identity differs")


def validate_metadata(spec: BoundarySpec, content: bytes) -> dict[str, str]:
    root = ET.fromstring(content)
    values: dict[str, list[str]] = {}
    for node in root.iter():
        tag = node.tag.rsplit("}", 1)[-1]
        if tag in {"fileIdentifier", "title", "dateStamp", "otherConstraints", "useLimitation"}:
            values.setdefault(tag, []).append(" ".join(node.itertext()).strip())
    if spec.metadata_id not in values.get("fileIdentifier", []):
        raise ValueError("Official metadata identifier differs")
    titles = [
        title
        for title in values.get("title", [])
        if spec.source_release in title and "Continente" in title
    ]
    if not titles:
        raise ValueError("Official metadata release differs")
    terms = " ".join(values.get("otherConstraints", []) + values.get("useLimitation", []))
    if LICENSE_URL not in terms or ATTRIBUTION not in terms:
        raise ValueError("Expected licence and attribution are not present in official metadata")
    return {"title": titles[0], "metadata_date": values.get("dateStamp", [""])[0]}


def validate_feature(spec: BoundarySpec, feature: dict[str, Any]) -> dict[str, Any]:
    properties = feature.get("properties", {})
    if (
        feature.get("type") != "Feature"
        or feature.get("id") != spec.municipality_code
        or properties.get("dtmn") != spec.municipality_code
        or properties.get("municipio") != spec.municipality_name
    ):
        raise ValueError("Boundary municipality identity differs")
    geometry = feature.get("geometry") or {}
    if geometry.get("type") not in {"Polygon", "MultiPolygon"}:
        raise ValueError("Boundary requires polygon geometry")
    polygon = shape(geometry)
    if polygon.is_empty or not polygon.is_valid or polygon.has_z:
        raise ValueError("Invalid boundary geometry")
    west, south, east, north = polygon.bounds
    if not (-9.6 <= west < east <= -6.1 and 36.8 <= south < north <= 42.2):
        raise ValueError("Boundary coordinates are not mainland Portugal longitude/latitude")
    if geometry_digest(geometry) != spec.expected_geometry_sha256:
        raise ValueError("Boundary geometry checksum differs from the pinned geometry")
    metric = transform(Transformer.from_crs("OGC:CRS84", 3763, always_xy=True).transform, polygon)
    reported_ha = float(properties["area_ha"])
    if (
        not math.isfinite(reported_ha)
        or reported_ha <= 0
        or not math.isclose(metric.area, reported_ha * 10000, rel_tol=1e-4)
    ):
        raise ValueError("Projected boundary area disagrees with the official area")
    return {
        "geometry_type": polygon.geom_type,
        "bounds_crs84": list(polygon.bounds),
        "area_m2": metric.area,
        "reported_area_ha": reported_ha,
    }


def validate_retained_boundary(path: Path, spec: BoundarySpec) -> dict[str, Any]:
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("contract_version") != 1 or manifest.get("config") != asdict(spec):
        raise ValueError("Retained boundary manifest contract or configuration differs")
    if set(manifest.get("files", {})) != set(FILES):
        raise ValueError("Retained boundary manifest file inventory differs")
    for name in FILES:
        if sha256((path / name).read_bytes()) != manifest["files"][name]:
            raise ValueError(f"Retained boundary file checksum differs: {name}")
    validate_collection(spec, json.loads((path / "collection_before.json").read_bytes()))
    validate_collection(spec, json.loads((path / "collection_after.json").read_bytes()))
    validate_metadata(spec, (path / "metadata.xml").read_bytes())
    validate_feature(spec, json.loads((path / "boundary.geojson").read_bytes()))
    return manifest


@dataclass(frozen=True)
class RetainedBoundary:
    spec: BoundarySpec
    geometry_wkb: bytes
    manifest_sha256: str
    geojson_sha256: str
    acquired_at_utc: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "area_id": self.spec.area_id,
            "source_release": self.spec.source_release,
            "municipality_code": self.spec.municipality_code,
            "municipality_name": self.spec.municipality_name,
            "metadata_id": self.spec.metadata_id,
            "geometry_sha256": self.spec.expected_geometry_sha256,
            "manifest_sha256": self.manifest_sha256,
            "geojson_sha256": self.geojson_sha256,
            "acquired_at_utc": self.acquired_at_utc,
            "source_crs": CRS84,
            "license": "CC-BY-4.0",
            "license_url": LICENSE_URL,
            "attribution": ATTRIBUTION,
        }


def load_retained_boundary(spec: BoundarySpec, root: Path) -> RetainedBoundary:
    """Load verified local inputs only; acquisition is a separate explicit operation."""
    path = root / spec.source_release / spec.area_id / spec.expected_geometry_sha256[:16]
    manifest = validate_retained_boundary(path, spec)
    content = (path / "boundary.geojson").read_bytes()
    if sha256(content) != manifest["files"]["boundary.geojson"]:
        raise ValueError("Retained boundary changed while loading")
    return RetainedBoundary(
        spec=spec,
        geometry_wkb=bytes(shape(json.loads(content)["geometry"]).wkb),
        manifest_sha256=sha256((path / "manifest.json").read_bytes()),
        geojson_sha256=sha256(content),
        acquired_at_utc=manifest["acquired_at_utc"],
    )


def acquire_boundary(spec: BoundarySpec, root: Path) -> Path:
    destination = root / spec.source_release / spec.area_id / spec.expected_geometry_sha256[:16]
    if destination.exists():
        validate_retained_boundary(destination, spec)
        return destination
    budget = DownloadBudget(
        spec.max_download_bytes,
        allowed_hosts=frozenset(
            {
                "ogcapi.dgterritorio.gov.pt",
                "snig.dgterritorio.gov.pt",
            }
        ),
    )
    urls = {
        "collection_before.json": BASE_URL + "?f=json",
        "metadata.xml": (
            "https://snig.dgterritorio.gov.pt/rndg/srv/api/records/"
            + spec.metadata_id
            + "/formatters/xml"
        ),
        "source_feature.json": BASE_URL + "/items/" + spec.municipality_code + "?f=json",
        "collection_after.json": BASE_URL + "?f=json",
    }
    payloads, sources = {}, {}
    for name, url in urls.items():
        content = budget.get(url)
        payloads[name] = content
        sources[name] = {
            "url": url,
            "bytes": len(content),
            "etag": budget.last_response_headers.get("etag"),
            "last_modified": budget.last_response_headers.get("last-modified"),
        }
        if name.startswith("collection_"):
            validate_collection(spec, json.loads(content))
        elif name == "metadata.xml":
            validate_metadata(spec, content)
        else:
            if budget.last_response_headers.get("content-crs", "").strip("<>") != CRS84:
                raise ValueError("Boundary response CRS is not the expected CRS84")
            validate_feature(spec, json.loads(content))
    feature = json.loads(payloads["source_feature.json"])
    # Keep the original full geometry, but omit live service navigation from the derived GeoJSON.
    retained = {key: feature[key] for key in ("type", "id", "geometry", "properties")}
    payloads["boundary.geojson"] = json.dumps(retained, ensure_ascii=False).encode()
    metadata = validate_metadata(spec, payloads["metadata.xml"])
    manifest = {
        "contract_version": 1,
        "config": asdict(spec),
        "acquired_at_utc": datetime.now(UTC).isoformat(),
        "source_crs": CRS84,
        "metric_crs": "EPSG:3763",
        "license": "CC-BY-4.0",
        "license_url": LICENSE_URL,
        "attribution": ATTRIBUTION,
        "metadata": metadata,
        "geometry_sha256": spec.expected_geometry_sha256,
        "downloaded_bytes": budget.transferred,
        "sources": sources,
        "files": {name: sha256(content) for name, content in payloads.items()},
        **validate_feature(spec, retained),
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".staging-{uuid.uuid4().hex}"
    staging.mkdir()
    for name, content in payloads.items():
        (staging / name).write_bytes(content)
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    staging.rename(destination)
    return destination
