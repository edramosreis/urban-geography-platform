"""Small retained raw geometry cases shared by processing and full-path acceptance tests."""

import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import Transformer
from shapely.geometry import box
from shapely.ops import transform

from urban_geography.config import RunConfig
from urban_geography.ingest import config_key, digest


@dataclass(frozen=True)
class ProcessingCase:
    config: RunConfig
    config_path: Path
    raw_root: Path
    raw: Path
    output: Path


def make_processing_case(
    root: Path, *, release: str = "2026-09-23.1", raw_root: Path | None = None
) -> ProcessingCase:
    root.mkdir(parents=True, exist_ok=True)
    config = RunConfig("offline_processing_test", release, (-8.435, 40.195, -8.415, 40.215))
    config_path = root / "config.json"
    config_path.write_text(json.dumps(config.as_dict()), encoding="utf-8")
    raw_root = raw_root if raw_root is not None else root / "raw"
    raw = raw_root / config.release / config.area_id / config_key(config)
    raw.mkdir(parents=True)
    x, y = Transformer.from_crs(4326, 3763, always_xy=True).transform(-8.425, 40.205)
    to_wgs84 = Transformer.from_crs(3763, 4326, always_xy=True).transform
    footprints = {
        "near": box(x - 505, y - 5, x - 495, y + 5),
        "far": box(x + 495, y - 5, x + 505, y + 5),
        "outside": box(x + 1995, y - 5, x + 2005, y + 5),
    }
    buildings = [
        {"id": key, "geometry": bytes(transform(to_wgs84, geom).wkb)}
        for key, geom in footprints.items()
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
    return ProcessingCase(config, config_path, raw_root, raw, root / "published")
