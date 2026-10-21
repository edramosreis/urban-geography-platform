"""Independent Shapely/PROJ validation of a retained pilot run (no network)."""

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq
from pyproj import Transformer
from shapely import from_wkb
from shapely.geometry import box
from shapely.ops import transform

from urban_geography.config import load_config
from urban_geography.ingest import config_key, validate_raw


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/coimbra_pilot.json"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/coimbra-pilot"))
    parser.add_argument("--compare-output", type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    raw = Path("data/raw") / config.release / config.area_id / config_key(config)
    validate_raw(raw, config)
    forward = Transformer.from_crs(4326, 3763, always_xy=True).transform
    reverse = Transformer.from_crs(3763, 4326, always_xy=True).transform
    area = box(*config.bbox)
    buildings = {}
    for row in pq.read_table(raw / "building.parquet").to_pylist():
        point = transform(forward, from_wkb(row["geometry"])).representative_point()
        if area.covers(transform(reverse, point)):
            buildings[row["id"]] = point
    parks = {
        row["id"]: transform(forward, from_wkb(row["geometry"]))
        for row in pq.read_table(raw / "land_use.parquet").to_pylist()
    }
    facts = pq.read_table(args.output / "building_proximity").to_pylist()
    assert len(facts) == len(buildings)
    assert {row["building_id"] for row in facts} == set(buildings)
    max_error = 0.0
    matched = 0
    for row in facts:
        point = buildings[row["building_id"]]
        candidates = sorted(
            (point.distance(park), key)
            for key, park in parks.items()
            if point.distance(park) <= config.threshold_m
        )
        assert row["within_threshold"] == bool(candidates), row["building_id"]
        if candidates:
            distance, park_id = candidates[0]
            assert row["park_id"] == park_id, row["building_id"]
            error = abs(row["distance_m"] - distance)
            assert error <= 1e-4, (row["building_id"], error)
            max_error = max(max_error, error)
            matched += 1
        else:
            assert row["distance_m"] is None and row["park_id"] is None
    grids = pq.read_table(args.output / "grid_proximity").to_pylist()
    assert sum(row["mapped_buildings"] for row in grids) == len(buildings)
    assert sum(row["buildings_within"] for row in grids) == matched
    result = {
        "oracle": "Shapely + PROJ",
        "rows_checked": len(facts),
        "matched": matched,
        "distance_tolerance_m": 1e-4,
        "max_distance_error_m": max_error,
        "grid_totals_match": True,
    }
    if args.compare_output:
        for dataset in ("building_proximity", "grid_proximity"):
            first = pq.read_table(args.output / dataset).to_pylist()
            second = pq.read_table(args.compare_output / dataset).to_pylist()
            canonical_first = sorted(json.dumps(row, sort_keys=True) for row in first)
            canonical_second = sorted(json.dumps(row, sort_keys=True) for row in second)
            assert canonical_first == canonical_second, f"Replay mismatch: {dataset}"
        result["replay_logically_identical"] = True
    (args.output / "independent_validation.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
