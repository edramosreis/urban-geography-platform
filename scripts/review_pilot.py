"""Audit a retained pilot's park selection and boundary effects without fetching data."""

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pyproj import Transformer
from shapely import from_wkb
from shapely.geometry import Point, box, mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from urban_geography.config import load_config
from urban_geography.ingest import config_key, digest, validate_raw


def audit_geometry(
    points: dict[str, BaseGeometry],
    parks: dict[str, BaseGeometry],
    area: BaseGeometry,
    threshold: float,
) -> tuple[dict[str, Any], set[str]]:
    """Compare full park candidates with the counterfactual of dropping outside parks."""
    if not points:
        raise ValueError("The review requires selected buildings")
    if len(points) * len(parks) > 2_000_000:
        raise ValueError("Review exceeds the small-pilot pair budget")
    served, served_without_outside = set(), set()
    inventory = []
    for number, (park_id, polygon) in enumerate(sorted(parks.items()), 1):
        relation = (
            "inside"
            if area.covers(polygon)
            else "crosses_boundary"
            if area.intersects(polygon)
            else "outside"
        )
        qualifying = {
            building_id
            for building_id, point in points.items()
            if point.distance(polygon) <= threshold
        }
        served.update(qualifying)
        if relation != "outside":
            served_without_outside.update(qualifying)
        inventory.append(
            {
                "id": park_id,
                "label": f"P{number:02}",
                "relation": relation,
                "area_m2": polygon.area,
                "qualifying_buildings": len(qualifying),
            }
        )
    return {
        "selected_buildings": len(points),
        "matched_buildings": len(served),
        "matched_without_wholly_outside_parks": len(served_without_outside),
        "buildings_relying_on_wholly_outside_parks": len(served - served_without_outside),
        "boundary_dependent_building_ids": sorted(served - served_without_outside),
        "park_relations": dict(Counter(item["relation"] for item in inventory)),
        "parks": inventory,
    }, served


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/coimbra_pilot.json"))
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--run", type=Path, default=Path("artifacts/coimbra-pilot"))
    parser.add_argument("--references", type=Path)
    parser.add_argument("--output", type=Path, default=Path("artifacts/coimbra-coverage-review"))
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Review output already exists; choose a new directory")
    config = load_config(args.config)
    raw = args.raw_root / config.release / config.area_id / config_key(config)
    manifest = validate_raw(raw, config)
    summary = json.loads((args.run / "summary.json").read_text())
    if summary["raw_manifest_sha256"] != digest(raw / "manifest.json"):
        raise ValueError("Processing run refers to different raw inputs")
    if summary["config"] != json.loads(json.dumps(config.as_dict())):
        raise ValueError("Processing run configuration differs")
    for relative, expected in summary["files"].items():
        path = (args.run / relative).resolve()
        if not path.is_relative_to(args.run.resolve()) or digest(path) != expected:
            raise ValueError(f"Output checksum mismatch: {relative}")

    forward = Transformer.from_crs(4326, config.metric_crs, always_xy=True).transform
    area = transform(forward, box(*config.bbox))
    raw_buildings = pq.read_table(raw / "building.parquet").to_pylist()
    raw_parks = pq.read_table(raw / "land_use.parquet").to_pylist()
    if any(row.get("class") != "park" for row in raw_parks):
        raise ValueError("This review expects the retained class=park selection")
    buildings = {row["id"]: transform(forward, from_wkb(row["geometry"])) for row in raw_buildings}
    parks = {row["id"]: transform(forward, from_wkb(row["geometry"])) for row in raw_parks}
    fact_rows = pq.read_table(args.run / "building_proximity").to_pylist()
    facts = {row["building_id"]: row for row in fact_rows}
    if len(facts) != len(fact_rows) or not set(facts).issubset(buildings):
        raise ValueError("Fact keys are duplicated or absent from the retained raw data")
    points = {key: buildings[key].representative_point() for key in facts}
    audit, served = audit_geometry(points, parks, area, config.threshold_m)
    if served != {key for key, row in facts.items() if row["within_threshold"]}:
        raise ValueError("Independent geometry review disagrees with the Spark match flags")
    match_key = "buildings_within_500m" if config.threshold_m == 500 else "buildings_within"
    if audit["matched_buildings"] != summary[match_key]:
        raise ValueError("Processing summary disagrees with retained facts")
    nearest_counts = Counter(row["park_id"] for row in facts.values() if row["park_id"])
    raw_by_id = {row["id"]: row for row in raw_parks}
    for item in audit["parks"]:
        item["nearest_for_buildings"] = nearest_counts[item["id"]]
        item["sources"] = raw_by_id[item["id"]].get("sources", [])

    references = []
    if args.references:
        for site in json.loads(args.references.read_text()):
            point = transform(forward, Point(site["longitude"], site["latitude"]))
            references.append(
                {
                    **site,
                    "covering_park_ids": [
                        key for key, polygon in parks.items() if polygon.covers(point)
                    ],
                    "nearest_park_distance_m": min(
                        point.distance(polygon) for polygon in parks.values()
                    ),
                }
            )
    report = {
        "review_version": 1,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "config": config.as_dict(),
        "run_summary_sha256": digest(args.run / "summary.json"),
        "raw_manifest_sha256": digest(raw / "manifest.json"),
        "raw_buildings": manifest["extracts"]["building"]["rows"],
        "raw_buildings_excluded_from_facts": len(buildings) - len(facts),
        "spark_match_flags_verified": True,
        "source_datasets": dict(
            Counter(
                source.get("dataset", "unknown")
                for row in raw_parks
                for source in row.get("sources", [])
            )
        ),
        "references": references,
        "limitations": [
            "Only retained class=park features are reviewed; other classes were not extracted.",
            "Reference coordinates check placement, not full boundaries, completeness or access.",
            "Distances are straight-line proximity, not walking routes or population coverage.",
            "The study rectangle is not an authoritative municipality boundary.",
            "Park labels P01, P02, ... are local identifiers; names were not retained.",
            "Shapely is used for bounded QA; the production transformation remains Spark/Sedona.",
        ],
        **audit,
    }
    dependent = set(audit["boundary_dependent_building_ids"])
    plot_bounds = Transformer.from_crs(4326, config.metric_crs, always_xy=True).transform_bounds(
        *config.park_bbox(), densify_pts=21
    )
    plot_data = {
        "crs": config.metric_crs,
        "study_area": mapping(area),
        "plot_bounds": plot_bounds,
        "buildings": [
            {
                "id": key,
                "status": "outside_dependent"
                if key in dependent
                else "matched"
                if key in served
                else "unmatched"
                if key in facts
                else "excluded",
                "geometry": mapping(geometry),
                "point": list(geometry.representative_point().coords[0]),
            }
            for key, geometry in buildings.items()
        ],
        "parks": [
            {
                "id": item["id"],
                "label": item["label"],
                "relation": item["relation"],
                "geometry": mapping(parks[item["id"]]),
                "label_xy": list(
                    parks[item["id"]]
                    .intersection(box(*plot_bounds))
                    .representative_point()
                    .coords[0]
                ),
            }
            for item in audit["parks"]
        ],
        "references": [
            {**site, "xy": forward(site["longitude"], site["latitude"])} for site in references
        ],
    }
    args.output.mkdir(parents=True)
    (args.output / "coverage_review.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    (args.output / "map_geometry.json").write_text(json.dumps(plot_data), encoding="utf-8")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "selected_buildings",
                    "matched_buildings",
                    "park_relations",
                    "buildings_relying_on_wholly_outside_parks",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
