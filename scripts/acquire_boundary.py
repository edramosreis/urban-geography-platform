"""Retain one pinned official municipality boundary without expanding pilot data."""

import argparse
import json
from pathlib import Path

from urban_geography.boundary import acquire_boundary, load_boundary_spec


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/coimbra_boundary.json"))
    parser.add_argument("--output-root", type=Path, default=Path("data/boundaries"))
    args = parser.parse_args()
    path = acquire_boundary(load_boundary_spec(args.config), args.output_root)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "path": str(path),
                "release": manifest["config"]["source_release"],
                "municipality": manifest["config"]["municipality_name"],
                "area_km2": manifest["area_m2"] / 1e6,
                "downloaded_bytes_at_acquisition": manifest["downloaded_bytes"],
            }
        )
    )


if __name__ == "__main__":
    main()
