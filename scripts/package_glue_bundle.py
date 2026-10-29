"""Assemble and check a local Glue zip of wheels using only retained verified inputs."""

import argparse
import json
from pathlib import Path

from urban_geography.glue_bundle import build_bundle

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel-cache", type=Path, required=True)
    parser.add_argument("--project-package", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = build_bundle(PROJECT_ROOT, args.wheel_cache, args.project_package, args.output)
    print(
        json.dumps(
            {
                "status": receipt["status"],
                "bundle": receipt["bundle"]["filename"],
                "sha256": receipt["bundle"]["sha256"],
                "bytes": receipt["bundle"]["bytes"],
            }
        )
    )


if __name__ == "__main__":
    main()
