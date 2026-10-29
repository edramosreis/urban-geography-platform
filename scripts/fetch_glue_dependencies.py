"""Explicitly download the frozen Glue-target Python wheels to a fresh local cache."""

import argparse
import json
from pathlib import Path

from urban_geography.glue_dependencies import fetch_dependencies

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = fetch_dependencies(PROJECT_ROOT, args.output)
    print(
        json.dumps(
            {
                "status": receipt["status"],
                "wheels": len(receipt["wheels"]),
                "bytes": sum(item["bytes"] for item in receipt["wheels"]),
            }
        )
    )


if __name__ == "__main__":
    main()
