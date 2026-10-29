"""Build the project wheel offline and verify its installed code and SQL resources."""

import argparse
import json
from pathlib import Path

from urban_geography.glue_package import build_package

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = build_package(PROJECT_ROOT, args.output)
    print(
        json.dumps(
            {
                "status": receipt["status"],
                "output": str(args.output),
                "wheel_sha256": receipt["wheel"]["sha256"],
            }
        )
    )


if __name__ == "__main__":
    main()
