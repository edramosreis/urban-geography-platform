"""Print the reviewed Glue trial plan without constructing clients or starting a job."""

import argparse
import json
from pathlib import Path

from glue_control import load_plan


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    args = parser.parse_args()
    print(json.dumps({"mode": "plan_only", **load_plan(args.release, args.release_sha256)}))


if __name__ == "__main__":
    main()
