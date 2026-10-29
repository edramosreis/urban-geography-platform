"""Render the proposed Glue startup configuration locally, without AWS or Spark."""

import argparse
import json
from pathlib import Path

from urban_geography.glue_runtime import runtime_plan

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-name", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = runtime_plan(
        deployment_name=args.deployment_name, account_id=args.account_id, region=args.region
    )
    if args.output.resolve().is_relative_to((PROJECT_ROOT / "data").resolve()):
        raise ValueError("Runtime configuration output must be outside retained data")
    if args.output.exists():
        raise ValueError("Runtime configuration output already exists; use a fresh path")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps(plan, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"status": plan["status"], "path": str(args.output)}))


if __name__ == "__main__":
    main()
