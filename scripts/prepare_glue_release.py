"""Freeze a local Glue delivery inventory; no AWS calls or uploads."""

import argparse
import json
from pathlib import Path

from urban_geography.glue_release import freeze_release


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--summary-sha256", required=True)
    parser.add_argument("--jar", type=Path, action="append", required=True)
    parser.add_argument("--deployment-name", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = freeze_release(
        Path(__file__).resolve().parents[1],
        bundle=args.bundle,
        prepared_input=args.input,
        expected_summary_sha256=args.summary_sha256,
        jars=args.jar,
        output=args.output,
        target={
            "deployment_name": args.deployment_name,
            "account_id": args.account_id,
            "region": args.region,
        },
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
