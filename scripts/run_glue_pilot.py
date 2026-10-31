"""Plan by default; --execute starts one separately approved Glue trial and monitors it."""

import argparse
import json
from pathlib import Path

from glue_control import Journal, create_glue_client, execute_trial, load_plan

from urban_geography.glue_sdk import create_input_client


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    plan = load_plan(args.release, args.release_sha256)
    if not args.execute:
        print(json.dumps({"mode": "plan_only", **plan}))
        return
    if args.output is None:
        parser.error("--execute requires a fresh --output under ignored artifacts")
    journal = Journal(args.output, args.release)
    glue = create_glue_client(region=plan["region"])
    try:
        s3 = create_input_client(region=plan["region"])
        try:
            result = execute_trial(glue, s3, plan, journal.record)
        finally:
            s3.close()
    finally:
        glue.close()
    journal.receipt(result)
    print(json.dumps(result))
    if result["outcome"] != "job_succeeded":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
