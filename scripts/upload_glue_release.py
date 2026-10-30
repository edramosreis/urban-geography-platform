"""Print a verified delivery plan; --execute performs an explicitly requested S3 upload."""

import argparse
import json
from pathlib import Path

from urban_geography.glue_delivery import delivery_plan, upload_release
from urban_geography.glue_sdk import create_input_client


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    plan = delivery_plan(args.release, args.release_sha256)
    if not args.execute:
        print(json.dumps({"mode": "plan_only", **plan}))
        return
    client = create_input_client()
    try:
        result = upload_release(client, args.release, args.release_sha256)
    finally:
        client.close()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
