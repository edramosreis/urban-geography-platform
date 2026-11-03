"""Fetch a pinned S3 input and run the writer in an approved startup-configured Glue job."""

import argparse
import json
import re
from pathlib import Path

from pyspark.sql import SparkSession

from urban_geography.glue_runtime import runtime_plan
from urban_geography.glue_s3_bootstrap import run_s3_writer
from urban_geography.glue_sdk import create_input_client
from urban_geography.glue_writer import run_writer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--summary-sha256", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--deployment-name", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--region", default="eu-north-1")
    parser.add_argument("--table-name", default="building_proximity")
    parser.add_argument("--scratch-root", type=Path, default=Path("/tmp"))
    args, _ = parser.parse_known_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.table_name):
        raise ValueError("Writer table_name must be a safe lowercase identifier")
    recipe = runtime_plan(
        deployment_name=args.deployment_name, account_id=args.account_id, region=args.region
    )
    result = run_s3_writer(
        deployment_name=args.deployment_name,
        account_id=args.account_id,
        expected_summary_sha256=args.summary_sha256,
        expected_manifest_sha256=args.manifest_sha256,
        scratch_root=args.scratch_root,
        region=args.region,
        client_factory=lambda: create_input_client(region=args.region),
        write=lambda prepared: run_writer(
            prepared,
            args.summary_sha256,
            catalog_name=recipe["catalog"],
            namespace=recipe["database"],
            table_name=args.table_name,
            expected_settings=recipe["spark_settings"],
            session_factory=lambda: SparkSession.builder.getOrCreate(),
        ),
    )
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
