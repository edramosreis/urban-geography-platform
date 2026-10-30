"""Publish a prepared local input using the startup-configured Glue Spark session."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from urban_geography.glue_runtime import runtime_plan
from urban_geography.glue_writer import run_writer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--summary-sha256", required=True)
    parser.add_argument("--deployment-name", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--table-name", default="building_proximity")
    # Glue supplies platform arguments alongside the writer arguments.
    args, _ = parser.parse_known_args()
    recipe = runtime_plan(
        deployment_name=args.deployment_name, account_id=args.account_id, region=args.region
    )
    result = run_writer(
        args.input,
        args.summary_sha256,
        catalog_name=recipe["catalog"],
        namespace=recipe["database"],
        table_name=args.table_name,
        expected_settings=recipe["spark_settings"],
        session_factory=lambda: SparkSession.builder.getOrCreate(),
    )
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
