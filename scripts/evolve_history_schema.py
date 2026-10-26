"""Explicitly add processing-time lineage to a local Iceberg history table."""

import argparse
import json
import uuid
from pathlib import Path

from urban_geography.lakehouse import evolve_history_schema
from urban_geography.spark import create_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-schema-id", type=int, required=True)
    parser.add_argument("--warehouse", type=Path, default=Path("data/lakehouse"))
    parser.add_argument("--table-name", default="building_proximity_history")
    parser.add_argument("--events-root", type=Path, default=Path("artifacts/iceberg-events"))
    args = parser.parse_args()
    if args.expected_schema_id < 0:
        parser.error("--expected-schema-id must be nonnegative")
    spark = create_session(args.events_root / uuid.uuid4().hex, warehouse=args.warehouse)
    try:
        receipt = evolve_history_schema(
            spark, table_name=args.table_name, expected_schema_id=args.expected_schema_id
        )
        receipt["warehouse"] = args.warehouse.resolve().as_uri()
    finally:
        spark.stop()
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
