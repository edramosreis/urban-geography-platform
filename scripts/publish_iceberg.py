"""Publish a verified retained processing run as one local Iceberg v2 fact snapshot."""

import argparse
import json
import uuid
from pathlib import Path

from urban_geography.lakehouse import load_completed_run, publish_facts
from urban_geography.spark import create_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, default=Path("artifacts/coimbra-boundary-pilot"))
    parser.add_argument("--warehouse", type=Path, default=Path("data/lakehouse"))
    parser.add_argument("--table-name", default="building_proximity")
    parser.add_argument("--events-root", type=Path, default=Path("artifacts/iceberg-events"))
    args = parser.parse_args()
    run = load_completed_run(args.run)
    spark = create_session(args.events_root / uuid.uuid4().hex, warehouse=args.warehouse)
    try:
        receipt = publish_facts(spark, run, table_name=args.table_name)
        receipt["warehouse"] = args.warehouse.resolve().as_uri()
    finally:
        spark.stop()
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
