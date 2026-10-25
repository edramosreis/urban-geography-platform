"""Publish comparable release partitions, with explicit guarded replacement, to local Iceberg."""

import argparse
import json
import uuid
from pathlib import Path

from urban_geography.lakehouse import load_completed_run, publish_release
from urban_geography.spark import create_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, default=Path("artifacts/coimbra-boundary-pilot"))
    parser.add_argument("--warehouse", type=Path, default=Path("data/lakehouse"))
    parser.add_argument("--table-name", default="building_proximity_history")
    parser.add_argument("--events-root", type=Path, default=Path("artifacts/iceberg-events"))
    parser.add_argument(
        "--replace", action="store_true", help="Replace only the input run's release/area"
    )
    parser.add_argument("--expected-snapshot-id", type=int)
    args = parser.parse_args()
    if args.replace and args.expected_snapshot_id is None:
        parser.error("--replace requires --expected-snapshot-id")
    if args.expected_snapshot_id is not None and args.expected_snapshot_id <= 0:
        parser.error("--expected-snapshot-id must be positive")
    run = load_completed_run(args.run)
    spark = create_session(args.events_root / uuid.uuid4().hex, warehouse=args.warehouse)
    try:
        receipt = publish_release(
            spark,
            run,
            table_name=args.table_name,
            replace=args.replace,
            expected_snapshot_id=args.expected_snapshot_id,
        )
        receipt["warehouse"] = args.warehouse.resolve().as_uri()
    finally:
        spark.stop()
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
