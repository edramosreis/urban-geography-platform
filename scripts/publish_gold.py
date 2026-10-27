"""Materialize the existing Gold grid SQL from a completed dimension publication."""

import argparse
import json
import uuid
from pathlib import Path

from urban_geography.gold_storage import publish_gold_run, validate_gold_request
from urban_geography.spark import create_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dimension-publication", type=Path, required=True)
    parser.add_argument("--warehouse", type=Path, default=Path("data/lakehouse"))
    parser.add_argument("--table-name", default="gold_grid_proximity")
    parser.add_argument("--expected-snapshot-id", type=int)
    parser.add_argument("--events-root", type=Path, default=Path("artifacts/gold-storage-events"))
    args = parser.parse_args()
    validate_gold_request(args.table_name, args.expected_snapshot_id)
    if not (args.dimension_publication / "manifest.json").is_file():
        raise ValueError("Dimension publication completion is missing")
    spark = create_session(args.events_root / uuid.uuid4().hex, warehouse=args.warehouse)
    try:
        receipt = publish_gold_run(
            spark,
            args.dimension_publication,
            table_name=args.table_name,
            expected_snapshot_id=args.expected_snapshot_id,
        )
    finally:
        spark.stop()
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
