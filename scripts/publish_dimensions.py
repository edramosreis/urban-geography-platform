"""Publish and verify release/study-area dimensions for a retained Iceberg fact snapshot."""

import argparse
import json
import uuid
from pathlib import Path

from urban_geography.dimension_publication import publish_dimensions, validate_publication_request
from urban_geography.dimensions import build_dimension_contract
from urban_geography.lakehouse import load_completed_run
from urban_geography.spark import create_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, action="append", required=True)
    parser.add_argument("--warehouse", type=Path, default=Path("data/lakehouse"))
    parser.add_argument(
        "--publication-root", type=Path, default=Path("data/dimension-publications")
    )
    parser.add_argument("--table-prefix", default="dim")
    parser.add_argument("--fact-table-name", default="building_proximity_history")
    parser.add_argument("--fact-snapshot-id", type=int)
    parser.add_argument("--events-root", type=Path, default=Path("artifacts/dimension-events"))
    args = parser.parse_args()
    validate_publication_request(args.table_prefix, args.fact_table_name, args.fact_snapshot_id)
    runs = [load_completed_run(path) for path in args.run]
    build_dimension_contract(runs)
    spark = create_session(args.events_root / uuid.uuid4().hex, warehouse=args.warehouse)
    try:
        receipt = publish_dimensions(
            spark,
            runs,
            publication_root=args.publication_root,
            table_prefix=args.table_prefix,
            fact_table_name=args.fact_table_name,
            fact_snapshot_id=args.fact_snapshot_id,
        )
    finally:
        spark.stop()
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
