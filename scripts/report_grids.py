"""Write an offline Gold SQL report from one retained local Iceberg history snapshot."""

import argparse
import json
import uuid
from pathlib import Path

from urban_geography.gold import grid_report, validate_report_request
from urban_geography.spark import create_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, default=Path("data/lakehouse"))
    parser.add_argument("--table-name", default="building_proximity_history")
    parser.add_argument("--snapshot-id", type=int)
    parser.add_argument("--output", type=Path, default=Path("artifacts/gold-grid-report.json"))
    parser.add_argument("--events-root", type=Path, default=Path("artifacts/gold-events"))
    args = parser.parse_args()
    validate_report_request(args.table_name, args.snapshot_id)
    if args.output.exists():
        raise ValueError("Gold report output already exists; use a new path")
    spark = create_session(args.events_root / uuid.uuid4().hex, warehouse=args.warehouse)
    try:
        report = grid_report(spark, table_name=args.table_name, snapshot_id=args.snapshot_id)
    finally:
        spark.stop()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps(report, indent=2, allow_nan=False))
    print(
        json.dumps(
            {
                "status": "complete",
                "path": str(args.output),
                "snapshot_id": report["snapshot_id"],
                "grid_rows": len(report["grids"]),
            }
        )
    )


if __name__ == "__main__":
    main()
