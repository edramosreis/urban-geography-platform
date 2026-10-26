"""Compare two retained processed releases under the same spatial and metric definition."""

import argparse
import json
from pathlib import Path

from urban_geography.lakehouse import load_completed_run
from urban_geography.release_comparison import compare_runs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-run", type=Path, required=True)
    parser.add_argument("--second-run", type=Path, required=True)
    parser.add_argument("--distance-tolerance-m", type=float, default=1e-4)
    parser.add_argument("--output", type=Path, default=Path("artifacts/release-comparison.json"))
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Comparison output already exists; use a new report path")
    report = compare_runs(
        load_completed_run(args.first_run),
        load_completed_run(args.second_run),
        distance_tolerance_m=args.distance_tolerance_m,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps(report, indent=2, allow_nan=False))
    print(
        json.dumps(
            {
                "status": "complete",
                "path": str(args.output),
                "first_release": report["first"]["release"],
                "second_release": report["second"]["release"],
                "common_buildings": report["common_cohort"]["buildings"],
                "changed_common_records": report["common_cohort"]["changed_result_records"],
            }
        )
    )


if __name__ == "__main__":
    main()
