"""Run the separately approved retained-cohort two-executor acceptance only."""

import argparse
import json
from pathlib import Path

from urban_geography.benchmark import prepare_baseline
from urban_geography.cluster import run_two_executor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = prepare_baseline(
        Path("configs/coimbra_performance_trial.json"),
        Path("artifacts/coimbra-performance-trial-20261028/reference"),
        output=args.output,
        boundary_config=Path("configs/coimbra_boundary.json"),
    )
    report = run_two_executor(plan)
    print(json.dumps({"status": report["status"], "report": str(plan.output / "report.json")}))
    if report["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
