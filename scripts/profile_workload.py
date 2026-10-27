"""Profile an already retained pilot without downloads or table writes."""

import argparse
import json
from pathlib import Path

from urban_geography.benchmark import prepare_baseline
from urban_geography.workload import run_profile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/coimbra_pilot_october.json"))
    parser.add_argument(
        "--reference-run", type=Path, default=Path("artifacts/coimbra-october-pilot")
    )
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--boundary-config", type=Path)
    parser.add_argument("--boundary-root", type=Path, default=Path("data/boundaries"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = prepare_baseline(
        args.config,
        args.reference_run,
        output=args.output,
        raw_root=args.raw_root,
        boundary_config=args.boundary_config,
        boundary_root=args.boundary_root,
    )
    report = run_profile(plan)
    print(json.dumps({"status": report["status"], "report": str(plan.output / "profile.json")}))


if __name__ == "__main__":
    main()
