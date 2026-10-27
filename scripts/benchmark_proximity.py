"""Measure the current pilot proximity operator against retained, verified reference results."""

import argparse
import json
from pathlib import Path

from urban_geography.benchmark import prepare_baseline, run_baseline, validate_trial_options


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
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--trial-timeout", type=float, default=60)
    args = parser.parse_args()
    validate_trial_options(args.trials, args.trial_timeout)
    plan = prepare_baseline(
        args.config,
        args.reference_run,
        output=args.output,
        raw_root=args.raw_root,
        boundary_config=args.boundary_config,
        boundary_root=args.boundary_root,
    )
    report = run_baseline(plan, trials=args.trials, timeout_seconds=args.trial_timeout)
    print(json.dumps({"status": report["status"], "report": str(plan.output / "report.json")}))
    if report["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
