"""Demonstrate the retained-data processing-to-Gold path offline in a fresh output directory."""

import argparse
import json
from pathlib import Path

from urban_geography.demo import prepare_demo, run_demo


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", action="append", type=Path)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--boundary-config", type=Path)
    parser.add_argument("--boundary-root", type=Path, default=Path("data/boundaries"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = prepare_demo(
        args.config
        or [Path("configs/coimbra_pilot.json"), Path("configs/coimbra_pilot_october.json")],
        raw_root=args.raw_root,
        output=args.output,
        boundary_config=args.boundary_config,
        boundary_root=args.boundary_root,
    )
    report = run_demo(plan)
    print(
        json.dumps(
            {
                "status": report["status"],
                "report": str(plan.output / "acceptance.json"),
                "grid_rows": report["gold"]["grid_rows"],
            }
        )
    )


if __name__ == "__main__":
    main()
