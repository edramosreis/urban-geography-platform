"""Inspect retained Spark stage/task balance without starting Spark or Docker."""

import argparse
import json
from pathlib import Path

from urban_geography.task_diagnostics import write_stage_report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-dir", type=Path, required=True)
    parser.add_argument(
        "--group", action="append", required=True, help="Job group; repeat as needed"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = write_stage_report(args.event_dir, args.group, args.output)
    print(
        json.dumps(
            {
                "report": str(args.output),
                "application_id": report["application_id"],
                "stages": sum(len(value["stages"]) for value in report["groups"].values()),
                "groups_without_task_events": [
                    name
                    for name, value in report["groups"].items()
                    if not value["task_events_available"]
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
