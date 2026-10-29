"""Prepare a verified local retained-input folder; no Spark startup or AWS calls."""

import argparse
import json
from pathlib import Path

from urban_geography.glue_input import stage_input


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--summary-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = stage_input(args.run, args.summary_sha256, args.output)
    print(
        json.dumps(
            {
                "status": receipt["status"],
                "source_run_sha256": receipt["source_run_sha256"],
                "payload_bytes": receipt["payload_bytes"],
                "files": len(receipt["files"]),
                "output": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()
