"""Export private Terraform IAM inputs from an explicitly pinned local release."""

import argparse
import json
from pathlib import Path
from typing import Any

from urban_geography.glue_delivery import delivery_plan
from urban_geography.glue_input import PROJECT_ROOT
from urban_geography.glue_release import load_release


def prepare(release: Path, pin: str, output: Path) -> dict[str, Any]:
    output = output.resolve()
    if output.exists() or not output.name.endswith(".tfvars.json"):
        raise ValueError("Use a fresh private .tfvars.json file")
    if output.is_relative_to(release.resolve()) or any(
        output.is_relative_to(PROJECT_ROOT / name) for name in ("data", "src", ".git")
    ):
        raise ValueError("IAM inputs must be outside release/data/source/Git")
    manifest = load_release(release, pin)
    plan = delivery_plan(release, pin)
    target = manifest["identity"]["target"]
    result = {
        "deployment_name": target["deployment_name"],
        "aws_account_id": target["account_id"],
        "aws_region": target["region"],
        "glue_release": {
            "release_id": manifest["release_id"],
            "manifest_sha256": pin,
            "summary_sha256": manifest["identity"]["summary_sha256"],
            "object_keys": [item["key"] for item in plan["objects"]],
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.release, args.release_sha256, args.output)
    print(
        json.dumps(
            {
                "status": "prepared_local_iam_inputs",
                "release_id": result["glue_release"]["release_id"],
                "object_keys": len(result["glue_release"]["object_keys"]),
            }
        )
    )


if __name__ == "__main__":
    main()
