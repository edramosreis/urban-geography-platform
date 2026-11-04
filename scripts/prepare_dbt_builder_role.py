"""Prepare private two-view dbt builder role inputs from one explicitly pinned frozen release."""

import argparse
import json
from pathlib import Path
from typing import Any

from aws_role_session import validate_role_principal

from urban_geography.glue_input import PROJECT_ROOT
from urban_geography.glue_release import load_release


def prepare(release: Path, pin: str, principal_arn: str, output: Path) -> dict[str, Any]:
    destination = output.resolve()
    source = release.resolve()
    if (
        output.is_symlink()
        or output.exists()
        or not output.name.endswith(".tfvars.json")
        or not destination.is_relative_to(PROJECT_ROOT / "artifacts")
        or destination.is_relative_to(source)
        or source.is_relative_to(destination)
    ):
        raise ValueError(
            "Use a fresh private .tfvars.json under artifacts, separate from the release"
        )
    manifest = load_release(release, pin)
    target = manifest["identity"]["target"]
    role = f"arn:aws:iam::{target['account_id']}:role/{target['deployment_name']}-dbt-view-builder"
    principal = validate_role_principal(principal_arn, target["account_id"], role)
    result = {
        "aws_account_id": target["account_id"],
        "aws_region": target["region"],
        "deployment_name": target["deployment_name"],
        "enable_dbt_view_builder_policy": True,
        "dbt_view_builder_principal_arn": principal,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.release, args.release_sha256, args.source_role_arn, args.output)
    print(
        json.dumps(
            {
                "status": "prepared_local_builder_role_inputs",
                "region": result["aws_region"],
                "aws_calls": 0,
                "execution_authorized": False,
            }
        )
    )


if __name__ == "__main__":
    main()
