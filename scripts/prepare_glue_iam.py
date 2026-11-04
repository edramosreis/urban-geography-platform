"""Export private Terraform IAM inputs from an explicitly pinned local release."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from aws_role_session import validate_role_principal as validate_role_principal

from urban_geography.glue_delivery import delivery_plan
from urban_geography.glue_input import MAX_SUMMARY_BYTES, PROJECT_ROOT
from urban_geography.glue_release import load_release


def validate_operator_principal(value: str, owner: str, deployment: str) -> str:
    return validate_role_principal(
        value, owner, f"arn:aws:iam::{owner}:role/{deployment}-glue-operator"
    )


def prepare(
    release: Path,
    pin: str,
    output: Path,
    *,
    include_job: bool = False,
    operator_principal_arn: str | None = None,
    cleanup_principal_arn: str | None = None,
) -> dict[str, Any]:
    if operator_principal_arn is not None and not include_job:
        raise ValueError("An operator requires --include-job")
    if cleanup_principal_arn is not None and not include_job:
        raise ValueError("Cleanup requires --include-job")
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
    if include_job:
        with (release / "release.json").open("rb") as source:
            content = source.read(MAX_SUMMARY_BYTES + 1)
        if len(content) > MAX_SUMMARY_BYTES or hashlib.sha256(content).hexdigest() != pin:
            raise ValueError("Release manifest changed before job export")
        result["glue_job_release_json"] = content.decode("utf-8")
    if operator_principal_arn is not None:
        result["glue_operator_principal_arn"] = validate_operator_principal(
            operator_principal_arn, target["account_id"], target["deployment_name"]
        )
    if cleanup_principal_arn is not None:
        result["trial_cleanup_principal_arn"] = validate_role_principal(
            cleanup_principal_arn,
            target["account_id"],
            f"arn:aws:iam::{target['account_id']}:role/{target['deployment_name']}-trial-cleanup",
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-job", action="store_true")
    parser.add_argument("--operator-principal-arn")
    parser.add_argument("--cleanup-principal-arn")
    args = parser.parse_args()
    result = prepare(
        args.release,
        args.release_sha256,
        args.output,
        include_job=args.include_job,
        operator_principal_arn=args.operator_principal_arn,
        cleanup_principal_arn=args.cleanup_principal_arn,
    )
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
