"""Prepare private cleanup identity/inventory inputs; never call AWS or delete data."""

import argparse
import json
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from aws_role_session import validate_role_principal
from prepare_glue_iam import prepare as prepare_iam
from trial_cleanup_inventory import ARTIFACTS, cleanup_scope


def prepare(release: Path, pin: str, principal: str, output: Path) -> dict[str, Any]:
    scope = cleanup_scope(release, pin)
    owner = scope["target"]["account_id"]
    principal = validate_role_principal(principal, owner, scope["cleanup_role_arn"])
    destination, source = output.resolve(), release.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or destination == ARTIFACTS.resolve()
        or not destination.is_relative_to(ARTIFACTS.resolve())
        or destination.is_relative_to(source)
        or source.is_relative_to(destination)
    ):
        raise ValueError("Use fresh cleanup preparation under artifacts, separate from the release")
    output.mkdir(parents=True)
    prepare_iam(
        release,
        pin,
        output / "cleanup.private.tfvars.json",
        include_job=True,
        cleanup_principal_arn=principal,
    )
    write_once(output / "inventory-plan.json", canonical(scope))
    result = {
        "status": "prepared_local_cleanup_inputs",
        "region": scope["target"]["region"],
        "release_manifest_sha256": pin,
        "exact_release_keys": len(scope["buckets"]["lake"]["exact_keys"]),
        "aws_requests": 0,
        "execution_authorized": False,
    }
    write_once(output / "preparation.json", canonical(result))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.release, args.release_sha256, args.source_role_arn, args.output)))


if __name__ == "__main__":
    main()
