"""Local-only scope for inspecting named infrastructure after the dedicated trial teardown."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from athena_control import canonical
from aws_role_session import validate_role_principal
from trial_cleanup_inventory import cleanup_scope

MAX_CALLS = 18
MAX_PAGES = 2
PAGE_SIZE = 50
OBSERVATION_SECONDS = 300
MAX_RECORD_BYTES = 64 * 1024
SERVICES = ("sts", "s3", "glue", "athena", "iam", "logs")


def plan_sha256(plan: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(plan)).hexdigest()


def absence_plan(release: Path, pin: str, principal: str) -> dict[str, Any]:
    scope = cleanup_scope(release, pin)
    target = scope["target"]
    owner, deployment = target["account_id"], target["deployment_name"]
    roles = [
        ("glue-writer", "service-role/"),
        ("glue-operator", ""),
        ("athena-reader", ""),
        ("dbt-view-builder", ""),
        ("trial-cleanup", ""),
    ]
    role_targets = [
        {
            "kind": "role",
            "name": deployment + "-" + suffix,
            "arn": f"arn:aws:iam::{owner}:role/{path}{deployment}-{suffix}",
        }
        for suffix, path in roles
    ]
    principal = validate_role_principal(principal, owner, scope["cleanup_role_arn"])
    if principal.rsplit("/", 1)[1] in {r["name"] for r in role_targets}:
        raise ValueError("Post-teardown inspection needs a source role outside the trial resources")
    targets = [
        *[
            {"kind": "bucket", "name": scope["buckets"][kind]["name"]}
            for kind in ("lake", "results")
        ],
        {"kind": "database", "name": scope["expected_services"]["database"]},
        {"kind": "job", "name": scope["expected_services"]["job"]},
        {"kind": "workgroup", "name": scope["expected_services"]["workgroup"]},
        *role_targets,
        *[
            {
                "kind": "policy",
                "name": deployment + "-" + suffix,
                "arn": f"arn:aws:iam::{owner}:policy/{deployment}-{suffix}",
            }
            for suffix in (
                "release-delivery",
                "job-deployment",
                "athena-reader",
                "dbt-view-builder",
            )
        ],
        *[
            {"kind": "log_group", "name": f"/urban-geography/{deployment}/glue/{kind}"}
            for kind in ("error", "output")
        ],
    ]
    assert len(targets) == 16
    return {
        "version": 1,
        "status": "prepared_named_infrastructure_absence",
        "release_manifest_sha256": pin,
        "target": target,
        "source_role_arn": principal,
        "targets": targets,
        "read_only": True,
        "deletion_authorized": False,
        "limits": {
            "max_calls": MAX_CALLS,
            "max_pages_per_listing": MAX_PAGES,
            "page_size": PAGE_SIZE,
            "observation_seconds": OBSERVATION_SECONDS,
            "max_record_bytes": MAX_RECORD_BYTES,
        },
        "scope": "explicit_resource_names_only",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    args = parser.parse_args()
    plan = absence_plan(args.release, args.release_sha256, args.source_role_arn)
    print(
        json.dumps(
            {
                "mode": "plan_only",
                "plan_sha256": plan_sha256(plan),
                "region": plan["target"]["region"],
                "targets": len(plan["targets"]),
                "credentials_read": False,
                "aws_requests": 0,
                "output_created": False,
            }
        )
    )


if __name__ == "__main__":
    main()
