"""Acquire one temporary two-view builder session from a pinned prepared dbt plan."""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import aws_role_session as roles
from athena_read_plan import same
from athena_reader_session import create_sts_client as create_sts_client
from athena_runner import pinned_record
from aws_login_bridge import BridgeError, TemporaryCredentials
from aws_role_session import SESSION_SECONDS, validate_role_principal
from dbt_view_plan import view_build_plan

# Admission horizon for one source readback, the bounded two-view worker and final gates.
# Not an overall deadline, credential refresh or permission for the later full dbt trial.
BUILDER_REQUIRED_SECONDS = 1800


def load_builder_plan(project: Path, path: Path, pin: str) -> dict[str, Any]:
    prepared = pinned_record(path, pin)
    try:
        expected = view_build_plan(project, **prepared["source_plan"]["target"])
    except (KeyError, TypeError):
        raise ValueError("Malformed dbt view preparation") from None
    if not same(prepared, expected):
        raise ValueError("dbt view preparation differs from the fixed two-view recipe")
    return expected


def builder_plan(project: Path, path: Path, pin: str, principal_arn: str) -> dict[str, Any]:
    """Check project bytes and both fixed DDLs before credentials or SDK clients."""
    prepared = load_builder_plan(project, path, pin)
    target = prepared["source_plan"]["target"]
    owner, deployment = target["account_id"], target["deployment_name"]
    role = f"arn:aws:iam::{owner}:role/{deployment}-dbt-view-builder"
    return {
        "owner": owner,
        "source_role_arn": validate_role_principal(principal_arn, owner, role),
        "role_name": deployment + "-dbt-view-builder",
        "role_arn": role,
        "region": target["region"],
        "view_plan_sha256": pin,
        "project_receipt_sha256": prepared["project_receipt_sha256"],
        "binding": prepared["source_plan"]["binding"],
        "duration_seconds": SESSION_SECONDS,
        "required_lifetime_seconds": BUILDER_REQUIRED_SECONDS,
    }


@contextmanager
def builder_credentials(
    project: Path, path: Path, pin: str, principal_arn: str, content: bytes
) -> Iterator[TemporaryCredentials]:
    """Own/clear one scoped snapshot using the same pinned analytics STS factory."""
    plan = builder_plan(project, path, pin, principal_arn)
    acquisition = roles.scoped_credentials(
        plan,
        content,
        create_sts_client=create_sts_client,
        session_name="dbt-builder-" + uuid.uuid4().hex,
    )
    content = b""
    with acquisition as credentials:
        if not same(builder_plan(project, path, pin, principal_arn), plan):
            raise BridgeError("builder_preparation_changed")
        yield credentials
