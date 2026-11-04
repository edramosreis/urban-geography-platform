"""Acquire one temporary cleanup identity bound to a verified read-only inventory plan."""

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
from trial_cleanup_inventory import cleanup_scope

# Admission for at most 44 single-attempt S3 reads at 5s connect / 10s read timeouts,
# plus local verification/reserve. Not a deadline or admission for future deletion.
INVENTORY_REQUIRED_SECONDS = 900


def load_cleanup_plan(release: Path, release_pin: str, path: Path, pin: str) -> dict[str, Any]:
    prepared = pinned_record(path, pin)
    expected = cleanup_scope(release, release_pin)
    if not same(prepared, expected):
        raise ValueError("Cleanup inventory plan differs from the verified release recipe")
    return expected


def cleanup_session_plan(
    release: Path, release_pin: str, path: Path, pin: str, principal_arn: str
) -> dict[str, Any]:
    """Check the release, entire inventory recipe and source trust before credentials."""
    prepared = load_cleanup_plan(release, release_pin, path, pin)
    target = prepared["target"]
    owner, deployment = target["account_id"], target["deployment_name"]
    role = prepared["cleanup_role_arn"]
    return {
        "owner": owner,
        "source_role_arn": validate_role_principal(principal_arn, owner, role),
        "role_name": deployment + "-trial-cleanup",
        "role_arn": role,
        "region": target["region"],
        "release_manifest_sha256": release_pin,
        "inventory_plan_sha256": pin,
        "duration_seconds": SESSION_SECONDS,
        "required_lifetime_seconds": INVENTORY_REQUIRED_SECONDS,
    }


@contextmanager
def cleanup_credentials(
    release: Path,
    release_pin: str,
    path: Path,
    pin: str,
    principal_arn: str,
    content: bytes,
) -> Iterator[TemporaryCredentials]:
    """Verify both identities, close STS clients, and clear owned keys on every exit."""
    plan = cleanup_session_plan(release, release_pin, path, pin, principal_arn)
    acquisition = roles.scoped_credentials(
        plan,
        content,
        create_sts_client=create_sts_client,
        session_name="trial-cleanup-" + uuid.uuid4().hex,
    )
    content = b""
    with acquisition as credentials:
        if not same(cleanup_session_plan(release, release_pin, path, pin, principal_arn), plan):
            raise BridgeError("cleanup_preparation_changed")
        yield credentials
