"""Acquire one scoped fact-reader snapshot using the isolated analytics SDK pins."""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import aws_role_session as roles
from athena_read_plan import tooling_sdk_pins
from athena_runner import load_plan
from aws_login_bridge import REGION, BridgeError, TemporaryCredentials
from aws_role_session import SESSION_SECONDS, STSClient, validate_role_principal

# Conservative admission horizon for source readback, one query/watch/stop and result checks.
# This is not an end-to-end deadline or an automatic credential refresh.
READER_REQUIRED_SECONDS = 1200


def reader_plan(query_plan: Path, pin: str, principal_arn: str) -> dict[str, Any]:
    """Verify the fixed query plan before parsing credentials or constructing SDK clients."""
    prepared = load_plan(query_plan, pin)
    target = prepared["target"]
    owner, deployment = target["account_id"], target["deployment_name"]
    role = f"arn:aws:iam::{owner}:role/{deployment}-athena-reader"
    return {
        "owner": owner,
        "source_role_arn": validate_role_principal(principal_arn, owner, role),
        "role_name": deployment + "-athena-reader",
        "role_arn": role,
        "region": target["region"],
        "query_plan_sha256": pin,
        "query_sha256": prepared["query_sha256"],
        "binding": prepared["binding"],
        "duration_seconds": SESSION_SECONDS,
        "required_lifetime_seconds": READER_REQUIRED_SECONDS,
    }


def create_sts_client(credentials: TemporaryCredentials) -> STSClient:
    """Explicit keys, fixed regional TLS endpoint and one bounded SDK request attempt."""
    credentials.require_lifetime()
    if not all(credentials.session_options().values()):
        raise BridgeError("empty_temporary_credentials")
    try:
        for name, pin in tooling_sdk_pins().items():
            if version(name) != pin:
                raise BridgeError("reader_session_sdk_version_mismatch")
        boto3 = import_module("boto3")
        config_class = import_module("botocore.config").Config
    except (PackageNotFoundError, ModuleNotFoundError):
        raise BridgeError("reader_session_sdk_unavailable") from None
    config = config_class(
        region_name=REGION,
        signature_version="v4",
        connect_timeout=5,
        read_timeout=10,
        max_pool_connections=1,
        retries={"mode": "standard", "total_max_attempts": 1},
        ignore_configured_endpoint_urls=True,
        use_fips_endpoint=False,
        use_dualstack_endpoint=False,
    )
    session = boto3.Session(region_name=REGION, **credentials.session_options())
    return session.client(
        "sts",
        region_name=REGION,
        use_ssl=True,
        verify=True,
        endpoint_url=f"https://sts.{REGION}.amazonaws.com",
        config=config,
    )


@contextmanager
def reader_credentials(
    query_plan: Path, pin: str, principal_arn: str, content: bytes
) -> Iterator[TemporaryCredentials]:
    """Own/clear one verified snapshot; never submit SQL, refresh or print keys."""
    plan = reader_plan(query_plan, pin, principal_arn)
    acquisition = roles.scoped_credentials(
        plan,
        content,
        create_sts_client=create_sts_client,
        session_name="athena-reader-" + uuid.uuid4().hex,
    )
    content = b""
    with acquisition as credentials:
        yield credentials
