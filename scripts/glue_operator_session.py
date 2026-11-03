"""Acquire one explicitly scoped operator snapshot; never print or persist its keys."""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import aws_role_session as roles
from aws_login_bridge import REGION, BridgeError, TemporaryCredentials
from aws_role_session import SESSION_SECONDS, STSClient
from aws_role_session import SOURCE_REQUIRED_SECONDS as SOURCE_REQUIRED_SECONDS
from glue_control import credential_admission
from prepare_glue_iam import validate_operator_principal

from urban_geography.glue_release import load_release
from urban_geography.glue_sdk import SDK_PINS


def operator_plan(release: Path, pin: str, principal_arn: str) -> dict[str, Any]:
    """Read and hash the frozen release without discovering credentials or opening clients."""
    manifest = load_release(release, pin)
    target = manifest["identity"]["target"]
    owner, deployment = target["account_id"], target["deployment_name"]
    return {
        "owner": owner,
        "source_role_arn": validate_operator_principal(principal_arn, owner, deployment),
        "role_name": deployment + "-glue-operator",
        "role_arn": f"arn:aws:iam::{owner}:role/{deployment}-glue-operator",
        "region": target["region"],
        "release_id": manifest["release_id"],
        "release_sha256": pin,
        "duration_seconds": SESSION_SECONDS,
        "required_lifetime_seconds": credential_admission()["required_lifetime_seconds"],
    }


def create_sts_client(credentials: TemporaryCredentials) -> STSClient:
    return _create_control_client(credentials, "sts")


def create_logs_client(credentials: TemporaryCredentials) -> Any:
    return _create_control_client(credentials, "logs")


def _create_control_client(credentials: TemporaryCredentials, service: str) -> Any:
    """Share the explicit snapshot/TLS/timeout policy for the two controller services."""
    if service not in {"sts", "logs"}:
        raise BridgeError("unsupported_operator_service")
    credentials.require_lifetime()
    if not all(credentials.session_options().values()):
        raise BridgeError("empty_temporary_credentials")
    try:
        for name, pin in SDK_PINS.items():
            if version(name) != pin:
                raise BridgeError("operator_session_sdk_version_mismatch")
        boto3 = import_module("boto3")
        config_class = import_module("botocore.config").Config
    except (PackageNotFoundError, ModuleNotFoundError):
        raise BridgeError("operator_session_sdk_unavailable") from None
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
        service,
        region_name=REGION,
        use_ssl=True,
        verify=True,
        endpoint_url=f"https://{service}.{REGION}.amazonaws.com",
        config=config,
    )


@contextmanager
def operator_credentials(
    release: Path, pin: str, principal_arn: str, content: bytes
) -> Iterator[TemporaryCredentials]:
    """Verify the release before acquiring one owned session for the Glue controller."""
    plan = operator_plan(release, pin, principal_arn)
    acquisition = roles.scoped_credentials(
        plan,
        content,
        create_sts_client=create_sts_client,
        session_name="glue-pilot-" + uuid.uuid4().hex,
    )
    content = b""
    with acquisition as credentials:
        yield credentials
