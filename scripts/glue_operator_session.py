"""Acquire one explicitly scoped operator snapshot; never print or persist its keys."""

import re
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol

from aws_login_bridge import REGION, BridgeError, TemporaryCredentials, receive, temporary
from glue_control import credential_admission
from prepare_glue_iam import validate_operator_principal

from urban_geography.glue_release import load_release
from urban_geography.glue_sdk import SDK_PINS

SESSION_SECONDS = 3600
SOURCE_REQUIRED_SECONDS = 120


class STSClient(Protocol):
    def get_caller_identity(self, **kwargs: Any) -> dict[str, Any]: ...
    def assume_role(self, **kwargs: Any) -> dict[str, Any]: ...
    def close(self) -> None: ...


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
    """Only explicit keys, the pinned SDK, regional HTTPS and one total request attempt."""
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
        "sts",
        region_name=REGION,
        use_ssl=True,
        verify=True,
        endpoint_url=f"https://sts.{REGION}.amazonaws.com",
        config=config,
    )


def verify_source_identity(identity: dict[str, Any], plan: dict[str, Any]) -> None:
    name = re.escape(plan["source_role_arn"].rsplit("/", 1)[1])
    if (
        identity.get("Account") != plan["owner"]
        or not isinstance(identity.get("Arn"), str)
        or not re.fullmatch(
            r"arn:aws:sts::"
            + plan["owner"]
            + r":assumed-role/"
            + name
            + r"/[A-Za-z0-9+=,.@_-]{2,64}",
            identity["Arn"],
        )
    ):
        raise BridgeError("operator_source_identity_differs")


def parse_assumed_credentials(
    response: dict[str, Any], plan: dict[str, Any], session_name: str
) -> tuple[TemporaryCredentials, dict[str, str]]:
    """Check the SDK response before using it to construct a second STS client."""
    user = response.get("AssumedRoleUser")
    expected = f"arn:aws:sts::{plan['owner']}:assumed-role/{plan['role_name']}/{session_name}"
    if (
        not isinstance(user, dict)
        or user.get("Arn") != expected
        or not isinstance(user.get("AssumedRoleId"), str)
        or not re.fullmatch(r"[A-Za-z0-9]{1,128}:" + session_name, user["AssumedRoleId"])
    ):
        raise BridgeError("operator_assumed_identity_differs")
    record = response.get("Credentials")
    if not isinstance(record, dict):
        raise BridgeError("operator_credentials_missing")
    required = {"AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"}
    if not required <= record.keys() or not record.keys() <= required | {"AccountId"}:
        raise BridgeError("operator_credential_fields_invalid")
    expires = record.get("Expiration")
    if not isinstance(expires, datetime):
        raise BridgeError("operator_expiration_invalid")
    offset = expires.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise BridgeError("operator_expiration_invalid")
    now = datetime.now(UTC)
    if (expires - now).total_seconds() > SESSION_SECONDS:
        raise BridgeError("operator_expiration_exceeds_requested_duration")
    # Preserve any provider AccountId so the ordinary envelope check cannot mask a mismatch.
    value = {**record, "Version": 1, "Expiration": expires.isoformat()}
    result = temporary(value, now)
    try:
        if result.owner is not None and result.owner != plan["owner"]:
            raise BridgeError("credential_account_differs")
        result.require_operation(plan["required_lifetime_seconds"], now)
        result.owner = plan["owner"]
        return result, {"Account": plan["owner"], "Arn": expected, "UserId": user["AssumedRoleId"]}
    except BaseException:
        result.clear()
        raise


@contextmanager
def operator_credentials(
    release: Path, pin: str, principal_arn: str, content: bytes
) -> Iterator[TemporaryCredentials]:
    """Close both STS clients before yielding; clear owned credentials on every exit.

    This does not launch a job, refresh a snapshot, or export credentials to stdout.
    Exceptions from the consuming controller retain their original type and meaning.
    """
    plan = operator_plan(release, pin, principal_arn)
    source, owner = receive(content)
    content = b""
    assumed = None
    response: dict[str, Any] = {}
    try:
        try:
            if owner != plan["owner"]:
                raise BridgeError("credential_account_differs")
            source.require_operation(SOURCE_REQUIRED_SECONDS)
            session_name = "glue-pilot-" + uuid.uuid4().hex
            with closing(create_sts_client(source)) as client:
                verify_source_identity(client.get_caller_identity(), plan)
                source.require_operation(SOURCE_REQUIRED_SECONDS)
                try:
                    response = client.assume_role(
                        RoleArn=plan["role_arn"],
                        RoleSessionName=session_name,
                        DurationSeconds=SESSION_SECONDS,
                    )
                    assumed, expected_identity = parse_assumed_credentials(
                        response, plan, session_name
                    )
                finally:
                    if isinstance(response, dict):
                        raw = response.pop("Credentials", None)
                        if isinstance(raw, dict):
                            raw.clear()
                        response.clear()
            source.clear()
            del client
            with closing(create_sts_client(assumed)) as client:
                identity = client.get_caller_identity()
                if any(identity.get(key) != value for key, value in expected_identity.items()):
                    raise BridgeError("operator_verified_identity_differs")
                assumed.require_operation(plan["required_lifetime_seconds"])
            del client
        except BridgeError:
            raise
        except Exception:
            raise BridgeError("operator_session_acquisition_failed") from None
        yield assumed
    finally:
        source.clear()
        if assumed is not None:
            assumed.clear()
