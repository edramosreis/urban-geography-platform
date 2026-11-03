"""Share verified role-session acquisition and ownership across pinned SDK environments."""

import re
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from typing import Any, Protocol

from aws_login_bridge import BridgeError, TemporaryCredentials, receive, temporary

SESSION_SECONDS = 3600
SOURCE_REQUIRED_SECONDS = 120


class STSClient(Protocol):
    def get_caller_identity(self, **kwargs: Any) -> dict[str, Any]: ...
    def assume_role(self, **kwargs: Any) -> dict[str, Any]: ...
    def close(self) -> None: ...


def validate_role_principal(value: str, owner: str, target_role_arn: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 576
        or not re.fullmatch(
            r"arn:aws:iam::" + owner + r":role/(?:[A-Za-z0-9_+=,.@-]+/)*[A-Za-z0-9_+=,.@-]{1,64}",
            value,
        )
        or value == target_role_arn
    ):
        raise ValueError(
            "Use one existing same-account IAM role ARN, excluding target-role self-trust"
        )
    return value


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
        raise BridgeError("role_source_identity_differs")


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
        raise BridgeError("role_assumed_identity_differs")
    record = response.get("Credentials")
    if not isinstance(record, dict):
        raise BridgeError("role_credentials_missing")
    required = {"AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"}
    if not required <= record.keys() or not record.keys() <= required | {"AccountId"}:
        raise BridgeError("role_credential_fields_invalid")
    expires = record.get("Expiration")
    if not isinstance(expires, datetime):
        raise BridgeError("role_expiration_invalid")
    offset = expires.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise BridgeError("role_expiration_invalid")
    now = datetime.now(UTC)
    if (expires - now).total_seconds() > SESSION_SECONDS:
        raise BridgeError("role_expiration_exceeds_requested_duration")
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
def scoped_credentials(
    plan: dict[str, Any],
    content: bytes,
    *,
    create_sts_client: Callable[[TemporaryCredentials], STSClient],
    session_name: str,
) -> Iterator[TemporaryCredentials]:
    """Close both STS clients before yielding; clear owned credentials on every exit.

    This does not launch a job, refresh a snapshot, or export credentials to stdout.
    Exceptions from the consuming controller retain their original type and meaning.
    """
    source, owner = receive(content)
    content = b""
    assumed = None
    response: dict[str, Any] = {}
    try:
        try:
            if owner != plan["owner"]:
                raise BridgeError("credential_account_differs")
            source.require_operation(SOURCE_REQUIRED_SECONDS)
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
                    raise BridgeError("role_verified_identity_differs")
                assumed.require_operation(plan["required_lifetime_seconds"])
            del client
        except BridgeError:
            raise
        except Exception:
            raise BridgeError("role_session_acquisition_failed") from None
        yield assumed
    finally:
        source.clear()
        if assumed is not None:
            assumed.clear()
