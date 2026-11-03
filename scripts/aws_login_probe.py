"""Receive credentials only on stdin; own pinned clients for one STS identity lookup."""

import argparse
import json
import re
import sys
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

from aws_login_bridge import MAX_BYTES, REGION, BridgeError, TemporaryCredentials, receive, reply


@contextmanager
def owned_clients(credentials: TemporaryCredentials, sdk: str) -> Iterator[dict[str, Any]]:
    import boto3

    session = boto3.Session(region_name=REGION, **credentials.session_options())
    with ExitStack() as stack:
        if sdk == "glue":
            from glue_control import create_glue_client

            from urban_geography.glue_sdk import create_input_client

            s3 = create_input_client(session=session)
            stack.callback(s3.close)
            glue = create_glue_client(session=session)
            stack.callback(glue.close)
            sts = session.client(
                "sts",
                region_name=REGION,
                use_ssl=True,
                verify=True,
                endpoint_url=f"https://sts.{REGION}.amazonaws.com",
                config=glue.meta.config,
            )
            stack.callback(sts.close)
            yield {"s3": s3, "glue": glue, "sts": sts}
        elif sdk == "analytics":
            from athena_clients import query_clients

            yield stack.enter_context(query_clients(session=session, include_results=True))
        else:
            raise BridgeError("unsupported_sdk_environment")


def probe(content: bytes, sdk: str, *, offline: bool = False) -> dict[str, Any]:
    if sdk not in {"glue", "analytics"}:
        raise BridgeError("unsupported_sdk_environment")
    credentials, owner = receive(content)
    try:
        with owned_clients(credentials, sdk) as clients:
            with ExitStack() as stack:
                if offline:
                    from botocore.stub import Stubber

                    stub = stack.enter_context(Stubber(clients["sts"]))
                    stub.add_response(
                        "get_caller_identity",
                        {
                            "Account": owner,
                            "Arn": f"arn:aws:sts::{owner}:assumed-role/login-bridge/offline",
                            "UserId": "AROA" + "A" * 17 + ":offline",
                        },
                        {},
                    )
                identity = clients["sts"].get_caller_identity()
                if (
                    identity.get("Account") != owner
                    or not isinstance(identity.get("Arn"), str)
                    or not re.fullmatch(
                        r"arn:aws:sts::"
                        + owner
                        + r":assumed-role/[A-Za-z0-9+=,.@_/-]+/[A-Za-z0-9+=,.@_-]+",
                        identity["Arn"],
                    )
                ):
                    raise BridgeError("identity_differs_from_expected_project_role")
                if offline:
                    stub.assert_no_pending_responses()
            credentials.require_lifetime()
        return reply(sdk, offline)
    finally:
        credentials.clear()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--sdk", choices=("glue", "analytics"), required=True)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    try:
        content = sys.stdin.buffer.read(MAX_BYTES + 1)
        print(json.dumps(probe(content, args.sdk, offline=args.offline)))
    except Exception:
        print(json.dumps({"status": "failed", "error": "login_probe_failed"}))
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
