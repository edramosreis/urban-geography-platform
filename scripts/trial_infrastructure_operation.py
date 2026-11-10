"""Owned read-only source-session connection for inspection after trial roles are removed."""

import time
from collections.abc import Callable
from contextlib import ExitStack
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from athena_control import canonical
from athena_read_plan import tooling_sdk_pins
from athena_runner import pinned_record
from aws_login_bridge import BridgeError, TemporaryCredentials, read_record, receive
from aws_role_session import LifetimeClient
from trial_cleanup_inventory import _save, validate_inventory_output
from trial_infrastructure_absence import observe_absence
from trial_infrastructure_plan import MAX_RECORD_BYTES, SERVICES, absence_plan, plan_sha256

REQUIRED_SECONDS = 600
READ_SECONDS = 90
CALLS = {
    "sts": ("get_caller_identity",),
    "s3": ("list_objects_v2",),
    "glue": ("get_database", "get_job"),
    "athena": ("list_work_groups",),
    "iam": ("get_role", "get_policy"),
    "logs": ("describe_log_groups",),
}


def create_client(snapshot: TemporaryCredentials, service: str) -> Any:
    if service not in SERVICES:
        raise BridgeError("unsupported_infrastructure_service")
    snapshot.require_operation(READ_SECONDS)
    options = snapshot.session_options()
    try:
        if not all(options.values()):
            raise BridgeError("empty_temporary_credentials")
        try:
            for name, pin in tooling_sdk_pins().items():
                if version(name) != pin:
                    raise BridgeError("infrastructure_sdk_version_differs")
            boto3 = import_module("boto3")
            config_class = import_module("botocore.config").Config
        except (PackageNotFoundError, ModuleNotFoundError):
            raise BridgeError("infrastructure_sdk_unavailable") from None
        region = "us-east-1" if service == "iam" else "eu-north-1"
        endpoint = (
            "https://iam.amazonaws.com"
            if service == "iam"
            else f"https://{service}.{region}.amazonaws.com"
        )
        config = config_class(
            region_name=region,
            signature_version="s3v4" if service == "s3" else "v4",
            connect_timeout=5,
            read_timeout=10,
            max_pool_connections=1,
            retries={"mode": "standard", "total_max_attempts": 1},
            ignore_configured_endpoint_urls=True,
            use_fips_endpoint=False,
            use_dualstack_endpoint=False,
            **({"s3": {"addressing_style": "virtual"}} if service == "s3" else {}),
        )
        session = boto3.Session(region_name=region, **options)
        return session.client(
            service,
            region_name=region,
            endpoint_url=endpoint,
            config=config,
            use_ssl=True,
            verify=True,
        )
    finally:
        options.clear()


def prepare_operation(
    release: Path, pin: str, principal: str, expected_plan_sha256: str, output: Path
) -> dict[str, Any]:
    plan = absence_plan(release, pin, principal)
    if expected_plan_sha256 != plan_sha256(plan):
        raise ValueError("Use the reviewed named-infrastructure plan SHA256")
    validate_inventory_output(release, output)
    return plan


def run_absence(
    release: Path,
    pin: str,
    principal: str,
    expected_plan_sha256: str,
    content: bytes,
    output: Path,
    *,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """No role assumption: the outside observer must survive the trial's own teardown."""
    plan = prepare_operation(release, pin, principal, expected_plan_sha256, output)
    snapshot, owner = receive(content)
    content = b""
    try:
        if owner != plan["target"]["account_id"]:
            raise BridgeError("credential_account_differs")
        snapshot.require_operation(REQUIRED_SECONDS)
        with ExitStack() as owned:
            clients = {}
            for service in SERVICES:
                raw = create_client(snapshot, service)
                owned.callback(raw.close)
                clients[service] = LifetimeClient(
                    raw, snapshot, {name: READ_SECONDS for name in CALLS[service]}
                )
            report = observe_absence(
                clients,
                release,
                pin,
                principal,
                expected_plan_sha256,
                output / "inspection",
                now=now,
            )
            snapshot.require_operation(READ_SECONDS)
    finally:
        snapshot.clear()
    pinned_record(release / "release.json", pin)
    saved, inventory_pin = read_record(output / "inspection/inventory.json")
    if canonical(saved) != canonical(report):
        raise BridgeError("infrastructure_saved_report_differs")
    # All owned clients closed and the owned source snapshot cleared before this record.
    completion = {
        "version": 1,
        "status": "source_session_infrastructure_inspection_complete",
        "plan_sha256": expected_plan_sha256,
        "inventory_sha256": inventory_pin,
        "named_infrastructure_absence_observed": report["named_infrastructure_absence_observed"],
        "requires_review": report["requires_review"],
        "read_only": True,
        "deletion_performed": False,
    }
    _save(output / "session-complete.json", completion, MAX_RECORD_BYTES)
    return completion
