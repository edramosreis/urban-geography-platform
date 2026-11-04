"""Connect a verified temporary cleanup session to read-only S3 inventory; no live CLI."""

import hashlib
from contextlib import ExitStack
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from athena_read_plan import same, tooling_sdk_pins
from aws_login_bridge import REGION, BridgeError, TemporaryCredentials
from aws_role_session import LifetimeClient
from trial_cleanup_inventory import MAX_REPORT_BYTES, inventory_s3, validate_inventory_output
from trial_cleanup_session import (
    INVENTORY_REQUIRED_SECONDS,
    cleanup_credentials,
    cleanup_session_plan,
    load_cleanup_plan,
)
from trial_service_inventory import ATHENA_READ_CALLS, GLUE_READ_CALLS, inventory_services
from trial_service_inventory import MAX_REPORT_BYTES as MAX_SERVICE_REPORT_BYTES

# Combined maximum: 44 S3 + 29 service reads; admission includes local verification/reserve.
SERVICES_REQUIRED_SECONDS = 1500
READ_SECONDS = 90
READ_CALLS = {
    name: READ_SECONDS
    for name in (
        "get_bucket_location",
        "get_bucket_versioning",
        "list_object_versions",
        "list_objects_v2",
        "list_multipart_uploads",
    )
}


def create_s3_client(snapshot: TemporaryCredentials) -> Any:
    return _create_inventory_client(snapshot, "s3")


def create_service_client(snapshot: TemporaryCredentials, service: str) -> Any:
    if service not in {"glue", "athena"}:
        raise BridgeError("unsupported_inventory_service")
    return _create_inventory_client(snapshot, service)


def _create_inventory_client(snapshot: TemporaryCredentials, service: str) -> Any:
    """Explicit keys, fixed regional TLS endpoint and one SDK attempt; caller closes."""
    snapshot.require_operation(READ_SECONDS)
    options = snapshot.session_options()
    try:
        if not all(options.values()):
            raise BridgeError("empty_temporary_credentials")
        try:
            for name, pin in tooling_sdk_pins().items():
                if version(name) != pin:
                    raise BridgeError("cleanup_inventory_sdk_version_mismatch")
            boto3 = import_module("boto3")
            config_class = import_module("botocore.config").Config
        except (PackageNotFoundError, ModuleNotFoundError):
            raise BridgeError("cleanup_inventory_sdk_unavailable") from None
        config = config_class(
            region_name=REGION,
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
        session = boto3.Session(region_name=REGION, **options)
        return session.client(
            service,
            region_name=REGION,
            use_ssl=True,
            verify=True,
            endpoint_url=f"https://{service}.{REGION}.amazonaws.com",
            config=config,
        )
    finally:
        options.clear()


def prepare_operation(
    release: Path, release_pin: str, path: Path, pin: str, principal_arn: str, output: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify local scope/trust/output before credentials, SDK clients or output creation."""
    prepared = load_cleanup_plan(release, release_pin, path, pin)
    identity = cleanup_session_plan(release, release_pin, path, pin, principal_arn)
    validate_inventory_output(release, output)
    destination, protected = output.resolve(), path.parent.resolve()
    if destination.is_relative_to(protected) or protected.is_relative_to(destination):
        raise ValueError("Keep inventory output separate from its prepared plan directory")
    return prepared, identity


def _saved_pin(path: Path, report: dict[str, Any], limit: int) -> str:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Inventory completion requires a regular saved report")
    with path.open("rb") as stream:
        content = stream.read(limit + 1)
    if len(content) > limit or content != canonical(report):
        raise ValueError("Saved inventory differs before scoped completion")
    return hashlib.sha256(content).hexdigest()


def run_inventory(
    release: Path,
    release_pin: str,
    path: Path,
    pin: str,
    principal_arn: str,
    content: bytes,
    *,
    output: Path,
    include_services: bool = False,
) -> dict[str, Any]:
    """Read only; record outer completion after clients close and owned keys clear."""
    prepared, identity = prepare_operation(release, release_pin, path, pin, principal_arn, output)
    if type(include_services) is not bool:
        raise ValueError("include_services must be an explicit boolean")
    acquisition = cleanup_credentials(release, release_pin, path, pin, principal_arn, content)
    content = b""
    service_report = None
    intent: dict[str, Any] = {}
    with acquisition as snapshot:
        snapshot.require_operation(
            SERVICES_REQUIRED_SECONDS if include_services else INVENTORY_REQUIRED_SECONDS
        )
        with ExitStack() as owned:
            client = create_s3_client(snapshot)
            owned.callback(client.close)
            if include_services:
                clients = {}
                for service, calls in (("glue", GLUE_READ_CALLS), ("athena", ATHENA_READ_CALLS)):
                    raw = create_service_client(snapshot, service)
                    owned.callback(raw.close)
                    clients[service] = LifetimeClient(
                        raw, snapshot, {name: READ_SECONDS for name in calls}
                    )
                output.mkdir(parents=True)
                intent = {
                    "version": 1,
                    "include_services": True,
                    "release_manifest_sha256": release_pin,
                    "inventory_plan_sha256": pin,
                    "required_lifetime_seconds": SERVICES_REQUIRED_SECONDS,
                    "read_only": True,
                    "deletion_authorized": False,
                }
                write_once(output / "operation-intent.json", canonical(intent))
                service_report = inventory_services(
                    clients["glue"], clients["athena"], release, release_pin, output / "services"
                )
            s3_output = output / "s3" if include_services else output
            report = inventory_s3(
                LifetimeClient(client, snapshot, READ_CALLS), release, release_pin, s3_output
            )
            snapshot.require_operation(READ_SECONDS)
            if not same(load_cleanup_plan(release, release_pin, path, pin), prepared):
                raise BridgeError("cleanup_preparation_changed")
    completion = {
        "version": 1,
        "status": "scoped_s3_inventory_complete",
        "release_manifest_sha256": release_pin,
        "inventory_plan_sha256": pin,
        "cleanup_role_arn": identity["role_arn"],
        "inventory_sha256": _saved_pin(s3_output / "inventory.json", report, MAX_REPORT_BYTES),
        "inventory_status": report["status"],
        "all_keys_in_cleanup_scope": report["all_keys_in_cleanup_scope"],
        "quiescence_and_catalog_observed": False,
        "deletion_authorized": False,
    }
    if service_report is not None:
        completion.update(
            status="scoped_trial_inventory_complete",
            include_services=True,
            operation_intent_sha256=_saved_pin(output / "operation-intent.json", intent, 64 * 1024),
            service_inventory_sha256=_saved_pin(
                output / "services/inventory.json", service_report, MAX_SERVICE_REPORT_BYTES
            ),
            service_inventory_status=service_report["status"],
            requires_review=report["status"] == "s3_inventory_requires_review"
            or service_report["status"] == "service_inventory_requires_review",
        )
    write_once(output / "session-complete.json", canonical(completion))
    return completion
