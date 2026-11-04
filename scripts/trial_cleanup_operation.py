"""Connect a verified temporary cleanup session to read-only S3 inventory; no live CLI."""

import hashlib
from contextlib import closing
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
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=10,
            max_pool_connections=1,
            retries={"mode": "standard", "total_max_attempts": 1},
            ignore_configured_endpoint_urls=True,
            use_fips_endpoint=False,
            use_dualstack_endpoint=False,
            s3={"addressing_style": "virtual"},
        )
        session = boto3.Session(region_name=REGION, **options)
        return session.client(
            "s3",
            region_name=REGION,
            use_ssl=True,
            verify=True,
            endpoint_url=f"https://s3.{REGION}.amazonaws.com",
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


def run_inventory(
    release: Path,
    release_pin: str,
    path: Path,
    pin: str,
    principal_arn: str,
    content: bytes,
    *,
    output: Path,
) -> dict[str, Any]:
    """Read only; record outer completion after clients close and owned keys clear."""
    prepared, identity = prepare_operation(release, release_pin, path, pin, principal_arn, output)
    acquisition = cleanup_credentials(release, release_pin, path, pin, principal_arn, content)
    content = b""
    with acquisition as snapshot:
        snapshot.require_operation(INVENTORY_REQUIRED_SECONDS)
        with closing(create_s3_client(snapshot)) as client:
            report = inventory_s3(
                LifetimeClient(client, snapshot, READ_CALLS), release, release_pin, output
            )
            snapshot.require_operation(READ_SECONDS)
            if not same(load_cleanup_plan(release, release_pin, path, pin), prepared):
                raise BridgeError("cleanup_preparation_changed")
    inventory_path = output / "inventory.json"
    if inventory_path.is_symlink() or not inventory_path.is_file():
        raise ValueError("Inventory completion requires a regular saved report")
    with inventory_path.open("rb") as stream:
        content = stream.read(MAX_REPORT_BYTES + 1)
    if len(content) > MAX_REPORT_BYTES or content != canonical(report):
        raise ValueError("Saved inventory differs before scoped completion")
    completion = {
        "version": 1,
        "status": "scoped_s3_inventory_complete",
        "release_manifest_sha256": release_pin,
        "inventory_plan_sha256": pin,
        "cleanup_role_arn": identity["role_arn"],
        "inventory_sha256": hashlib.sha256(content).hexdigest(),
        "inventory_status": report["status"],
        "all_keys_in_cleanup_scope": report["all_keys_in_cleanup_scope"],
        "quiescence_and_catalog_observed": False,
        "deletion_authorized": False,
    }
    write_once(output / "session-complete.json", canonical(completion))
    return completion
