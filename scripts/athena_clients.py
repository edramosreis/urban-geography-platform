"""Own query-control clients using one frozen credential snapshot; store no secrets."""

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from importlib import import_module
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from athena_read_plan import create_athena_client, tooling_sdk_pins

if TYPE_CHECKING:
    from aws_login_bridge import TemporaryCredentials


@dataclass
class QueryResources:
    clients: dict[str, Any] = field(repr=False)
    reader_environment: dict[str, str] = field(repr=False)


@contextmanager
def query_resources(
    *,
    session: Any = None,
    snapshot: "TemporaryCredentials | None" = None,
    include_results: bool = False,
) -> Iterator[QueryResources]:
    """Share credentials across identity/catalog/query calls and close even on partial startup."""
    if session is not None and snapshot is not None:
        raise ValueError("Choose one explicit session or temporary snapshot")
    if type(include_results) is not bool:
        raise ValueError("include_results must be an explicit boolean")
    for name, pin in tooling_sdk_pins().items():
        if version(name) != pin:
            raise ValueError("Use the isolated analytics/athena tooling environment")
    boto3 = import_module("boto3")
    if snapshot is None:
        selected = boto3.Session(region_name="eu-north-1") if session is None else session
        credentials = selected.get_credentials()
        if credentials is None:
            raise ValueError("No credentials are available for the explicitly expected caller")
        frozen = credentials.get_frozen_credentials()
        # Preserve the compatibility path's single frozen provider selection.
        options = {
            "aws_access_key_id": frozen.access_key,
            "aws_secret_access_key": frozen.secret_key,
            "aws_session_token": frozen.token,
        }
    else:
        snapshot.require_lifetime()
        options = snapshot.session_options()
        if not all(options.values()):
            options.clear()
            raise ValueError("Use a complete explicit temporary credential snapshot")
    reader_environment = {
        "AWS_ACCESS_KEY_ID": options["aws_access_key_id"],
        "AWS_SECRET_ACCESS_KEY": options["aws_secret_access_key"],
    }
    if options["aws_session_token"] is not None:
        reader_environment["AWS_SESSION_TOKEN"] = options["aws_session_token"]
    try:
        fixed = boto3.Session(region_name="eu-north-1", **options)
        with ExitStack() as owned:
            athena = create_athena_client(session=fixed)
            owned.callback(athena.close)
            clients = {"athena": athena}
            config_class = import_module("botocore.config").Config
            # SDK effective metadata drops some construction flags; make them explicit again.
            controls = athena.meta.config.merge(
                config_class(
                    ignore_configured_endpoint_urls=True,
                    use_fips_endpoint=False,
                    use_dualstack_endpoint=False,
                )
            )
            for service in ("sts", "glue"):
                client = fixed.client(
                    service,
                    region_name="eu-north-1",
                    use_ssl=True,
                    verify=True,
                    endpoint_url=f"https://{service}.eu-north-1.amazonaws.com",
                    config=controls,
                )
                owned.callback(client.close)
                clients[service] = client
            if include_results:
                config = controls.merge(
                    config_class(signature_version="s3v4", s3={"addressing_style": "virtual"})
                )
                s3 = fixed.client(
                    "s3",
                    region_name="eu-north-1",
                    use_ssl=True,
                    verify=True,
                    endpoint_url="https://s3.eu-north-1.amazonaws.com",
                    config=config,
                )
                owned.callback(s3.close)
                clients["s3"] = s3
            yield QueryResources(clients, reader_environment)
    finally:
        options.clear()
        reader_environment.clear()


@contextmanager
def query_clients(
    *,
    session: Any = None,
    snapshot: "TemporaryCredentials | None" = None,
    include_results: bool = False,
) -> Iterator[dict[str, Any]]:
    """Compatibility interface: credentials remain hidden; callers own this context."""
    with query_resources(
        session=session, snapshot=snapshot, include_results=include_results
    ) as resources:
        yield resources.clients
