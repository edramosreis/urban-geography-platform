"""Bind the pinned Athena adapter through its connection/session factory in one worker."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


@contextmanager
def bound_adapter(session: Any, profile: dict[str, Any]) -> Iterator[None]:
    from dbt.adapters import athena
    from dbt.adapters.athena.connections import AthenaConnection
    from dbt.adapters.base import AdapterPlugin
    from dbt.adapters.contracts.connection import ConnectionState
    from dbt.adapters.factory import FACTORY

    if FACTORY.adapters or "athena" in FACTORY.plugins:
        raise RuntimeError("Use a fresh, dedicated dbt worker")
    original = athena.Plugin

    class BoundConnections(athena.AthenaConnectionManager):
        @classmethod
        def open(cls, connection: Any) -> Any:
            if connection.state == "open":
                return connection
            credentials = connection.credentials
            for name in ("database", "schema", "region_name", "work_group", "s3_staging_dir"):
                if getattr(credentials, name) != profile[name]:
                    raise ValueError("dbt connection target differs: " + name)
            for name in (
                "aws_profile_name",
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
                "assume_role_arn",
                "endpoint_url",
            ):
                if getattr(credentials, name):
                    raise ValueError("Unexpected dbt credential/endpoint mode")
            if credentials.connection_manager != "api":
                raise ValueError("Use the pinned API connection manager")
            connection.handle = AthenaConnection(
                credentials, boto_session_factory=lambda _: session
            ).connect()
            connection.state = ConnectionState.OPEN
            return connection

    class BoundAthena(athena.AthenaAdapter):
        ConnectionManager = BoundConnections

        @classmethod
        def get_adapter_run_info(cls, config: Any) -> Any:
            # dbt 1.12 derives a module name from the subclass name here.
            # Preserve the stock Athena tracking identity and model details.
            return original.adapter.get_adapter_run_info(config)

    athena.Plugin = AdapterPlugin(
        adapter=BoundAthena,
        credentials=original.credentials,
        include_path=original.include_path,
        dependencies=original.dependencies,
        project_name=original.project_name,
    )
    try:
        yield
    finally:
        try:
            FACTORY.reset_adapters()
        finally:
            athena.Plugin = original
            FACTORY.plugins.pop("athena", None)
