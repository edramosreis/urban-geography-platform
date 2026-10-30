"""Create the Glue 5.1 provided S3 SDK client with an explicit per-client policy."""

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from typing import Any

SDK_PINS = {"boto3": "1.40.61", "botocore": "1.40.61"}


def create_input_client(*, region: str = "eu-west-1", session: Any = None) -> Any:
    """Use the managed SDK; import nothing optional until this factory is called."""
    if region != "eu-west-1":
        raise ValueError("The input SDK client is scoped to eu-west-1")
    try:
        for name, pin in SDK_PINS.items():
            if version(name) != pin:
                raise ValueError(f"Use the Glue 5.1 provided {name}=={pin}")
        boto3 = import_module("boto3")
        config_class = import_module("botocore.config").Config
    except (PackageNotFoundError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            "This factory requires the Glue 5.1 provided boto3/botocore SDK"
        ) from exc
    config = config_class(
        region_name=region,
        signature_version="s3v4",
        connect_timeout=5,
        read_timeout=10,
        max_pool_connections=1,
        retries={"mode": "standard", "total_max_attempts": 1},
        s3={"addressing_style": "virtual"},
        ignore_configured_endpoint_urls=True,
        use_fips_endpoint=False,
        use_dualstack_endpoint=False,
    )
    selected = boto3.Session(region_name=region) if session is None else session
    return selected.client("s3", region_name=region, use_ssl=True, verify=True, config=config)
