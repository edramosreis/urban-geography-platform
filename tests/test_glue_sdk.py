"""Managed SDK version and configuration checks without installing or contacting AWS."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import urban_geography.glue_sdk as sdk


@pytest.fixture
def modules(monkeypatch):
    selected = SimpleNamespace(client=Mock(return_value=SimpleNamespace(close=Mock())))
    config = Mock(side_effect=lambda **kwargs: SimpleNamespace(**kwargs))
    boto = SimpleNamespace(Session=Mock(return_value=selected))
    monkeypatch.setattr(sdk, "version", lambda name: sdk.SDK_PINS[name])
    monkeypatch.setattr(
        sdk,
        "import_module",
        lambda name: boto if name == "boto3" else SimpleNamespace(Config=config),
    )
    return SimpleNamespace(session=selected, config=config, boto=boto)


def test_factory_uses_explicit_region_tls_timeouts_and_no_retries(modules):
    result = sdk.create_input_client()
    modules.boto.Session.assert_called_once_with(region_name="eu-north-1")
    args, options = modules.session.client.call_args
    assert args == ("s3",)
    assert options["region_name"] == "eu-north-1" and options["use_ssl"] and options["verify"]
    config = options["config"]
    assert config.connect_timeout == 5 and config.read_timeout == 10
    assert config.retries == {"mode": "standard", "total_max_attempts": 1}
    assert config.max_pool_connections == 1 and config.signature_version == "s3v4"
    assert config.s3 == {"addressing_style": "virtual"} and config.ignore_configured_endpoint_urls
    assert not config.use_fips_endpoint and not config.use_dualstack_endpoint
    assert result is modules.session.client.return_value


def test_factory_accepts_a_supplied_sdk_session_for_offline_contract_checks(modules):
    other = SimpleNamespace(client=Mock())
    sdk.create_input_client(session=other)
    modules.boto.Session.assert_not_called()
    other.client.assert_called_once()


@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_bad_region_never_imports_or_creates_an_sdk_client(monkeypatch, region):
    monkeypatch.setattr(sdk, "version", lambda *a: pytest.fail("No SDK resolution"))
    with pytest.raises(ValueError, match="scoped"):
        sdk.create_input_client(region=region)


@pytest.mark.parametrize("package", ["boto3", "botocore"])
def test_version_drift_fails_before_client_creation(modules, monkeypatch, package):
    monkeypatch.setattr(
        sdk, "version", lambda name: "wrong" if name == package else sdk.SDK_PINS[name]
    )
    with pytest.raises(ValueError, match=package):
        sdk.create_input_client()
    modules.session.client.assert_not_called()


def test_missing_managed_sdk_is_reported_without_fallback(modules, monkeypatch):
    def missing(name):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(sdk, "import_module", missing)
    with pytest.raises(RuntimeError, match="Glue 5.1 provided"):
        sdk.create_input_client()
    modules.session.client.assert_not_called()
