"""One in-memory credential snapshot and reliable client cleanup; no credential discovery."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from athena_preflight_fixtures import CLIENTS as clients


@pytest.fixture
def case(monkeypatch):
    selected = Mock()
    frozen = SimpleNamespace(
        access_key="offline-testing", secret_key="offline-testing", token="offline-token"
    )
    selected.get_credentials.return_value.get_frozen_credentials.return_value = frozen
    fixed = Mock()
    session_class = Mock(return_value=fixed)
    monkeypatch.setattr(
        clients, "import_module", Mock(return_value=SimpleNamespace(Session=session_class))
    )
    monkeypatch.setattr(clients, "version", lambda name: clients.tooling_sdk_pins()[name])
    athena, sts, glue = Mock(), Mock(), Mock()
    fixed.client.side_effect = [sts, glue]
    monkeypatch.setattr(clients, "create_athena_client", Mock(return_value=athena))
    return selected, fixed, session_class, athena, sts, glue


def test_clients_share_frozen_identity_and_policy_and_close_without_logging_secrets(case):
    selected, fixed, session_class, athena, sts, glue = case
    with clients.query_clients(session=selected) as opened:
        assert opened == {"athena": athena, "sts": sts, "glue": glue}
        selected.get_credentials.assert_called_once()
        selected.get_credentials.return_value.get_frozen_credentials.assert_called_once()
        session_class.assert_called_once_with(
            aws_access_key_id="offline-testing",
            aws_secret_access_key="offline-testing",
            aws_session_token="offline-token",
            region_name="eu-west-1",
        )
        clients.create_athena_client.assert_called_once_with(session=fixed)
        for call, service in zip(fixed.client.call_args_list, ("sts", "glue"), strict=True):
            assert call.args == (service,)
            assert call.kwargs == {
                "region_name": "eu-west-1",
                "use_ssl": True,
                "verify": True,
                "endpoint_url": f"https://{service}.eu-west-1.amazonaws.com",
                "config": athena.meta.config,
            }
        for client in opened.values():
            client.close.assert_not_called()
    for client in (athena, sts, glue):
        client.close.assert_called_once()


@pytest.mark.parametrize("stage", ["sts", "glue", "body"])
def test_failure_at_each_stage_closes_every_created_client(case, stage):
    selected, fixed, session_class, athena, sts, glue = case
    if stage == "sts":
        fixed.client.side_effect = RuntimeError("creation failed")
    elif stage == "glue":
        fixed.client.side_effect = [sts, RuntimeError("creation failed")]
    with pytest.raises(RuntimeError):
        with clients.query_clients(session=selected):
            raise RuntimeError("body failed")
    athena.close.assert_called_once()
    if stage != "sts":
        sts.close.assert_called_once()
    if stage == "body":
        glue.close.assert_called_once()


@pytest.mark.parametrize("package", ["boto3", "botocore"])
def test_wrong_sdk_fails_before_credential_discovery(case, monkeypatch, package):
    selected = case[0]
    monkeypatch.setattr(
        clients,
        "version",
        lambda name: "wrong" if name == package else clients.tooling_sdk_pins()[name],
    )
    with pytest.raises(ValueError, match="tooling"):
        with clients.query_clients(session=selected):
            pytest.fail("No clients expected")
    selected.get_credentials.assert_not_called()


def test_missing_credentials_prevent_client_creation(case):
    selected, fixed, session_class, athena, sts, glue = case
    selected.get_credentials.return_value = None
    with pytest.raises(ValueError, match="credentials"):
        with clients.query_clients(session=selected):
            pytest.fail("No clients expected")
    session_class.assert_not_called()
    clients.create_athena_client.assert_not_called()


def test_optional_result_client_shares_credentials_and_uses_s3_policy(case, monkeypatch):
    selected, fixed, session_class, athena, sts, glue = case
    s3 = Mock()
    fixed.client.side_effect = [sts, glue, s3]
    importer = clients.import_module
    config_class = Mock(return_value="s3-overrides")
    monkeypatch.setattr(
        clients,
        "import_module",
        lambda name: SimpleNamespace(Config=config_class)
        if name == "botocore.config"
        else importer(name),
    )
    with clients.query_clients(session=selected, include_results=True) as opened:
        assert opened["s3"] is s3
        config_class.assert_called_once_with(
            signature_version="s3v4", s3={"addressing_style": "virtual"}
        )
        athena.meta.config.merge.assert_called_once_with("s3-overrides")
        fixed.client.assert_called_with(
            "s3",
            region_name="eu-west-1",
            use_ssl=True,
            verify=True,
            endpoint_url="https://s3.eu-west-1.amazonaws.com",
            config=athena.meta.config.merge.return_value,
        )
    for client in (athena, sts, glue, s3):
        client.close.assert_called_once()


def test_optional_result_client_creation_failure_closes_existing_clients(case, monkeypatch):
    selected, fixed, session_class, athena, sts, glue = case
    fixed.client.side_effect = [sts, glue, RuntimeError("S3 creation failed")]
    importer = clients.import_module
    monkeypatch.setattr(
        clients,
        "import_module",
        lambda name: SimpleNamespace(Config=Mock())
        if name == "botocore.config"
        else importer(name),
    )
    with pytest.raises(RuntimeError, match="S3 creation"):
        with clients.query_clients(session=selected, include_results=True):
            pytest.fail("No complete client set")
    for client in (athena, sts, glue):
        client.close.assert_called_once()


def test_result_client_opt_in_requires_a_boolean_before_credential_discovery(case):
    selected = case[0]
    with pytest.raises(ValueError, match="boolean"):
        with clients.query_clients(session=selected, include_results=1):
            pytest.fail("No clients")
    selected.get_credentials.assert_not_called()


def test_reader_credentials_are_explicit_hidden_and_cleared_on_exit(case):
    selected = case[0]
    with clients.query_resources(session=selected) as resources:
        environment = resources.reader_environment
        assert environment == {
            "AWS_ACCESS_KEY_ID": "offline-testing",
            "AWS_SECRET_ACCESS_KEY": "offline-testing",
            "AWS_SESSION_TOKEN": "offline-token",
        }
        assert "offline-testing" not in repr(resources) and "offline-token" not in repr(resources)
    assert environment == {}


def test_reader_credential_mapping_is_cleared_after_body_failure(case):
    with pytest.raises(RuntimeError):
        with clients.query_resources(session=case[0]) as resources:
            environment = resources.reader_environment
            raise RuntimeError("reader failed")
    assert environment == {}
