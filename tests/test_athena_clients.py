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
