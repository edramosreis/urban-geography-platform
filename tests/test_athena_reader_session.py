"""Reader-only identity acquisition with synthetic credentials and no SDK network calls."""

import hashlib
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
for name in (
    "aws_login_bridge",
    "aws_role_session",
    "prepare_dbt_project",
    "athena_read_plan",
    "athena_control",
    "athena_preflight",
    "athena_results",
    "athena_runner",
    "athena_reader_session",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
session = sys.modules["athena_reader_session"]
bridge = sys.modules["aws_login_bridge"]
roles = sys.modules["aws_role_session"]
OWNER = "123456789012"
PRINCIPAL = f"arn:aws:iam::{OWNER}:role/managed/project/login"
ROLE = "urban-geo-test-athena-reader"
NAME = "athena-reader-" + "a" * 32


def packet(seconds=900, owner=OWNER):
    value = bridge.temporary(
        {
            "Version": 1,
            "AccessKeyId": "ASIA" + "A" * 16,
            "SecretAccessKey": "s" * 40,
            "SessionToken": "dummyLoginToken",
            "Expiration": (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(),
            "AccountId": owner,
        }
    )
    try:
        return bridge.envelope(value, owner)
    finally:
        value.clear()


@pytest.fixture
def case(tmp_path, monkeypatch):
    prepared = sys.modules["athena_read_plan"].read_plan(
        deployment_name="urban-geo-test",
        account_id=OWNER,
        snapshot_id=123,
        source_run_sha256="a" * 64,
    )
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(prepared), encoding="utf-8")
    pin = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(session.uuid, "uuid4", lambda: SimpleNamespace(hex="a" * 32))
    source = {
        "Account": OWNER,
        "Arn": f"arn:aws:sts::{OWNER}:assumed-role/login/browser",
        "UserId": "AROA" + "B" * 17 + ":browser",
    }
    target = {
        "Account": OWNER,
        "Arn": f"arn:aws:sts::{OWNER}:assumed-role/{ROLE}/{NAME}",
        "UserId": "AROA" + "C" * 17 + ":" + NAME,
    }
    raw = {
        "AccessKeyId": "ASIA" + "B" * 16,
        "SecretAccessKey": "t" * 40,
        "SessionToken": "dummyReaderToken",
        "Expiration": datetime.now(UTC) + timedelta(seconds=3500),
    }
    response = {
        "Credentials": raw,
        "AssumedRoleUser": {"Arn": target["Arn"], "AssumedRoleId": target["UserId"]},
    }
    clients = [
        SimpleNamespace(
            get_caller_identity=Mock(return_value=identity),
            assume_role=Mock(return_value=response),
            close=Mock(),
        )
        for identity in (source, target)
    ]
    credentials = []

    def create(value):
        credentials.append(value)
        return clients[len(credentials) - 1]

    factory = Mock(side_effect=create)
    monkeypatch.setattr(session, "create_sts_client", factory)
    return SimpleNamespace(
        path=path,
        pin=pin,
        prepared=prepared,
        source=source,
        target=target,
        raw=raw,
        response=response,
        clients=clients,
        credentials=credentials,
        factory=factory,
    )


def acquire(case, content=None):
    return session.reader_credentials(
        case.path, case.pin, PRINCIPAL, packet() if content is None else content
    )


def test_reader_plan_is_bound_to_the_pinned_fixed_query_and_reader(case):
    assert session.reader_plan(case.path, case.pin, PRINCIPAL) == {
        "owner": OWNER,
        "source_role_arn": PRINCIPAL,
        "role_name": ROLE,
        "role_arn": f"arn:aws:iam::{OWNER}:role/{ROLE}",
        "region": "eu-north-1",
        "query_plan_sha256": case.pin,
        "query_sha256": case.prepared["query_sha256"],
        "binding": case.prepared["binding"],
        "duration_seconds": 3600,
        "required_lifetime_seconds": 1200,
    }
    case.factory.assert_not_called()


@pytest.mark.parametrize("defect", ["pin", "bytes", "recipe"])
def test_bad_query_plan_is_rejected_before_receiving_credentials(case, monkeypatch, defect):
    pin = case.pin
    if defect == "pin":
        pin = "0" * 64
    elif defect == "bytes":
        case.path.write_bytes(b"changed")
    else:
        case.prepared["request_template"]["QueryString"] = "SELECT 1"
        case.path.write_text(json.dumps(case.prepared), encoding="utf-8")
        pin = hashlib.sha256(case.path.read_bytes()).hexdigest()
    receive = Mock(side_effect=AssertionError("credentials must not be parsed"))
    monkeypatch.setattr(roles, "receive", receive)
    with pytest.raises(ValueError):
        with session.reader_credentials(case.path, pin, PRINCIPAL, b"unused"):
            pytest.fail("must not yield")
    receive.assert_not_called()
    case.factory.assert_not_called()


@pytest.mark.parametrize(
    "principal",
    [
        "arn:aws:iam::999999999999:role/login",
        "arn:aws:iam::123456789012:role/urban-geo-test-athena-reader",
        "arn:aws:sts::123456789012:assumed-role/login/browser",
    ],
)
def test_bad_trust_never_opens_a_client(case, principal):
    with pytest.raises(ValueError, match="same-account IAM role"):
        with session.reader_credentials(case.path, case.pin, principal, packet()):
            pytest.fail("must not yield")
    case.factory.assert_not_called()


def test_exact_reader_request_has_verified_identity_and_owned_cleanup(case, capsys):
    with acquire(case) as credentials:
        credentials.require_operation(1200)
        assert credentials.owner == OWNER and credentials.token == "dummyReaderToken"
        assert case.credentials[0].token == ""
        assert case.raw == {} and case.response == {}
        for client in case.clients:
            client.get_caller_identity.assert_called_once_with()
            client.close.assert_called_once_with()
        case.clients[0].assume_role.assert_called_once_with(
            RoleArn=f"arn:aws:iam::{OWNER}:role/{ROLE}",
            RoleSessionName=NAME,
            DurationSeconds=3600,
        )
        case.clients[1].assume_role.assert_not_called()
    assert all(
        value.session_options()
        == {"aws_access_key_id": "", "aws_secret_access_key": "", "aws_session_token": ""}
        for value in case.credentials
    )
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "defect",
    [
        "short_source",
        "source_account",
        "source_role",
        "returned_role",
        "short_return",
        "target_identity",
    ],
)
def test_reader_rejects_wrong_identity_or_short_lifetime(case, defect):
    content = packet(seconds=119) if defect == "short_source" else packet()
    if defect == "source_account":
        case.source["Account"] = "999999999999"
    elif defect == "source_role":
        case.source["Arn"] = f"arn:aws:sts::{OWNER}:assumed-role/unapproved/browser"
    elif defect == "returned_role":
        case.response["AssumedRoleUser"]["Arn"] = (
            f"arn:aws:sts::{OWNER}:assumed-role/urban-geo-test-glue-operator/{NAME}"
        )
    elif defect == "short_return":
        case.raw["Expiration"] = datetime.now(UTC) + timedelta(seconds=1199)
    elif defect == "target_identity":
        case.target["UserId"] = "unapproved"
    with pytest.raises(bridge.BridgeError):
        with acquire(case, content):
            pytest.fail("must not yield")
    assert all(value.token == "" for value in case.credentials)
    expected = 0 if defect == "short_source" else 2 if defect == "target_identity" else 1
    assert len(case.credentials) == expected
    for client in case.clients[:expected]:
        client.close.assert_called_once_with()
    if defect in {"short_source", "source_account", "source_role"}:
        case.clients[0].assume_role.assert_not_called()


def test_denied_assumption_is_sanitized_and_never_retried(case, capsys):
    case.clients[0].assume_role.side_effect = RuntimeError("dummyLoginToken")
    with pytest.raises(bridge.BridgeError, match="^role_session_acquisition_failed$"):
        with acquire(case):
            pytest.fail("must not yield")
    case.clients[0].assume_role.assert_called_once()
    case.clients[0].close.assert_called_once()
    assert len(case.credentials) == 1 and case.credentials[0].token == ""
    assert capsys.readouterr() == ("", "")


def test_consumer_error_remains_available_for_query_recovery(case):
    failure = RuntimeError("query recovery required")
    with pytest.raises(RuntimeError) as caught:
        with acquire(case):
            raise failure
    assert caught.value is failure
    assert all(value.token == "" for value in case.credentials)


@pytest.mark.parametrize("defect", ["cleared", "mismatch", "missing"])
def test_factory_has_no_fallback_when_credentials_or_sdk_are_unavailable(monkeypatch, defect):
    credentials, _ = bridge.receive(packet())
    imported = Mock(side_effect=AssertionError("must not load SDK"))
    monkeypatch.setattr(session, "import_module", imported)
    if defect == "cleared":
        credentials.clear()
        reason = "empty_temporary_credentials"
    elif defect == "mismatch":
        monkeypatch.setattr(session, "version", lambda name: "unreviewed")
        reason = "reader_session_sdk_version_mismatch"
    else:
        monkeypatch.setattr(session, "version", Mock(side_effect=PackageNotFoundError()))
        reason = "reader_session_sdk_unavailable"
    try:
        with pytest.raises(bridge.BridgeError, match=reason):
            session.create_sts_client(credentials)
    finally:
        credentials.clear()
    imported.assert_not_called()
