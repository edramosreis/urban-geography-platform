"""Explicit STS acquisition, identity checks and credential ownership without AWS."""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
for name in ("aws_login_bridge", "glue_control", "prepare_glue_iam", "glue_operator_session"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
session = sys.modules["glue_operator_session"]
bridge = sys.modules["aws_login_bridge"]
OWNER = "123456789012"
PRINCIPAL = f"arn:aws:iam::{OWNER}:role/managed/project/login"
NAME = "glue-pilot-" + "a" * 32
KEY = "ASIA" + "A" * 16
SECRET = "s" * 40
TOKEN = "dummySessionToken"
ROLE = "urban-geo-test-glue-operator"


def packet(seconds=900, owner=OWNER):
    credentials = bridge.temporary(
        {
            "Version": 1,
            "AccessKeyId": KEY,
            "SecretAccessKey": SECRET,
            "SessionToken": TOKEN,
            "Expiration": (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(),
            "AccountId": owner,
        }
    )
    try:
        return bridge.envelope(credentials, owner)
    finally:
        credentials.clear()


@pytest.fixture
def case(frozen, monkeypatch):
    monkeypatch.setattr(session.uuid, "uuid4", lambda: SimpleNamespace(hex="a" * 32))
    source_identity = {
        "Account": OWNER,
        "Arn": f"arn:aws:sts::{OWNER}:assumed-role/login/browser",
        "UserId": "AROA" + "B" * 17 + ":browser",
    }
    target_identity = {
        "Account": OWNER,
        "Arn": f"arn:aws:sts::{OWNER}:assumed-role/{ROLE}/{NAME}",
        "UserId": "AROA" + "C" * 17 + ":" + NAME,
    }
    raw = {
        "AccessKeyId": "ASIA" + "B" * 16,
        "SecretAccessKey": "t" * 40,
        "SessionToken": "dummyOperatorToken",
        "Expiration": datetime.now(UTC) + timedelta(seconds=3500),
    }
    response = {
        "Credentials": raw,
        "AssumedRoleUser": {
            "Arn": target_identity["Arn"],
            "AssumedRoleId": target_identity["UserId"],
        },
    }
    clients = [
        SimpleNamespace(
            get_caller_identity=Mock(return_value=identity),
            assume_role=Mock(return_value=response),
            close=Mock(),
        )
        for identity in (source_identity, target_identity)
    ]
    credentials = []
    parsed = []
    original_parse = session.temporary

    def parse(value, now=None):
        result = original_parse(value, now)
        parsed.append(result)
        return result

    monkeypatch.setattr(session, "temporary", parse)

    def create(value):
        credentials.append(value)
        return clients[len(credentials) - 1]

    factory = Mock(side_effect=create)
    monkeypatch.setattr(session, "create_sts_client", factory)
    return SimpleNamespace(
        frozen=frozen,
        clients=clients,
        response=response,
        raw=raw,
        credentials=credentials,
        source_identity=source_identity,
        target_identity=target_identity,
        factory=factory,
        parsed=parsed,
    )


def acquire(case, content=None):
    return session.operator_credentials(
        case.frozen.path, case.frozen.pin, PRINCIPAL, packet() if content is None else content
    )


def test_plan_is_release_bound_and_opens_no_clients(case):
    plan = session.operator_plan(case.frozen.path, case.frozen.pin, PRINCIPAL)
    assert plan == {
        "owner": OWNER,
        "source_role_arn": PRINCIPAL,
        "role_name": ROLE,
        "role_arn": f"arn:aws:iam::{OWNER}:role/{ROLE}",
        "region": "eu-north-1",
        "release_id": case.frozen.manifest["release_id"],
        "release_sha256": case.frozen.pin,
        "duration_seconds": 3600,
        "required_lifetime_seconds": 1200,
    }
    case.factory.assert_not_called()


def test_bad_release_is_rejected_without_credential_or_sdk_work(case, monkeypatch):
    parse = Mock(side_effect=AssertionError("must not receive credentials"))
    monkeypatch.setattr(session, "receive", parse)
    with pytest.raises(ValueError):
        with session.operator_credentials(case.frozen.path, "0" * 64, PRINCIPAL, b"unused"):
            pytest.fail("must not yield")
    parse.assert_not_called()
    case.factory.assert_not_called()


def test_success_uses_one_exact_request_and_closes_clients_before_consumption(case, capsys):
    with acquire(case) as credentials:
        assert credentials.owner == OWNER
        credentials.require_operation(1200)
        assert credentials.access_key == "ASIA" + "B" * 16
        assert case.credentials[0].token == ""
        assert case.raw == {} and case.response == {}
        for client in case.clients:
            client.close.assert_called_once_with()
            client.get_caller_identity.assert_called_once_with()
        case.clients[0].assume_role.assert_called_once_with(
            RoleArn=f"arn:aws:iam::{OWNER}:role/{ROLE}",
            RoleSessionName=NAME,
            DurationSeconds=3600,
        )
        case.clients[1].assume_role.assert_not_called()
    assert all(
        value.token == value.access_key == value.secret_key == "" for value in case.credentials
    )
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("content", ["wrong_account", "too_short", "invalid", "oversized"])
def test_bad_source_snapshot_stops_before_clients(case, content):
    data = {
        "wrong_account": lambda: packet(owner="999999999999"),
        "too_short": lambda: packet(seconds=119),
        "invalid": lambda: b"not json",
        "oversized": lambda: b"x" * (32768 + 1),
    }[content]()
    with pytest.raises(bridge.BridgeError):
        with acquire(case, data):
            pytest.fail("must not yield")
    case.factory.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        {"Account": "999999999999"},
        {"Arn": f"arn:aws:sts::{OWNER}:assumed-role/unapproved/browser"},
        {"Arn": f"arn:aws:iam::{OWNER}:root"},
    ],
)
def test_wrong_source_identity_never_attempts_assumption(case, change):
    case.source_identity.update(change)
    with pytest.raises(bridge.BridgeError, match="source_identity_differs"):
        with acquire(case):
            pytest.fail("must not yield")
    case.clients[0].assume_role.assert_not_called()
    case.clients[0].close.assert_called_once()
    assert len(case.credentials) == 1 and case.credentials[0].token == ""


def test_source_lifetime_is_rechecked_after_identity(case):
    def expire():
        case.credentials[0].expiration = datetime.now(UTC) + timedelta(seconds=119)
        return case.source_identity

    case.clients[0].get_caller_identity.side_effect = expire
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        with acquire(case):
            pytest.fail("must not yield")
    case.clients[0].assume_role.assert_not_called()
    case.clients[0].close.assert_called_once()
    assert case.credentials[0].token == ""


@pytest.mark.parametrize(
    "defect",
    [
        "role",
        "session",
        "user_id",
        "missing_credentials",
        "naive",
        "offset",
        "short",
        "expired",
        "too_long",
        "long_term",
        "missing_token",
        "account",
        "extra",
        "unexpected_version",
    ],
)
def test_bad_assume_response_never_builds_the_target_client(case, defect):
    if defect in {"role", "session"}:
        key = "unapproved" if defect == "role" else "unrequested-session"
        case.response["AssumedRoleUser"]["Arn"] = (
            f"arn:aws:sts::{OWNER}:assumed-role/{key}/{NAME}"
            if defect == "role"
            else f"arn:aws:sts::{OWNER}:assumed-role/{ROLE}/{key}"
        )
    elif defect == "user_id":
        case.response["AssumedRoleUser"]["AssumedRoleId"] = "AROAwrong:other"
    elif defect == "missing_credentials":
        case.response.pop("Credentials")
    elif defect == "naive":
        case.raw["Expiration"] = datetime.now()
    elif defect == "offset":
        case.raw["Expiration"] = datetime.now(timezone(timedelta(hours=1))) + timedelta(hours=1)
    elif defect in {"short", "expired", "too_long"}:
        seconds = {"short": 1199, "expired": -1, "too_long": 3700}[defect]
        case.raw["Expiration"] = datetime.now(UTC) + timedelta(seconds=seconds)
    elif defect == "long_term":
        case.raw["AccessKeyId"] = "AKIA" + "B" * 16
    elif defect == "missing_token":
        case.raw.pop("SessionToken")
    elif defect == "account":
        case.raw["AccountId"] = "999999999999"
    elif defect == "extra":
        case.raw["Unexpected"] = "value"
    elif defect == "unexpected_version":
        case.raw["Version"] = 2
    with pytest.raises(bridge.BridgeError):
        with acquire(case):
            pytest.fail("must not yield")
    case.clients[0].assume_role.assert_called_once()
    case.clients[0].close.assert_called_once()
    assert len(case.credentials) == 1 and case.credentials[0].token == ""
    assert all(value.token == "" for value in case.parsed)
    assert case.response == {}
    if defect != "missing_credentials":
        assert case.raw == {}


@pytest.mark.parametrize("field", ["Account", "Arn", "UserId"])
def test_target_identity_must_match_both_requested_and_returned_identity(case, field):
    case.target_identity[field] = "unapproved"
    with pytest.raises(bridge.BridgeError, match="verified_identity_differs"):
        with acquire(case):
            pytest.fail("must not yield")
    assert len(case.credentials) == 2
    assert all(value.token == "" for value in case.credentials)
    for client in case.clients:
        client.close.assert_called_once()


def test_lifetime_is_rechecked_after_target_identity(case):
    def expire():
        case.credentials[-1].expiration = datetime.now(UTC) + timedelta(seconds=1199)
        return case.target_identity

    case.clients[1].get_caller_identity.side_effect = expire
    with pytest.raises(bridge.BridgeError, match="too_short_for_operation"):
        with acquire(case):
            pytest.fail("must not yield")
    assert all(value.token == "" for value in case.credentials)
    assert case.raw == {} and case.response == {}


@pytest.mark.parametrize(
    "stage",
    [
        "source_create",
        "source_identity",
        "assume",
        "source_close",
        "target_create",
        "target_identity",
        "target_close",
    ],
)
def test_acquisition_failures_are_safe_and_cleanup_owned_clients(case, stage, capsys):
    failure = RuntimeError(KEY + SECRET + TOKEN)
    if stage.endswith("create"):
        original = case.factory.side_effect

        def create(value):
            if len(case.credentials) == (0 if stage == "source_create" else 1):
                raise failure
            return original(value)

        case.factory.side_effect = create
    else:
        index = 1 if stage.startswith("target") else 0
        name = (
            "assume_role"
            if stage == "assume"
            else "close"
            if stage.endswith("close")
            else "get_caller_identity"
        )
        getattr(case.clients[index], name).side_effect = failure
    with pytest.raises(bridge.BridgeError) as caught:
        with acquire(case):
            pytest.fail("must not yield")
    assert str(caught.value) == "operator_session_acquisition_failed"
    assert not any(value in str(caught.value) for value in (KEY, SECRET, TOKEN))
    assert all(value.token == "" for value in case.credentials)
    assert all(value.token == "" for value in case.parsed)
    for client in case.clients[: len(case.credentials)]:
        client.close.assert_called_once()
    assert capsys.readouterr() == ("", "")


def test_consumer_error_keeps_its_meaning_and_clears_credentials(case):
    failure = RuntimeError("controller recovery required")
    with pytest.raises(RuntimeError) as caught:
        with acquire(case):
            raise failure
    assert caught.value is failure
    assert all(value.token == "" for value in case.credentials)


def test_sdk_factory_refuses_cleared_credentials_without_default_provider(monkeypatch):
    credentials, _ = bridge.receive(packet())
    credentials.clear()
    imported = Mock(side_effect=AssertionError("must not import SDK"))
    monkeypatch.setattr(session, "import_module", imported)
    with pytest.raises(bridge.BridgeError, match="empty_temporary_credentials"):
        session.create_sts_client(credentials)
    imported.assert_not_called()


def test_sdk_factory_requires_reviewed_versions(monkeypatch):
    credentials, _ = bridge.receive(packet())
    monkeypatch.setattr(session, "version", lambda name: "unreviewed")
    try:
        with pytest.raises(bridge.BridgeError, match="sdk_version_mismatch"):
            session.create_sts_client(credentials)
    finally:
        credentials.clear()


@pytest.mark.parametrize(
    "remaining,accepted",
    [
        (1199.999, False),
        (1200, True),
        (3600, True),
        (3600.001, False),
    ],
)
def test_returned_lifetime_window_has_exact_bounds(case, monkeypatch, remaining, accepted):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 3, tzinfo=UTC)

    now = Clock.now(UTC)
    monkeypatch.setattr(session, "datetime", Clock)
    case.raw["Expiration"] = now + timedelta(seconds=remaining)
    plan = session.operator_plan(case.frozen.path, case.frozen.pin, PRINCIPAL)
    if accepted:
        credentials, _ = session.parse_assumed_credentials(case.response, plan, NAME)
        credentials.clear()
    else:
        with pytest.raises(bridge.BridgeError):
            session.parse_assumed_credentials(case.response, plan, NAME)


@pytest.mark.parametrize("service", ["s3", "glue", "athena"])
def test_shared_factory_cannot_open_another_service(monkeypatch, service):
    credentials, _ = bridge.receive(packet())
    imported = Mock(side_effect=AssertionError("must not import SDK"))
    monkeypatch.setattr(session, "import_module", imported)
    try:
        with pytest.raises(bridge.BridgeError, match="unsupported_operator_service"):
            session._create_control_client(credentials, service)
    finally:
        credentials.clear()
    imported.assert_not_called()
