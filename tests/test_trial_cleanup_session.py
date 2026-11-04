"""Cleanup role acquisition and release/recipe binding with synthetic credentials."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from trial_cleanup_fixtures import (
    NAME,
    OWNER,
    PRINCIPAL,
    ROLE,
    acquire,
    bridge,
    make_case,
    packet,
    roles,
    session,
)


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    return make_case(frozen, tmp_path, monkeypatch)


def test_session_plan_binds_release_inventory_and_separate_cleanup_identity(case):
    assert session.cleanup_session_plan(
        case.frozen.path, case.frozen.pin, case.path, case.pin, PRINCIPAL
    ) == {
        "owner": OWNER,
        "source_role_arn": PRINCIPAL,
        "role_name": ROLE,
        "role_arn": f"arn:aws:iam::{OWNER}:role/{ROLE}",
        "region": "eu-north-1",
        "release_manifest_sha256": case.frozen.pin,
        "inventory_plan_sha256": case.pin,
        "duration_seconds": 3600,
        "required_lifetime_seconds": 900,
    }
    case.factory.assert_not_called()


@pytest.mark.parametrize("defect", ["pin", "bytes", "prefix", "release", "authorization", "limit"])
def test_bad_preparation_is_rejected_before_credentials_or_clients(case, monkeypatch, defect):
    pin = case.pin
    if defect == "pin":
        pin = "0" * 64
    elif defect == "bytes":
        case.path.write_bytes(b"changed")
    elif defect == "release":
        (case.frozen.path / "release.json").write_bytes(b"changed")
    else:
        if defect == "prefix":
            case.prepared["buckets"]["lake"]["prefixes"] = [""]
        elif defect == "authorization":
            case.prepared["deletion_authorized"] = True
        else:
            case.prepared["limits"]["max_s3_calls"] = 1000
        case.path.write_text(json.dumps(case.prepared), encoding="utf-8")
        pin = hashlib.sha256(case.path.read_bytes()).hexdigest()
    receive = Mock(side_effect=AssertionError("credentials must not be parsed"))
    monkeypatch.setattr(roles, "receive", receive)
    with pytest.raises(ValueError):
        with session.cleanup_credentials(
            case.frozen.path, case.frozen.pin, case.path, pin, PRINCIPAL, b"unused"
        ):
            pytest.fail("must not yield")
    receive.assert_not_called()
    case.factory.assert_not_called()


@pytest.mark.parametrize(
    "principal",
    [
        "arn:aws:iam::999999999999:role/login",
        f"arn:aws:iam::{OWNER}:role/{ROLE}",
        f"arn:aws:sts::{OWNER}:assumed-role/login/browser",
    ],
)
def test_bad_trust_never_opens_clients(case, principal):
    with pytest.raises(ValueError, match="same-account IAM role"):
        with session.cleanup_credentials(
            case.frozen.path, case.frozen.pin, case.path, case.pin, principal, packet()
        ):
            pytest.fail("must not yield")
    case.factory.assert_not_called()


def test_exact_assumption_closes_sts_before_yield_and_clears_owned_credentials(case, capsys):
    with acquire(case) as credentials:
        credentials.require_operation(900)
        assert credentials.owner == OWNER and credentials.token == "dummyCleanupToken"
        assert case.credentials[0].token == "" and case.raw == {} and case.response == {}
        for client in case.clients:
            client.get_caller_identity.assert_called_once_with()
            client.close.assert_called_once_with()
        case.clients[0].assume_role.assert_called_once_with(
            RoleArn=f"arn:aws:iam::{OWNER}:role/{ROLE}",
            RoleSessionName=NAME,
            DurationSeconds=3600,
        )
        case.clients[1].assume_role.assert_not_called()
    assert all(not any(value.session_options().values()) for value in case.credentials)
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "defect",
    [
        "short_source",
        "envelope_account",
        "source_account",
        "source_role",
        "returned_role",
        "short_return",
        "target_identity",
    ],
)
def test_wrong_identity_or_insufficient_lifetime_is_rejected_and_cleared(case, defect):
    content = packet(seconds=119) if defect == "short_source" else packet()
    if defect == "envelope_account":
        content = packet(owner="999999999999")
    elif defect == "source_account":
        case.source["Account"] = "999999999999"
    elif defect == "source_role":
        case.source["Arn"] = f"arn:aws:sts::{OWNER}:assumed-role/unapproved/browser"
    elif defect == "returned_role":
        case.response["AssumedRoleUser"]["Arn"] = (
            f"arn:aws:sts::{OWNER}:assumed-role/urban-geo-test-glue-operator/{NAME}"
        )
    elif defect == "short_return":
        case.raw["Expiration"] = datetime.now(UTC) + timedelta(seconds=899)
    elif defect == "target_identity":
        case.target["UserId"] = "unapproved"
    with pytest.raises(bridge.BridgeError):
        with acquire(case, content):
            pytest.fail("must not yield")
    assert all(value.token == "" for value in case.credentials)
    expected = (
        0
        if defect in {"short_source", "envelope_account"}
        else 2
        if defect == "target_identity"
        else 1
    )
    assert len(case.credentials) == expected
    for client in case.clients[:expected]:
        client.close.assert_called_once_with()
    if defect in {"short_source", "envelope_account", "source_account", "source_role"}:
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


@pytest.mark.parametrize("what", ["plan", "release"])
def test_preparation_drift_during_assumption_is_rejected_and_cleared(case, what):
    def change():
        path = case.path if what == "plan" else case.frozen.path / "release.json"
        path.write_bytes(b"changed")
        return case.target

    case.clients[1].get_caller_identity.side_effect = change
    with pytest.raises(ValueError):
        with acquire(case):
            pytest.fail("changed preparation must never yield")
    assert all(value.token == "" for value in case.credentials)
    for client in case.clients:
        client.close.assert_called_once()


def test_consumer_failure_is_preserved_and_keys_are_cleared(case):
    failure = RuntimeError("inventory failed")
    with pytest.raises(RuntimeError) as caught:
        with acquire(case):
            raise failure
    assert caught.value is failure
    assert all(value.token == "" for value in case.credentials)
