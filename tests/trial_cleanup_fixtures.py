"""Synthetic frozen release, source envelope and scoped STS replies; no network."""

import hashlib
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

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
    "trial_cleanup_inventory",
    "trial_cleanup_session",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
session = sys.modules["trial_cleanup_session"]
bridge = sys.modules["aws_login_bridge"]
inventory = sys.modules["trial_cleanup_inventory"]
roles = sys.modules["aws_role_session"]
OWNER = "123456789012"
PRINCIPAL = f"arn:aws:iam::{OWNER}:role/managed/project/login"
ROLE = "urban-geo-test-trial-cleanup"
NAME = "trial-cleanup-" + "a" * 32


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


def make_case(frozen, tmp_path, monkeypatch):
    prepared = inventory.cleanup_scope(frozen.path, frozen.pin)
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
        "SessionToken": "dummyCleanupToken",
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
        frozen=frozen,
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
    return session.cleanup_credentials(
        case.frozen.path,
        case.frozen.pin,
        case.path,
        case.pin,
        PRINCIPAL,
        packet() if content is None else content,
    )
