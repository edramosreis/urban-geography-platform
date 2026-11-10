"""One source snapshot, six read-only clients and final proof only after cleanup."""

import importlib.util
import sys
from unittest.mock import Mock

import pytest
from trial_cleanup_fixtures import packet
from trial_infrastructure_fixtures import ROOT, make_case

for name in ("trial_infrastructure_absence", "trial_infrastructure_operation"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
operation = sys.modules["trial_infrastructure_operation"]


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    case = make_case(frozen, tmp_path, monkeypatch)
    case.snapshots = []

    def client(snapshot, service):
        case.snapshots.append(snapshot)
        return case.clients[service]

    case.factory = Mock(side_effect=client)
    monkeypatch.setattr(operation, "create_client", case.factory)
    return case


def run(case, content=None):
    return operation.run_absence(
        case.frozen.path,
        case.frozen.pin,
        case.principal,
        case.pin,
        packet() if content is None else content,
        case.output,
        now=lambda: case.clock[0],
    )


def test_completion_is_saved_only_after_six_closes_and_source_keys_clear(case, monkeypatch):
    original = operation._save

    def save(path, value, limit):
        assert len(case.snapshots) == 6 and all(s is case.snapshots[0] for s in case.snapshots)
        assert not any(case.snapshots[0].session_options().values())
        assert all(c.close.call_count == 1 for c in case.clients.values())
        return original(path, value, limit)

    monkeypatch.setattr(operation, "_save", save)
    result = run(case)
    assert result["named_infrastructure_absence_observed"] and not result["requires_review"]
    assert result["read_only"] and not result["deletion_performed"]
    assert case.clients["sts"].get_caller_identity.call_count == 1


@pytest.mark.parametrize("defect", ["pin", "output", "short_source", "wrong_account"])
def test_bad_local_plan_or_source_opens_no_clients(case, defect):
    content = packet()
    if defect == "pin":
        case.pin = "0" * 64
    elif defect == "output":
        case.output.mkdir(parents=True)
    elif defect == "short_source":
        content = packet(599)
    else:
        content = packet(owner="000000000000")
    with pytest.raises(ValueError):
        run(case, content)
    case.factory.assert_not_called()


@pytest.mark.parametrize("which", ["factory", "read", "close"])
def test_failure_closes_every_created_client_clears_keys_and_leaves_no_outer_success(case, which):
    if which == "factory":
        original = case.factory.side_effect

        def broken(snapshot, service):
            if service == "iam":
                raise RuntimeError("private-error")
            return original(snapshot, service)

        case.factory.side_effect = broken
    elif which == "read":
        case.clients["sts"].get_caller_identity.side_effect = RuntimeError("private-error")
    else:
        case.clients["iam"].close.side_effect = RuntimeError("private-error")
    with pytest.raises(RuntimeError):
        run(case)
    assert case.snapshots and not any(case.snapshots[0].session_options().values())
    for service, client in case.clients.items():
        if which != "factory" or service in ("sts", "s3", "glue", "athena"):
            client.close.assert_called_once()
        else:
            client.close.assert_not_called()
    assert not (case.output / "session-complete.json").exists()


def test_denied_read_is_an_incomplete_observation_not_a_success_claim(case):
    from trial_infrastructure_fixtures import ServiceError

    case.clients["iam"].get_policy.side_effect = ServiceError("AccessDenied", 403)
    result = run(case)
    assert result["requires_review"] and not result["named_infrastructure_absence_observed"]
    assert all(c.close.call_count == 1 for c in case.clients.values())


def test_expiring_source_stops_before_more_forwarded_calls_and_outer_success(case):
    from datetime import UTC, datetime, timedelta

    original = case.clients["sts"].get_caller_identity.return_value

    def expire(**kw):
        case.snapshots[0].expiration = datetime.now(UTC) + timedelta(seconds=89)
        return original

    case.clients["sts"].get_caller_identity.side_effect = expire
    with pytest.raises(ValueError):
        run(case)
    case.clients["s3"].list_objects_v2.assert_not_called()
    assert not (case.output / "session-complete.json").exists()
    assert not any(case.snapshots[0].session_options().values())
