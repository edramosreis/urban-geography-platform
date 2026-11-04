"""Scoped S3 inventory ownership, lifetime gates and post-cleanup completion."""

import hashlib
import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError
from unittest.mock import Mock

import pytest
from trial_cleanup_fixtures import (
    OWNER,
    PRINCIPAL,
    ROOT,
    bridge,
    inventory,
    make_case,
    packet,
    roles,
)
from trial_service_fixtures import make_services

name = "trial_cleanup_operation"
if name not in sys.modules:
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
operation = sys.modules[name]


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    value = make_case(frozen, tmp_path, monkeypatch)
    monkeypatch.setattr(inventory, "ARTIFACTS", tmp_path / "artifacts")
    value.output = tmp_path / "artifacts" / "inventory"
    value.s3 = Mock(spec=[*operation.READ_CALLS, "close"])
    lake, results = (value.prepared["buckets"][kind]["name"] for kind in ("lake", "results"))
    value.s3.get_bucket_location.return_value = {"LocationConstraint": "eu-north-1"}
    value.s3.get_bucket_versioning.side_effect = (
        lambda **kw: {"Status": "Enabled"} if kw["Bucket"] == lake else {}
    )
    value.s3.list_object_versions.return_value = {
        "Name": lake,
        "IsTruncated": False,
        "Versions": [
            {
                "Key": value.prepared["buckets"]["lake"]["exact_keys"][0],
                "VersionId": "version-1",
                "IsLatest": True,
                "Size": 128,
                "ETag": '"offline-etag"',
                "LastModified": datetime.now(UTC),
            }
        ],
    }
    value.s3.list_objects_v2.return_value = {
        "Name": results,
        "IsTruncated": False,
        "Contents": [],
    }
    value.s3.list_multipart_uploads.side_effect = lambda **kw: {
        "Bucket": kw["Bucket"],
        "IsTruncated": False,
        "Uploads": [],
    }
    value.opened = []

    def create(snapshot):
        assert snapshot.owner == OWNER and snapshot.token == "dummyCleanupToken"
        value.opened.append(snapshot)
        return value.s3

    value.s3_factory = Mock(side_effect=create)
    monkeypatch.setattr(operation, "create_s3_client", value.s3_factory)
    return value


def run(case):
    return operation.run_inventory(
        case.frozen.path,
        case.frozen.pin,
        case.path,
        case.pin,
        PRINCIPAL,
        packet(),
        output=case.output,
    )


def assert_closed(case):
    assert all(not any(c.session_options().values()) for c in case.credentials)
    for client in case.clients:
        client.close.assert_called_once_with()
    case.s3.close.assert_called_once_with()


def test_full_read_only_connection_completes_after_client_close_and_credential_clear(
    case, monkeypatch, capsys
):
    writes = []
    original = operation.write_once

    def write(path, content):
        assert_closed(case)
        writes.append(path.name)
        return original(path, content)

    monkeypatch.setattr(operation, "write_once", write)

    def close():
        assert case.opened[0].token == "dummyCleanupToken"
        assert not (case.output / "session-complete.json").exists()

    case.s3.close.side_effect = close
    collect = operation.inventory_s3

    def read(client, *args):
        for forbidden in (
            "delete_object",
            "delete_objects",
            "abort_multipart_upload",
            "put_object",
        ):
            assert not hasattr(client, forbidden)
        return collect(client, *args)

    monkeypatch.setattr(operation, "inventory_s3", read)
    result = run(case)
    assert result["status"] == "scoped_s3_inventory_complete"
    assert result["inventory_status"] == "s3_inventory_complete"
    assert result["inventory_plan_sha256"] == case.pin
    assert (
        result["inventory_sha256"]
        == hashlib.sha256((case.output / "inventory.json").read_bytes()).hexdigest()
    )
    assert result["all_keys_in_cleanup_scope"]
    assert not result["deletion_authorized"] and not result["quiescence_and_catalog_observed"]
    assert writes == ["session-complete.json"] and case.s3_factory.call_count == 1
    for call in case.s3.method_calls:
        if call[0] != "close":
            assert call.kwargs["ExpectedBucketOwner"] == OWNER
    assert_closed(case)
    assert capsys.readouterr() == ("", "")


def test_unexpected_key_remains_review_required_after_scoped_completion(case):
    case.s3.list_object_versions.return_value["Versions"][0]["Key"] = "unknown/keep.csv"
    result = run(case)
    assert result["inventory_status"] == "s3_inventory_requires_review"
    assert not result["all_keys_in_cleanup_scope"] and not result["deletion_authorized"]
    assert_closed(case)


@pytest.mark.parametrize("defect", ["pin", "existing", "outside", "plan", "release"])
def test_invalid_local_scope_never_opens_credentials_or_clients(case, monkeypatch, defect):
    if defect == "pin":
        case.pin = "0" * 64
    elif defect == "existing":
        case.output.mkdir(parents=True)
    elif defect == "outside":
        case.output = case.output.parents[1] / "outside"
    elif defect == "plan":
        # Put a valid plan below artifacts, then request output under that plan's directory.
        folder = case.output.parent / "prepared"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "inventory-plan.json"
        path.write_bytes(case.path.read_bytes())
        case.path = path
        case.output = folder / "inventory"
    else:
        case.output = case.frozen.path / "inventory"
    receive = Mock(side_effect=AssertionError("credentials must not be parsed"))
    monkeypatch.setattr(roles, "receive", receive)
    with pytest.raises(ValueError):
        run(case)
    receive.assert_not_called()
    case.factory.assert_not_called()
    case.s3_factory.assert_not_called()


@pytest.mark.parametrize("defect", ["constructor", "service", "close"])
def test_client_and_credential_cleanup_prevents_outer_completion_on_failure(case, defect):
    failure = RuntimeError("sentinel-private-error")
    if defect == "constructor":
        case.s3_factory.side_effect = failure
    elif defect == "service":
        case.s3.list_objects_v2.side_effect = failure
    else:
        case.s3.close.side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        run(case)
    assert caught.value is failure
    assert all(not any(c.session_options().values()) for c in case.credentials)
    for client in case.clients:
        client.close.assert_called_once_with()
    if defect == "constructor":
        case.s3.close.assert_not_called()
        assert not case.output.exists()
    else:
        case.s3.close.assert_called_once_with()
    assert not (case.output / "session-complete.json").exists()
    if defect == "service":
        text = (case.output / "failure.json").read_text()
        assert "sentinel" not in text
        assert (case.output / "lake-versions-01.json").exists()


@pytest.mark.parametrize("defect", ["plan", "release", "final_lifetime"])
def test_final_drift_or_short_lifetime_keeps_only_provisional_inner_inventory(case, defect):
    original = case.s3.list_multipart_uploads.side_effect

    def uploads(**kw):
        if kw["Bucket"] == case.prepared["buckets"]["results"]["name"]:
            if defect == "final_lifetime":
                case.opened[0].expiration = datetime.now(UTC) + timedelta(seconds=80)
            else:
                path = case.path if defect == "plan" else case.frozen.path / "release.json"
                path.write_bytes(b"changed")
        return original(**kw)

    case.s3.list_multipart_uploads.side_effect = uploads
    with pytest.raises(ValueError):
        run(case)
    assert (case.output / "inventory.json").exists()
    assert not (case.output / "session-complete.json").exists()
    assert_closed(case)


def test_each_read_rechecks_lifetime_before_forwarding_to_sdk(case):
    def first(**kw):
        case.opened[0].expiration = datetime.now(UTC) + timedelta(seconds=80)
        return {"LocationConstraint": "eu-north-1"}

    case.s3.get_bucket_location.side_effect = first
    with pytest.raises(bridge.BridgeError, match="credential_lifetime_too_short_for_operation"):
        run(case)
    case.s3.get_bucket_location.assert_called_once()
    case.s3.get_bucket_versioning.assert_not_called()
    assert (case.output / "failure.json").exists()
    assert not (case.output / "session-complete.json").exists()
    assert_closed(case)


def test_changed_saved_report_cannot_produce_scoped_completion(case):
    case.s3.close.side_effect = lambda: (case.output / "inventory.json").write_bytes(b"changed")
    with pytest.raises(ValueError, match="Saved inventory differs"):
        run(case)
    assert not (case.output / "session-complete.json").exists()
    assert_closed(case)


@pytest.mark.parametrize("defect", ["version", "unavailable", "empty"])
def test_sdk_factory_rejects_wrong_pins_missing_sdk_or_empty_snapshot(monkeypatch, defect):
    snapshot = bridge.receive(packet())[0]
    fake_import = Mock(side_effect=AssertionError("SDK must not open"))
    monkeypatch.setattr(operation, "import_module", fake_import)
    monkeypatch.setattr(operation, "version", lambda name: "wrong")
    if defect == "unavailable":
        monkeypatch.setattr(operation, "version", Mock(side_effect=PackageNotFoundError()))
    elif defect == "empty":
        snapshot.token = ""
    try:
        with pytest.raises(bridge.BridgeError):
            operation.create_s3_client(snapshot)
        fake_import.assert_not_called()
    finally:
        snapshot.clear()


def selected(case):
    return operation.run_inventory(
        case.frozen.path,
        case.frozen.pin,
        case.path,
        case.pin,
        PRINCIPAL,
        packet(),
        output=case.output,
        include_services=True,
    )


def service_clients(case, monkeypatch):
    value = make_services(case.frozen)

    def create(snapshot, service):
        assert snapshot is case.opened[0] and snapshot.token == "dummyCleanupToken"
        return {"glue": value.glue, "athena": value.athena}[service]

    value.factory = Mock(side_effect=create)
    monkeypatch.setattr(operation, "create_service_client", value.factory)
    return value


def test_selected_services_bind_both_reports_and_intent_after_all_five_clients_close(
    case, monkeypatch
):
    value = service_clients(case, monkeypatch)
    original = operation.write_once

    def write(path, content):
        if path.name == "session-complete.json":
            assert_closed(case)
            value.glue.close.assert_called_once_with()
            value.athena.close.assert_called_once_with()
        return original(path, content)

    monkeypatch.setattr(operation, "write_once", write)
    result = selected(case)
    assert result["status"] == "scoped_trial_inventory_complete" and result["include_services"]
    assert not result["requires_review"] and not result["deletion_authorized"]
    for filename, field in [
        ("s3/inventory.json", "inventory_sha256"),
        ("services/inventory.json", "service_inventory_sha256"),
        ("operation-intent.json", "operation_intent_sha256"),
    ]:
        assert hashlib.sha256((case.output / filename).read_bytes()).hexdigest() == result[field]
    assert value.factory.call_count == 2
    assert {call.args[1] for call in value.factory.call_args_list} == {"glue", "athena"}
    assert {call[0] for call in value.glue.method_calls} <= {
        "close",
        "get_database",
        "get_tables",
        "get_job",
        "get_job_runs",
    }
    assert {call[0] for call in value.athena.method_calls} <= {
        "close",
        "get_work_group",
        "list_query_executions",
        "get_query_execution",
    }


def test_selected_active_work_remains_review_required_even_when_both_listings_complete(
    case, monkeypatch
):
    value = service_clients(case, monkeypatch)
    value.run["JobRunState"] = "RUNNING"
    result = selected(case)
    assert (
        result["requires_review"]
        and result["service_inventory_status"] == "service_inventory_requires_review"
    )
    assert not result["quiescence_and_catalog_observed"] and not result["deletion_authorized"]
    assert_closed(case)


def test_default_s3_only_mode_never_constructs_service_clients(case, monkeypatch):
    factory = Mock(side_effect=AssertionError("unexpected services"))
    monkeypatch.setattr(operation, "create_service_client", factory)
    result = run(case)
    assert result["status"] == "scoped_s3_inventory_complete" and "include_services" not in result
    factory.assert_not_called()


@pytest.mark.parametrize("flag", ["true", 1, None])
def test_service_selection_must_be_boolean_before_credentials(case, monkeypatch, flag):
    receive = Mock(side_effect=AssertionError("must not receive credentials"))
    monkeypatch.setattr(roles, "receive", receive)
    with pytest.raises(ValueError, match="explicit boolean"):
        operation.run_inventory(
            case.frozen.path,
            case.frozen.pin,
            case.path,
            case.pin,
            PRINCIPAL,
            b"unused",
            output=case.output,
            include_services=flag,
        )
    receive.assert_not_called()
    case.factory.assert_not_called()


@pytest.mark.parametrize("service", ["glue", "athena"])
def test_partial_service_client_startup_closes_previous_clients_and_clears_keys(
    case, monkeypatch, service
):
    value = service_clients(case, monkeypatch)
    create = value.factory.side_effect

    def fail(snapshot, name):
        if name == service:
            raise RuntimeError("constructor failed")
        return create(snapshot, name)

    value.factory.side_effect = fail
    with pytest.raises(RuntimeError, match="constructor failed"):
        selected(case)
    assert_closed(case)
    if service == "athena":
        value.glue.close.assert_called_once_with()
    else:
        value.glue.close.assert_not_called()
    value.athena.close.assert_not_called()
    assert not case.output.exists()


def test_service_read_failure_retains_partial_service_evidence_and_never_starts_s3_reads(
    case, monkeypatch
):
    value = service_clients(case, monkeypatch)
    value.athena.get_work_group.side_effect = RuntimeError("provider-private-sentinel")
    with pytest.raises(RuntimeError):
        selected(case)
    assert (case.output / "services/failure.json").exists()
    assert (case.output / "services/job-runs-01.json").exists()
    assert not (case.output / "session-complete.json").exists()
    case.s3.get_bucket_location.assert_not_called()
    assert_closed(case)
    value.glue.close.assert_called_once_with()
    value.athena.close.assert_called_once_with()


def test_selected_mode_requires_its_longer_admission_before_any_inventory_client(case, monkeypatch):
    value = service_clients(case, monkeypatch)
    case.raw["Expiration"] = datetime.now(UTC) + timedelta(seconds=1400)
    with pytest.raises(bridge.BridgeError, match="credential_lifetime_too_short_for_operation"):
        selected(case)
    assert all(not any(c.session_options().values()) for c in case.credentials)
    for client in case.clients:
        client.close.assert_called_once_with()
    case.s3_factory.assert_not_called()
    value.factory.assert_not_called()
    assert not case.output.exists()


def test_selected_service_calls_recheck_lifetime_before_forwarding(case, monkeypatch):
    value = service_clients(case, monkeypatch)

    def expire(**kwargs):
        case.opened[0].expiration = datetime.now(UTC) + timedelta(seconds=80)
        return {"Database": value.database}

    value.glue.get_database.side_effect = expire
    with pytest.raises(bridge.BridgeError):
        selected(case)
    value.glue.get_tables.assert_not_called()
    case.s3.get_bucket_location.assert_not_called()
    assert not (case.output / "session-complete.json").exists()
    assert_closed(case)
    value.glue.close.assert_called_once_with()
    value.athena.close.assert_called_once_with()


@pytest.mark.parametrize("filename", ["services/inventory.json", "operation-intent.json"])
def test_selected_evidence_changed_during_client_close_cannot_complete(case, monkeypatch, filename):
    service_clients(case, monkeypatch)
    case.s3.close.side_effect = lambda: (case.output / filename).write_bytes(b"changed")
    with pytest.raises(ValueError, match="Saved inventory differs"):
        selected(case)
    assert not (case.output / "session-complete.json").exists()
    assert_closed(case)
