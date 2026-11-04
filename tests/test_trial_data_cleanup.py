"""Exact deletion requests, fresh guards, subset-only recovery and independent absence checks."""

import hashlib
import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from trial_cleanup_fixtures import PRINCIPAL, bridge, make_case, packet
from trial_data_cleanup_fixtures import ROOT, SENTINEL, ServiceError, make_cleanup_case, plans

name = "trial_data_cleanup"
if name not in sys.modules:
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
cleanup = sys.modules[name]


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    value = make_cleanup_case(frozen, tmp_path, monkeypatch)
    plans.prepare_cleanup(
        value.glue,
        value.athena,
        value.s3,
        frozen.path,
        frozen.pin,
        value.evidence,
        value.output,
        coordinator_paused=True,
    )
    path = value.output / "plan.json"
    value.approved = plans.Record(path, hashlib.sha256(path.read_bytes()).hexdigest())
    value.output = tmp_path / "artifacts/cleanup-execution"
    for client in (value.glue, value.athena, value.s3):
        client.reset_mock()
    value.clock = [0.0]
    value.now = lambda: value.clock[0]
    return value


def execute(case, resume=False):
    return cleanup.execute_cleanup(
        case.glue,
        case.athena,
        case.s3,
        case.frozen.path,
        case.frozen.pin,
        case.evidence,
        case.approved,
        case.output,
        coordinator_paused=True,
        resume=resume,
        now=case.now,
    )


def no_changes(case):
    case.glue.delete_table.assert_not_called()
    case.s3.delete_object.assert_not_called()
    case.s3.abort_multipart_upload.assert_not_called()


def test_exact_selection_is_deleted_in_dependency_order_and_absence_is_observed(case):
    result = execute(case)
    assert result["status"] == "trial_data_absence_observed" and result["data_cleanup_observed"]
    assert result["actions_attempted"] == 7 and result["api_calls"] == 57
    assert (
        not case.catalog
        and not case.versions
        and not case.markers
        and not case.objects
        and not case.uploads
    )
    assert [c.kwargs["Name"] for c in case.glue.delete_table.call_args_list] == list(
        plans.TABLE_ORDER
    )
    for call in case.s3.delete_object.call_args_list:
        assert call.kwargs["ExpectedBucketOwner"] == "123456789012"
        if call.kwargs["Bucket"] == case.scope["buckets"]["lake"]["name"]:
            assert (
                call.kwargs["VersionId"] in {"version-1", "marker-1"}
                and "IfMatch" not in call.kwargs
            )
        else:
            assert call.kwargs["IfMatch"] == '"result-etag"' and "VersionId" not in call.kwargs
    case.s3.list_parts.assert_called_once_with(
        Bucket=case.scope["buckets"]["lake"]["name"],
        Key="warehouse/building_proximity/pending.parquet",
        UploadId="upload-1",
        ExpectedBucketOwner="123456789012",
        MaxParts=1,
    )
    assert result["multipart_parts_empty_observed"] and not result["infrastructure_destroyed"]
    assert (
        not result["catalog_orphan_versions_verified"] and not result["future_submissions_fenced"]
    )
    for client in (case.glue, case.athena, case.s3):
        client.close.assert_not_called()
    assert all(SENTINEL not in p.read_text() for p in case.output.rglob("*.json"))


def test_action_intent_is_saved_before_each_delete_or_abort(case):
    for client, name in [
        (case.glue, "delete_table"),
        (case.s3, "delete_object"),
        (case.s3, "abort_multipart_upload"),
    ]:
        method = getattr(client, name)
        original = method.side_effect

        def check(original=original, **kw):
            import json

            records = sorted(case.output.glob("action-*-intent.json"))
            assert records and json.loads(records[-1].read_bytes())["request"] == kw
            return original(**kw)

        method.side_effect = check
    assert execute(case)["data_cleanup_observed"]


@pytest.mark.parametrize("which", ["table", "upload", "version", "result"])
def test_first_rejected_action_is_not_retried_and_incomplete_data_never_counts_as_absent(
    case, which
):
    if which == "table":
        case.glue.delete_table.side_effect = ServiceError("AccessDeniedException", 400)
    elif which == "upload":
        case.s3.abort_multipart_upload.side_effect = ServiceError("AccessDenied", 403)
    else:
        original = case.s3.delete_object.side_effect

        def fail(**kw):
            if (which == "version" and "VersionId" in kw) or (
                which == "result" and "VersionId" not in kw
            ):
                raise ServiceError("AccessDenied", 403)
            return original(**kw)

        case.s3.delete_object.side_effect = fail
    report = execute(case)
    assert not report["data_cleanup_observed"] and report["requires_review"]
    assert report["changes"][-1]["error_code"] in {"AccessDenied", "AccessDeniedException"}
    requests = [
        (c[0], str(c.kwargs))
        for client in (case.glue, case.s3)
        for c in client.method_calls
        if c[0] in {"delete_table", "delete_object", "abort_multipart_upload"}
    ]
    assert len(requests) == len(set(requests))
    assert all(SENTINEL not in p.read_text() for p in case.output.rglob("*.json"))


def test_last_delete_with_lost_acknowledgement_can_be_resolved_by_fresh_absence(case):
    original = case.s3.delete_object.side_effect

    def lost(**kw):
        result = original(**kw)
        if "VersionId" not in kw:
            raise ServiceError("InternalError", 500)
        return result

    case.s3.delete_object.side_effect = lost
    result = execute(case)
    assert result["data_cleanup_observed"] and result["changes"][-1]["acknowledged"] is False
    assert case.s3.delete_object.call_count == 3


def test_explicit_resume_deletes_only_unchanged_remaining_items_from_the_same_plan(case):
    original = case.s3.delete_object.side_effect
    case.s3.delete_object.side_effect = ServiceError("InternalError", 500)
    assert execute(case)["requires_review"]
    assert not case.catalog and not case.uploads and case.versions
    case.s3.delete_object.side_effect = original
    for client in (case.glue, case.athena, case.s3):
        client.reset_mock()
    case.output = case.output.with_name("cleanup-resumed")
    case.versions[0]["IsLatest"] = True  # Version identity/content stay unchanged.
    result = execute(case, True)
    assert result["data_cleanup_observed"] and result["actions_attempted"] == 3
    case.glue.delete_table.assert_not_called()
    case.s3.abort_multipart_upload.assert_not_called()


def test_resume_after_success_is_observational_and_has_no_more_delete_requests(case):
    assert execute(case)["data_cleanup_observed"]
    for client in (case.glue, case.athena, case.s3):
        client.reset_mock()
    case.output = case.output.with_name("cleanup-observed-again")
    result = execute(case, True)
    assert result["data_cleanup_observed"] and result["actions_attempted"] == 0
    no_changes(case)


@pytest.mark.parametrize(
    "which", ["new_version", "changed_result", "changed_table", "bad_plan_pin"]
)
def test_changed_or_added_targets_never_expand_the_approved_selection(case, which):
    if which == "new_version":
        case.versions.append({**case.versions[0], "VersionId": "unapproved"})
    elif which == "changed_result":
        case.objects[0]["ETag"] = '"replacement"'
    elif which == "changed_table":
        case.catalog["release_proximity"]["VersionId"] = "2"
    else:
        case.approved = plans.Record(case.approved.path, "0" * 64)
    with pytest.raises(ValueError):
        execute(case, True)
    no_changes(case)


def test_new_table_generation_before_its_delete_is_rejected(case):
    original = case.glue.get_table.side_effect

    def changed(**kw):
        if case.glue.get_table.call_count == 4:
            case.catalog[kw["Name"]]["CreateTime"] += timedelta(seconds=1)
        return original(**kw)

    case.glue.get_table.side_effect = changed
    with pytest.raises(ValueError, match="generation changed"):
        execute(case)
    no_changes(case)


def test_ifmatch_protects_a_result_replaced_after_the_initial_snapshot(case):
    original = case.s3.delete_object.side_effect

    def replaced(**kw):
        if "VersionId" not in kw:
            case.objects[0]["ETag"] = '"replacement"'
        return original(**kw)

    case.s3.delete_object.side_effect = replaced
    result = execute(case)
    assert (
        not result["data_cleanup_observed"]
        and result["changes"][-1]["error_code"] == "PreconditionFailed"
    )
    assert case.objects[0]["ETag"] == '"replacement"'


@pytest.mark.parametrize("which", ["parts_remain", "access_denied", "wrong_missing_status"])
def test_missing_parts_verification_never_claims_cleanup_even_if_upload_listing_is_empty(
    case, which
):
    if which == "parts_remain":
        case.s3.list_parts.side_effect = lambda **kw: {
            "Bucket": kw["Bucket"],
            "Key": kw["Key"],
            "UploadId": kw["UploadId"],
            "IsTruncated": False,
            "Parts": [{"PartNumber": 1, "Size": 5}],
        }
    else:
        case.s3.list_parts.side_effect = ServiceError(
            "AccessDenied" if which == "access_denied" else "NoSuchUpload", 403
        )
    result = execute(case)
    assert (
        not case.uploads
        and not result["multipart_parts_empty_observed"]
        and not result["data_cleanup_observed"]
    )


def test_new_object_appearing_during_cleanup_is_retained_and_prevents_completion(case):
    original = case.s3.delete_object.side_effect

    def appeared(**kw):
        result = original(**kw)
        if "VersionId" not in kw:
            from trial_data_cleanup_fixtures import NOW

            case.versions.append(
                {
                    "Key": "warehouse/building_proximity/new.parquet",
                    "VersionId": "new-version",
                    "IsLatest": True,
                    "Size": 1,
                    "ETag": '"new"',
                    "LastModified": NOW,
                }
            )
        return result

    case.s3.delete_object.side_effect = appeared
    result = execute(case)
    assert not result["data_cleanup_observed"] and result["requires_review"]
    assert case.versions[0]["Key"].endswith("new.parquet")
    assert all(
        c.kwargs["Key"] != case.versions[0]["Key"] for c in case.s3.delete_object.call_args_list
    )


def test_active_work_in_the_fresh_batch_guard_blocks_all_mutation(case):
    original = case.glue.get_job_runs.side_effect

    def active(**kw):
        if case.glue.get_job_runs.call_count == 2:
            case.run["JobRunState"] = "RUNNING"
        return original(**kw)

    case.glue.get_job_runs.side_effect = active
    with pytest.raises(ValueError):
        execute(case)
    no_changes(case)


@pytest.mark.parametrize("which", ["calls", "time"])
def test_shared_api_and_deadline_budget_stop_before_further_requests(case, monkeypatch, which):
    if which == "calls":
        monkeypatch.setattr(cleanup, "MAX_CALLS", 5)
    else:
        original = case.glue.get_job.side_effect

        def expire(**kw):
            case.clock[0] = 2000
            return original(**kw)

        case.glue.get_job.side_effect = expire
    with pytest.raises(TimeoutError):
        execute(case)
    assert (case.output / "failure.json").exists() and not (case.output / "receipt.json").exists()
    no_changes(case)


@pytest.fixture
def connected(case, tmp_path, monkeypatch):
    case.auth = make_case(case.frozen, tmp_path, monkeypatch)
    case.snapshots = []

    def s3(snapshot):
        case.snapshots.append(snapshot)
        return case.s3

    def service(snapshot, name):
        assert snapshot is case.snapshots[0]
        return case.glue if name == "glue" else case.athena

    monkeypatch.setattr(cleanup, "create_s3_client", Mock(side_effect=s3))
    monkeypatch.setattr(cleanup, "create_service_client", Mock(side_effect=service))
    return case


def scoped(case, approved=True):
    return cleanup.run_data_cleanup(
        case.frozen.path,
        case.frozen.pin,
        case.evidence,
        PRINCIPAL,
        packet(),
        output=case.output,
        coordinator_paused=True,
        approved=case.approved if approved else None,
        now=case.now,
    )


def test_scoped_execution_completes_after_all_five_clients_close_and_keys_clear(
    connected, monkeypatch
):
    case = connected
    original = cleanup.write_once

    def write(path, content):
        assert all(not any(v.session_options().values()) for v in case.auth.credentials)
        for client in (*case.auth.clients, case.s3, case.glue, case.athena):
            client.close.assert_called_once_with()
        return original(path, content)

    monkeypatch.setattr(cleanup, "write_once", write)
    result = scoped(case)
    assert result["executed"] and result["data_cleanup_observed"]


def test_scoped_default_prepares_without_deletion(connected):
    result = scoped(connected, False)
    assert result["status"] == "scoped_data_cleanup_prepared" and not result["executed"]
    assert (connected.output / "attempt/plan.json").exists()
    no_changes(connected)


def test_short_scoped_lifetime_opens_no_inventory_clients(connected):
    connected.auth.raw["Expiration"] = datetime.now(UTC) + timedelta(seconds=1700)
    with pytest.raises(bridge.BridgeError):
        scoped(connected)
    cleanup.create_s3_client.assert_not_called()
    cleanup.create_service_client.assert_not_called()
    assert all(not any(v.session_options().values()) for v in connected.auth.credentials)


def test_scoped_close_failure_preserves_inner_receipt_without_outer_success(connected):
    connected.glue.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError):
        scoped(connected)
    for client in (connected.s3, connected.glue, connected.athena):
        client.close.assert_called_once_with()
    assert all(not any(v.session_options().values()) for v in connected.auth.credentials)
    assert (connected.output / "attempt/receipt.json").exists()
    assert not (connected.output / "session-complete.json").exists()
