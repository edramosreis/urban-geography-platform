"""Read-only enumeration of versions, markers, objects and unfinished uploads with hard bounds."""

import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
for name in (
    "prepare_dbt_project",
    "athena_read_plan",
    "athena_control",
    "aws_login_bridge",
    "aws_role_session",
    "prepare_glue_iam",
    "trial_cleanup_inventory",
    "prepare_trial_cleanup",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
inventory = sys.modules["trial_cleanup_inventory"]
prepare = sys.modules["prepare_trial_cleanup"]
NOW = datetime(2026, 11, 4, tzinfo=UTC)
PRINCIPAL = "arn:aws:iam::123456789012:role/managed/project/login"


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(inventory, "ARTIFACTS", tmp_path / "artifacts")
    monkeypatch.setattr(prepare, "ARTIFACTS", tmp_path / "artifacts")
    scope = inventory.cleanup_scope(frozen.path, frozen.pin)
    lake, results = (scope["buckets"][k]["name"] for k in ("lake", "results"))
    version = {
        "Key": scope["buckets"]["lake"]["exact_keys"][0],
        "VersionId": "version-1",
        "IsLatest": False,
        "Size": 100,
        "ETag": '"etag-1"',
        "LastModified": NOW,
    }
    marker = {"Key": version["Key"], "VersionId": "marker-1", "IsLatest": True, "LastModified": NOW}
    obj = {"Key": "queries/query-1.csv", "Size": 16, "ETag": '"etag-2"', "LastModified": NOW}
    client = Mock()
    client.get_bucket_location.return_value = {"LocationConstraint": "eu-north-1"}
    client.get_bucket_versioning.side_effect = (
        lambda **kw: {"Status": "Enabled"} if kw["Bucket"] == lake else {}
    )
    client.list_object_versions.return_value = {
        "Name": lake,
        "IsTruncated": False,
        "Versions": [version],
        "DeleteMarkers": [marker],
    }
    client.list_objects_v2.return_value = {"Name": results, "IsTruncated": False, "Contents": [obj]}

    def uploads(**kw):
        key = (
            "warehouse/building_proximity/pending.parquet"
            if kw["Bucket"] == lake
            else "queries/pending.csv"
        )
        return {
            "Bucket": kw["Bucket"],
            "IsTruncated": False,
            "Uploads": [{"Key": key, "UploadId": "upload-1", "Initiated": NOW}],
        }

    client.list_multipart_uploads.side_effect = uploads
    return SimpleNamespace(
        frozen=frozen,
        scope=scope,
        lake=lake,
        results=results,
        version=version,
        marker=marker,
        obj=obj,
        client=client,
        output=tmp_path / "artifacts/inventory",
    )


def run(c):
    return inventory.inventory_s3(c.client, c.frozen.path, c.frozen.pin, c.output)


def assert_read_only(c):
    assert {call[0] for call in c.client.method_calls} <= {
        "get_bucket_location",
        "get_bucket_versioning",
        "list_object_versions",
        "list_objects_v2",
        "list_multipart_uploads",
    }
    for call in c.client.method_calls:
        assert call.kwargs["ExpectedBucketOwner"] == "123456789012"
    c.client.close.assert_not_called()


def test_inventory_retains_hidden_versions_markers_and_uploads_without_authorizing_delete(case):
    report = run(case)
    assert report["status"] == "s3_inventory_complete" and report["enumeration_complete"]
    assert len(report["entries"]) == 5 and report["api_calls"] == 8
    assert {e["kind"] for e in report["entries"]} == {
        "version",
        "delete_marker",
        "object",
        "multipart_upload",
    }
    assert report["observed_object_bytes"] == 116 and not report["multipart_bytes_known"]
    assert not report["deletion_authorized"] and not report["quiescence_and_catalog_observed"]
    assert len(report["pages"]) == 4
    assert json.loads((case.output / "inventory.json").read_bytes()) == report
    assert_read_only(case)


@pytest.mark.parametrize("stream", ["versions", "objects", "uploads"])
def test_pagination_reuses_opaque_markers_within_explicit_limits(case, stream):
    if stream == "versions":
        first = {
            "Name": case.lake,
            "IsTruncated": True,
            "Versions": [case.version],
            "NextKeyMarker": case.version["Key"],
            "NextVersionIdMarker": "version-1",
        }
        second = {"Name": case.lake, "IsTruncated": False, "DeleteMarkers": [case.marker]}
        case.client.list_object_versions.side_effect = [first, second]
    elif stream == "objects":
        case.client.list_objects_v2.side_effect = [
            {
                "Name": case.results,
                "IsTruncated": True,
                "Contents": [case.obj],
                "NextContinuationToken": "opaque+token/%",
            },
            {"Name": case.results, "IsTruncated": False, "Contents": []},
        ]
    else:
        original = case.client.list_multipart_uploads.side_effect

        def uploads(**kw):
            if kw["Bucket"] == case.results:
                return original(**kw)
            if "KeyMarker" not in kw:
                return {
                    "Bucket": case.lake,
                    "IsTruncated": True,
                    "Uploads": [],
                    "NextKeyMarker": "warehouse/building_proximity/a%20b",
                    "NextUploadIdMarker": "opaque+upload/%",
                }
            return {"Bucket": case.lake, "IsTruncated": False, "Uploads": []}

        case.client.list_multipart_uploads.side_effect = uploads
    report = run(case)
    assert report["enumeration_complete"] and report["api_calls"] == 9
    if stream == "versions":
        assert case.client.list_object_versions.call_args.kwargs["VersionIdMarker"] == "version-1"
    elif stream == "objects":
        assert case.client.list_objects_v2.call_args.kwargs["ContinuationToken"] == "opaque+token/%"
    else:
        call = case.client.list_multipart_uploads.call_args_list[1]
        assert call.kwargs["KeyMarker"] == "warehouse/building_proximity/a%20b"
        assert call.kwargs["UploadIdMarker"] == "opaque+upload/%"
    assert_read_only(case)


@pytest.mark.parametrize(
    "key",
    [
        "runtime/releases/old-release/release.json",
        "private/keep.csv",
        "warehouse%2Fbuilding_proximity%2Fpart.parquet",
    ],
)
def test_unknown_keys_are_retained_for_review_without_broadening_scope(case, key):
    case.version["Key"] = key
    report = run(case)
    assert report["status"] == "s3_inventory_requires_review"
    assert not report["all_keys_in_cleanup_scope"] and not report["deletion_authorized"]
    assert any(e["key"] == key and not e["in_cleanup_scope"] for e in report["entries"])
    assert_read_only(case)


@pytest.mark.parametrize("defect", ["region", "lake_suspended", "results_versioned", "mfa_delete"])
def test_unexpected_bucket_configuration_never_completes(case, defect):
    if defect == "region":
        case.client.get_bucket_location.return_value = {"LocationConstraint": "eu-west-1"}
    else:
        original = case.client.get_bucket_versioning.side_effect

        def state(**kw):
            value = original(**kw)
            if defect == "lake_suspended" and kw["Bucket"] == case.lake:
                value["Status"] = "Suspended"
            if defect == "results_versioned" and kw["Bucket"] == case.results:
                value["Status"] = "Enabled"
            if defect == "mfa_delete":
                value["MFADelete"] = "Enabled"
            return value

        case.client.get_bucket_versioning.side_effect = state
    with pytest.raises(ValueError, match="differs"):
        run(case)
    assert not (case.output / "inventory.json").exists()
    assert not json.loads((case.output / "failure.json").read_bytes())["enumeration_complete"]
    assert_read_only(case)


@pytest.mark.parametrize(
    "defect",
    [
        "bucket",
        "numeric_flag",
        "missing_marker",
        "duplicate",
        "size",
        "version",
        "date",
        "row_limit",
    ],
)
def test_malformed_or_ambiguous_pages_are_never_accepted(case, defect):
    page = case.client.list_object_versions.return_value
    if defect == "bucket":
        page["Name"] = "other-bucket"
    elif defect == "numeric_flag":
        page["IsTruncated"] = 0
    elif defect == "missing_marker":
        page["IsTruncated"] = True
    elif defect == "duplicate":
        case.marker["VersionId"] = case.version["VersionId"]
    elif defect == "size":
        case.version["Size"] = True
    elif defect == "version":
        case.version["VersionId"] = ""
    elif defect == "date":
        case.version["LastModified"] = datetime(2026, 11, 4)
    else:
        page["Versions"] = [case.version | {"VersionId": str(i)} for i in range(101)]
    with pytest.raises(ValueError):
        run(case)
    assert not (case.output / "inventory.json").exists()
    assert_read_only(case)


@pytest.mark.parametrize("defect", ["cycle", "page_limit", "entry_limit", "byte_limit"])
def test_enumeration_and_local_storage_bounds_stop_without_a_complete_inventory(
    case, monkeypatch, defect
):
    if defect == "byte_limit":
        monkeypatch.setattr(inventory, "MAX_PAGE_BYTES", 16)
    elif defect == "entry_limit":
        monkeypatch.setattr(inventory, "MAX_ENTRIES", 1)
    else:
        monkeypatch.setattr(inventory, "MAX_PAGES", 3 if defect == "cycle" else 2)
        if defect == "cycle":
            case.client.list_object_versions.side_effect = lambda **kw: {
                "Name": case.lake,
                "IsTruncated": True,
                "NextKeyMarker": "same-key",
            }
        else:
            number = 0

            def next_page(**kw):
                nonlocal number
                number += 1
                return {"Name": case.lake, "IsTruncated": True, "NextKeyMarker": f"key-{number}"}

            case.client.list_object_versions.side_effect = next_page
    with pytest.raises(ValueError):
        run(case)
    assert not (case.output / "inventory.json").exists()
    assert case.client.list_object_versions.call_count <= 2
    assert_read_only(case)


def test_service_error_retains_prior_pages_without_provider_details(case):
    case.client.list_objects_v2.side_effect = RuntimeError("sentinel-private-provider-error")
    with pytest.raises(RuntimeError):
        run(case)
    assert (case.output / "lake-versions-01.json").exists()
    assert (case.output / "lake-uploads-01.json").exists()
    failure = json.loads((case.output / "failure.json").read_bytes())
    assert failure["stage"] == "results_objects" and failure["entries_retained"] == 3
    assert "sentinel" not in json.dumps(failure) and not failure["deletion_authorized"]
    assert_read_only(case)


@pytest.mark.parametrize("defect", ["pin", "existing", "outside", "release"])
def test_bad_local_scope_or_output_never_calls_the_client(case, defect):
    pin, output = case.frozen.pin, case.output
    if defect == "pin":
        pin = "0" * 64
    elif defect == "existing":
        output.mkdir(parents=True)
    elif defect == "outside":
        output = output.parents[1] / "outside"
    else:
        output = case.frozen.path / "inventory"
    with pytest.raises(ValueError):
        inventory.inventory_s3(case.client, case.frozen.path, pin, output)
    assert case.client.method_calls == []


def test_private_preparation_reuses_verified_release_and_exports_no_mutation_command(
    case, monkeypatch, capsys
):
    output = case.output.with_name("prepared")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare-cleanup",
            "--release",
            str(case.frozen.path),
            "--release-sha256",
            case.frozen.pin,
            "--source-role-arn",
            PRINCIPAL,
            "--output",
            str(output),
        ],
    )
    prepare.main()
    text = capsys.readouterr().out
    assert "123456789012" not in text and PRINCIPAL not in text
    variables = json.loads((output / "cleanup.private.tfvars.json").read_bytes())
    assert variables["trial_cleanup_principal_arn"] == PRINCIPAL
    assert "glue_job_release_json" in variables
    scope = json.loads((output / "inventory-plan.json").read_bytes())
    assert scope == case.scope and scope["deletion_authorized"] is False
    assert case.client.method_calls == []
