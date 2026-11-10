"""Quiet synthetic services, exact versioned objects and scoped completion evidence."""

import copy
import hashlib
import importlib.util
import sys
from datetime import UTC, datetime
from unittest.mock import Mock

from trial_quiescence_fixtures import ROOT, execute, make_controller_case, save

for name in ("dbt_view_catalog", "dbt_view_capture", "trial_cleanup_operation", "trial_cleanup_plan"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
plans = sys.modules["trial_cleanup_plan"]
NOW = datetime(2026, 11, 4, tzinfo=UTC)
SENTINEL = "sentinel-private-contents"


class ServiceError(RuntimeError):
    def __init__(self, code, status=404):
        super().__init__(SENTINEL)
        self.response = {
            "Error": {"Code": code, "Message": SENTINEL},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


def make_cleanup_case(frozen, folder, monkeypatch):
    value = make_controller_case(frozen, folder, monkeypatch)
    execute(value)
    receipt_path = value.output / "receipt.json"
    receipt = plans.Record(receipt_path, hashlib.sha256(receipt_path.read_bytes()).hexdigest())
    completion = save(
        folder / "quiet-proof/session-complete.json",
        {
            "status": "scoped_trial_quiescence_complete",
            "plan_sha256": value.pin,
            "inventory_plan_sha256": "unused",
            "receipt_sha256": receipt.sha256,
            "quiescence_observed": True,
            "requires_review": False,
            "deletion_authorized": False,
        },
    )
    inventory = save(folder / "prepared/inventory-plan.json", value.scope)
    completion_value = __import__("json").loads(completion.path.read_bytes())
    completion_value["inventory_plan_sha256"] = inventory.sha256
    completion = save(completion.path, completion_value)
    value.evidence = plans.CleanupEvidence(
        inventory, receipt, completion, value.job_receipt, value.evidence
    )
    value.output = folder / "artifacts" / "cleanup-prepared"
    value.catalog = {}
    owner = value.scope["target"]["account_id"]
    for name in plans.TABLE_ORDER:
        row = {
            "Name": name,
            "CatalogId": owner,
            "DatabaseName": value.scope["expected_services"]["database"],
            "TableType": "EXTERNAL_TABLE" if name == "building_proximity" else "VIRTUAL_VIEW",
            "CreateTime": NOW,
            "VersionId": "1",
            "Parameters": {"private": SENTINEL},
        }
        if name == "building_proximity":
            location = (
                "s3://" + value.scope["buckets"]["lake"]["name"] + "/warehouse/building_proximity"
            )
            row["StorageDescriptor"] = {"Location": location}
            row["Parameters"].update(
                table_type="ICEBERG", metadata_location=location + "/metadata/v1.metadata.json"
            )
        else:
            row["ViewOriginalText"] = SENTINEL
        value.catalog[name] = row
    prior = value.glue
    value.glue = Mock(spec=[*value.controller.GLUE_CALLS, "get_table", "delete_table", "close"])
    for method in ("get_database", "get_job", "get_job_runs", "get_job_run"):
        getattr(value.glue, method).side_effect = lambda method=method, **kw: getattr(
            prior, method
        )(**kw)
    value.glue.get_tables.side_effect = lambda **kw: {
        "TableList": [
            {"Name": r["Name"], "TableType": r["TableType"]} for r in value.catalog.values()
        ]
    }
    value.glue.get_table.side_effect = lambda **kw: {
        "Table": copy.deepcopy(value.catalog[kw["Name"]])
    }
    value.glue.delete_table.side_effect = lambda **kw: value.catalog.pop(kw["Name"]) and {}
    value.athena.reset_mock()
    lake, results = (value.scope["buckets"][k]["name"] for k in ("lake", "results"))
    value.versions = [
        {
            "Key": value.scope["buckets"]["lake"]["exact_keys"][0],
            "VersionId": "version-1",
            "IsLatest": False,
            "Size": 100,
            "ETag": '"version-etag"',
            "LastModified": NOW,
        }
    ]
    value.markers = [
        {
            "Key": value.versions[0]["Key"],
            "VersionId": "marker-1",
            "IsLatest": True,
            "LastModified": NOW,
        }
    ]
    value.objects = [
        {"Key": "queries/result.csv", "ETag": '"result-etag"', "Size": 16, "LastModified": NOW}
    ]
    value.uploads = [
        {
            "bucket": lake,
            "Key": "warehouse/building_proximity/pending.parquet",
            "UploadId": "upload-1",
            "Initiated": NOW,
        }
    ]
    value.s3 = Mock(
        spec=[
            "get_bucket_location",
            "get_bucket_versioning",
            "list_object_versions",
            "list_objects_v2",
            "list_multipart_uploads",
            "delete_object",
            "abort_multipart_upload",
            "list_parts",
            "close",
        ]
    )
    value.s3.get_bucket_location.return_value = {"LocationConstraint": "eu-north-1"}
    value.s3.get_bucket_versioning.side_effect = (
        lambda **kw: {"Status": "Enabled"} if kw["Bucket"] == lake else {}
    )
    value.s3.list_object_versions.side_effect = lambda **kw: {
        "Name": lake,
        "IsTruncated": False,
        "Versions": copy.deepcopy(value.versions),
        "DeleteMarkers": copy.deepcopy(value.markers),
    }
    value.s3.list_objects_v2.side_effect = lambda **kw: {
        "Name": results,
        "IsTruncated": False,
        "Contents": copy.deepcopy(value.objects),
    }
    value.s3.list_multipart_uploads.side_effect = lambda **kw: {
        "Bucket": kw["Bucket"],
        "IsTruncated": False,
        "Uploads": [
            {k: v for k, v in row.items() if k != "bucket"}
            for row in value.uploads
            if row["bucket"] == kw["Bucket"]
        ],
    }

    def delete(**kw):
        if "VersionId" in kw:
            value.versions[:] = [
                r
                for r in value.versions
                if (r["Key"], r["VersionId"]) != (kw["Key"], kw["VersionId"])
            ]
            value.markers[:] = [
                r
                for r in value.markers
                if (r["Key"], r["VersionId"]) != (kw["Key"], kw["VersionId"])
            ]
            return {"VersionId": kw["VersionId"]}
        if any(r["ETag"] != kw["IfMatch"] for r in value.objects if r["Key"] == kw["Key"]):
            raise ServiceError("PreconditionFailed", 412)
        value.objects[:] = [r for r in value.objects if r["Key"] != kw["Key"]]
        return {}

    def abort(**kw):
        value.uploads[:] = [
            r
            for r in value.uploads
            if (r["bucket"], r["Key"], r["UploadId"]) != (kw["Bucket"], kw["Key"], kw["UploadId"])
        ]
        return {}

    def parts(**kw):
        if any(r["UploadId"] == kw["UploadId"] for r in value.uploads):
            return {
                "Bucket": kw["Bucket"],
                "Key": kw["Key"],
                "UploadId": kw["UploadId"],
                "IsTruncated": False,
                "Parts": [{"PartNumber": 1, "Size": 5}],
            }
        raise ServiceError("NoSuchUpload")

    value.s3.delete_object.side_effect = delete
    value.s3.abort_multipart_upload.side_effect = abort
    value.s3.list_parts.side_effect = parts
    return value
