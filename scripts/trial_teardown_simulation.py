"""Evolving, explicitly synthetic AWS state for the joined teardown rehearsal only."""

import copy
import hashlib
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from athena_read_plan import read_plan
from aws_login_bridge import read_record
from dbt_view_plan import VIEWS, compiled_models
from trial_cleanup_command import Operation
from trial_cleanup_inventory import cleanup_scope
from trial_cleanup_plan import TABLE_ORDER
from trial_infrastructure_plan import absence_plan
from trial_quiescence_plan import QueryEvidence, Record

MODES = (
    "clean",
    "partial_delete",
    "changed_target",
    "unbound_work",
    "infra_leftover",
    "inspector_denied",
)
STAMP = datetime(2026, 11, 11, tzinfo=UTC)
SOURCE_KEY = "ASIA" + "A" * 16
ASSUMED_KEY = "ASIA" + "B" * 16


class SimulatedError(Exception):
    """A deliberately modeled provider failure, never a real AWS response."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__("synthetic provider failure")
        self.code, self.status = code, status
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


def record(path: Path, value: dict[str, Any]) -> Record:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_once(path, canonical(value))
    return saved(path)


def saved(path: Path) -> Record:
    _, pin = read_record(path)
    return Record(path, pin)


class Simulation:
    """One bounded world shared by all phases; no network or SDK dependency."""

    def __init__(self, release: Path, pin: str, principal: str, mode: str) -> None:
        if mode not in MODES:
            raise ValueError("Unknown teardown rehearsal scenario")
        self.release, self.pin, self.principal, self.mode = release, pin, principal, mode
        self.scope = cleanup_scope(release, pin)
        self.plan = absence_plan(release, pin, principal)
        self.owner = self.scope["target"]["account_id"]
        self.expected = self.scope["expected_services"]
        self.lake, self.results = (self.scope["buckets"][k]["name"] for k in ("lake", "results"))
        warehouse = "s3://" + self.lake + "/warehouse/"
        self.database = {
            "Name": self.expected["database"],
            "CatalogId": self.owner,
            "LocationUri": warehouse,
        }
        deployment = self.scope["target"]["deployment_name"]
        self.cleanup_name = self.scope["cleanup_role_arn"].rsplit("/", 1)[1]
        self.job = {
            "Name": self.expected["job"],
            "Role": f"arn:aws:iam::{self.owner}:role/service-role/{deployment}-glue-writer",
            "Command": {
                "ScriptLocation": "s3://"
                + self.lake
                + "/runtime/releases/"
                + self.scope["release_id"]
                + "/run_glue_s3_writer.py"
            },
        }
        self.runs = {
            "jr_owned": {
                "Id": "jr_owned",
                "JobName": self.expected["job"],
                "JobRunState": "RUNNING",
            }
        }
        if mode == "unbound_work":
            self.runs["jr_unbound"] = self.runs["jr_owned"] | {"Id": "jr_unbound"}
        self.group = {
            "Name": self.expected["workgroup"],
            "State": "ENABLED",
            "Configuration": {
                "EnforceWorkGroupConfiguration": True,
                "ResultConfiguration": {
                    "OutputLocation": "s3://" + self.results + "/queries/",
                    "ExpectedBucketOwner": self.owner,
                },
            },
        }
        self.catalog: dict[str, Any] = {}
        for name in TABLE_ORDER:
            row: dict[str, Any] = {
                "Name": name,
                "CatalogId": self.owner,
                "DatabaseName": self.expected["database"],
                "TableType": "EXTERNAL_TABLE" if name == "building_proximity" else "VIRTUAL_VIEW",
                "CreateTime": STAMP,
                "VersionId": "1",
                "Parameters": {},
            }
            if name == "building_proximity":
                location = warehouse + name
                row.update(
                    StorageDescriptor={"Location": location},
                    Parameters={
                        "table_type": "ICEBERG",
                        "metadata_location": location + "/metadata/v1.metadata.json",
                    },
                )
            self.catalog[name] = row
        key = self.scope["buckets"]["lake"]["exact_keys"][0]
        self.versions = [
            {
                "Key": key,
                "VersionId": "version-1",
                "IsLatest": False,
                "Size": 100,
                "ETag": '"version-etag"',
                "LastModified": STAMP,
            }
        ]
        self.markers = [
            {"Key": key, "VersionId": "marker-1", "IsLatest": True, "LastModified": STAMP}
        ]
        self.objects = [
            {
                "Key": "queries/result.csv",
                "ETag": '"result-etag"',
                "Size": 16,
                "LastModified": STAMP,
            }
        ]
        self.uploads = [
            {
                "Key": "warehouse/building_proximity/pending.parquet",
                "UploadId": "upload-1",
                "Initiated": STAMP,
            }
        ]
        self.queries: dict[str, Any] = {}
        self.remaining = {(t["kind"], t["name"]) for t in self.plan["targets"]}
        self.phase = "setup"
        self.session = ""
        self.removed = False
        self.failed_delete = False
        self.journal: list[dict[str, str]] = []
        self.unexpected: list[str] = []

    def inputs(self, folder: Path) -> Operation:
        """Create synthetic original run/query evidence, never pretend the writer ran."""
        manifest, _ = read_record(self.release / "release.json")
        source = read_plan(
            **self.scope["target"],
            snapshot_id=123,
            source_run_sha256=manifest["identity"]["summary_sha256"],
        )
        models = compiled_models(source["binding"])
        schema = source["binding"]["source_schema"]
        rows = [
            {
                "name": name,
                "sql": f'create or replace view "AwsDataCatalog"."{schema}"."{name}" as\n'
                + models[name],
            }
            for name in VIEWS
        ]
        for row in rows:
            row["sql_sha256"] = hashlib.sha256(row["sql"].encode()).hexdigest()
        parent = record(
            folder / "parent.json", {"contract_version": 1, "source_plan": source, "queries": rows}
        )
        job = record(
            folder / "job.json",
            {
                "job_name": self.expected["job"],
                "run_id": "jr_owned",
                "release_id": self.scope["release_id"],
                "release_sha256": self.pin,
                "outcome": "stop_unconfirmed",
                "terminal_confirmed": False,
            },
        )
        evidence = []
        for index, sql in enumerate(
            [source["request_template"]["QueryString"], *[r["sql"] for r in rows]]
        ):
            request = source["request_template"] | {
                "QueryString": sql,
                "ClientRequestToken": str(index + 1) * 64,
            }
            sql_pin = hashlib.sha256(sql.encode()).hexdigest()
            intent = {
                "contract_version": 1,
                "request": request,
                "plan": source
                if index == 0
                else {"query_sha256": sql_pin, "workgroup": source["workgroup"]},
            }
            if index:
                intent["parent_intent_sha256"] = parent.sha256
            original = record(folder / f"intent-{index}.json", intent)
            query_id = f"query-{index}"
            receipt = record(
                folder / f"receipt-{index}.json",
                {
                    "query_id": query_id,
                    "intent_sha256": original.sha256,
                    "query_sha256": sql_pin,
                    "terminal_confirmed": False,
                    "outcome": "stop_unconfirmed",
                },
            )
            evidence.append(QueryEvidence(original, receipt, parent if index else None))
            self.queries[query_id] = {
                "QueryExecutionId": query_id,
                "WorkGroup": self.expected["workgroup"],
                "QueryExecutionContext": request["QueryExecutionContext"],
                "Query": sql,
                "Status": {"State": "RUNNING"},
            }
        inventory = record(folder / "inventory-plan.json", self.scope)
        return Operation(
            self.release,
            self.pin,
            inventory,
            self.principal,
            "quiesce",
            job,
            tuple(evidence),
            paused=True,
        )

    def counts(self) -> dict[str, int]:
        return {
            "catalog": len(self.catalog),
            "versions": len(self.versions),
            "markers": len(self.markers),
            "objects": len(self.objects),
            "uploads": len(self.uploads),
            "infrastructure": len(self.remaining),
        }

    def remove_infrastructure(self, proof: dict[str, Any]) -> None:
        """An in-memory transition, not Terraform or a verification of destruction order."""
        if (
            self.removed
            or proof.get("phase") != "delete-data"
            or proof.get("data_cleanup_observed") is not True
            or proof.get("requires_review") is not False
            or any(v for k, v in self.counts().items() if k != "infrastructure")
        ):
            raise ValueError("Infrastructure simulation requires verified data absence")
        self.remaining.clear()
        if self.mode == "infra_leftover":
            self.remaining.add(("role", self.scope["target"]["deployment_name"] + "-trial-cleanup"))
        self.removed = True

    @staticmethod
    def expect(actual: dict[str, Any], expected: dict[str, Any]) -> None:
        if actual != expected:
            raise ValueError("Unexpected synthetic request parameters")

    def respond(
        self, service: str, method: str, params: dict[str, Any], source: bool
    ) -> dict[str, Any]:
        self.journal.append(
            {
                "phase": self.phase,
                "service": service,
                "method": method,
                "identity": "source" if source else "cleanup",
            }
        )
        try:
            return copy.deepcopy(self._respond(service, method, params, source))
        except SimulatedError:
            raise
        except Exception:
            self.unexpected.append(service + "." + method)
            raise

    def _respond(
        self, service: str, method: str, p: dict[str, Any], source: bool
    ) -> dict[str, Any]:
        if service == "sts":
            if method == "get_caller_identity":
                self.expect(p, {})
                name = (
                    self.principal.rsplit("/", 1)[1]
                    if source
                    else self.scope["cleanup_role_arn"].rsplit("/", 1)[1]
                )
                session = "browser" if source else self.session
                return {
                    "Account": self.owner,
                    "Arn": f"arn:aws:sts::{self.owner}:assumed-role/{name}/{session}",
                    "UserId": "AROA" + "C" * 17 + ":" + session,
                }
            if method == "assume_role" and source and not self.removed:
                self.session = p.get("RoleSessionName", "")
                if re.fullmatch(r"trial-cleanup-[0-9a-f]{32}", self.session) is None:
                    raise ValueError("Unexpected simulated session")
                self.expect(
                    p,
                    {
                        "RoleArn": self.scope["cleanup_role_arn"],
                        "RoleSessionName": self.session,
                        "DurationSeconds": 3600,
                    },
                )
                return {
                    "Credentials": {
                        "AccessKeyId": ASSUMED_KEY,
                        "SecretAccessKey": "s" * 40,
                        "SessionToken": "dummyTeardownToken",
                        "Expiration": datetime.now(UTC) + timedelta(seconds=3500),
                    },
                    "AssumedRoleUser": {
                        "Arn": (
                            f"arn:aws:sts::{self.owner}:assumed-role/"
                            f"{self.cleanup_name}/{self.session}"
                        ),
                        "AssumedRoleId": "AROA" + "C" * 17 + ":" + self.session,
                    },
                }
            raise ValueError("Unsupported synthetic STS request")
        if self.phase == "inspect-infrastructure":
            if not source or not self.removed:
                raise ValueError(
                    "Infrastructure observation requires outside source after transition"
                )
            return self._infrastructure(service, method, p)
        if source or self.removed:
            raise ValueError("Data operations require scoped cleanup session before transition")
        if service == "s3":
            return self._s3(method, p)
        if service == "glue":
            return self._glue(method, p)
        if service == "athena":
            return self._athena(method, p)
        raise ValueError("Unsupported synthetic service")

    def _glue(self, method: str, p: dict[str, Any]) -> dict[str, Any]:
        base = {"CatalogId": self.owner, "DatabaseName": self.expected["database"]}
        if method == "get_database":
            self.expect(p, {"CatalogId": self.owner, "Name": self.expected["database"]})
            return {"Database": self.database}
        if method == "get_tables":
            self.expect(p, base | {"AttributesToGet": ["NAME", "TABLE_TYPE"], "MaxResults": 10})
            return {
                "TableList": [
                    {"Name": r["Name"], "TableType": r["TableType"]} for r in self.catalog.values()
                ]
            }
        if method in {"get_table", "delete_table"}:
            name = p.get("Name")
            if name not in TABLE_ORDER:
                raise ValueError("Unscoped synthetic table")
            self.expect(p, base | {"Name": name})
            if method == "get_table":
                if name not in self.catalog:
                    raise SimulatedError("EntityNotFoundException", 400)
                return {"Table": self.catalog[name]}
            del self.catalog[name]
            return {}
        if method == "get_job":
            self.expect(p, {"JobName": self.expected["job"]})
            return {"Job": self.job}
        if method == "get_job_runs":
            self.expect(p, {"JobName": self.expected["job"], "MaxResults": 10})
            return {"JobRuns": list(self.runs.values())}
        if method == "get_job_run":
            self.expect(
                p,
                {
                    "JobName": self.expected["job"],
                    "RunId": "jr_owned",
                    "PredecessorsIncluded": False,
                },
            )
            return {"JobRun": self.runs["jr_owned"]}
        if method == "batch_stop_job_run":
            self.expect(p, {"JobName": self.expected["job"], "JobRunIds": ["jr_owned"]})
            self.runs["jr_owned"]["JobRunState"] = "STOPPED"
            return {
                "SuccessfulSubmissions": [
                    {"JobName": self.expected["job"], "JobRunId": "jr_owned"}
                ],
                "Errors": [],
            }
        raise ValueError("Unsupported synthetic Glue request")

    def _athena(self, method: str, p: dict[str, Any]) -> dict[str, Any]:
        if method == "get_work_group":
            self.expect(p, {"WorkGroup": self.expected["workgroup"]})
            return {"WorkGroup": self.group}
        if method == "update_work_group":
            self.expect(p, {"WorkGroup": self.expected["workgroup"], "State": "DISABLED"})
            self.group["State"] = "DISABLED"
            return {}
        if method == "list_query_executions":
            self.expect(p, {"WorkGroup": self.expected["workgroup"], "MaxResults": 10})
            return {"QueryExecutionIds": list(self.queries)}
        if method in {"get_query_execution", "stop_query_execution"}:
            identity = p.get("QueryExecutionId")
            if identity not in self.queries:
                raise ValueError("Unbound synthetic query")
            self.expect(p, {"QueryExecutionId": identity})
            if method == "get_query_execution":
                return {"QueryExecution": self.queries[identity]}
            self.queries[identity]["Status"]["State"] = "CANCELLED"
            return {}
        raise ValueError("Unsupported synthetic Athena request")

    def _s3(self, method: str, p: dict[str, Any]) -> dict[str, Any]:
        bucket = p.get("Bucket")
        if bucket not in {self.lake, self.results}:
            raise ValueError("Unscoped synthetic bucket")
        base = {"Bucket": bucket, "ExpectedBucketOwner": self.owner}
        if method in {"get_bucket_location", "get_bucket_versioning"}:
            self.expect(p, base)
            return (
                {"LocationConstraint": "eu-north-1"}
                if method == "get_bucket_location"
                else ({"Status": "Enabled"} if bucket == self.lake else {})
            )
        if method == "list_object_versions" and bucket == self.lake:
            self.expect(p, base | {"MaxKeys": 100})
            return {
                "Name": bucket,
                "IsTruncated": False,
                "Versions": self.versions,
                "DeleteMarkers": self.markers,
            }
        if method == "list_objects_v2" and bucket == self.results:
            self.expect(p, base | {"MaxKeys": 100})
            return {"Name": bucket, "IsTruncated": False, "Contents": self.objects}
        if method == "list_multipart_uploads":
            self.expect(p, base | {"MaxUploads": 100})
            return {
                "Bucket": bucket,
                "IsTruncated": False,
                "Uploads": self.uploads if bucket == self.lake else [],
            }
        if method == "delete_object":
            if bucket == self.lake:
                matches = [
                    r
                    for r in self.versions + self.markers
                    if r["Key"] == p.get("Key") and r["VersionId"] == p.get("VersionId")
                ]
                if len(matches) != 1:
                    raise ValueError("Unselected synthetic version")
                self.expect(
                    p, base | {"Key": matches[0]["Key"], "VersionId": matches[0]["VersionId"]}
                )
                if self.mode == "partial_delete" and not self.failed_delete:
                    self.failed_delete = True
                    raise SimulatedError("AccessDenied", 403)
                self.versions[:] = [r for r in self.versions if r not in matches]
                self.markers[:] = [r for r in self.markers if r not in matches]
                return {"VersionId": p["VersionId"]}
            matches = [r for r in self.objects if r["Key"] == p.get("Key")]
            if len(matches) != 1:
                raise ValueError("Unselected synthetic result")
            self.expect(p, base | {"Key": matches[0]["Key"], "IfMatch": matches[0]["ETag"]})
            self.objects.remove(matches[0])
            return {}
        if method in {"abort_multipart_upload", "list_parts"} and bucket == self.lake:
            expected = base | {
                "Key": "warehouse/building_proximity/pending.parquet",
                "UploadId": "upload-1",
            }
            self.expect(p, expected | ({"MaxParts": 1} if method == "list_parts" else {}))
            if method == "abort_multipart_upload":
                self.uploads.clear()
                return {}
            if self.uploads:
                return {
                    "Bucket": bucket,
                    "Key": p["Key"],
                    "UploadId": p["UploadId"],
                    "IsTruncated": False,
                    "Parts": [{"PartNumber": 1, "Size": 5}],
                }
            raise SimulatedError("NoSuchUpload", 404)
        raise ValueError("Unsupported synthetic S3 request")

    def _infrastructure(self, service: str, method: str, p: dict[str, Any]) -> dict[str, Any]:
        targets = self.plan["targets"]
        if service == "s3" and method == "list_objects_v2":
            if p.get("Bucket") not in {self.lake, self.results}:
                raise ValueError("Unscoped synthetic bucket inspection")
            self.expect(p, {"Bucket": p["Bucket"], "ExpectedBucketOwner": self.owner, "MaxKeys": 1})
            raise SimulatedError("NoSuchBucket", 404)
        if service == "glue" and method in {"get_database", "get_job"}:
            self.expect(
                p,
                {"CatalogId": self.owner, "Name": self.expected["database"]}
                if method == "get_database"
                else {"JobName": self.expected["job"]},
            )
            raise SimulatedError("EntityNotFoundException", 400)
        if service == "iam" and method in {"get_role", "get_policy"}:
            kind = method[4:]
            matches = [
                t
                for t in targets
                if t["kind"] == kind
                and (
                    t["name"] == p.get("RoleName")
                    if kind == "role"
                    else t["arn"] == p.get("PolicyArn")
                )
            ]
            if len(matches) != 1:
                raise ValueError("Unscoped synthetic IAM inspection")
            t = matches[0]
            self.expect(p, {"RoleName": t["name"]} if kind == "role" else {"PolicyArn": t["arn"]})
            if self.mode == "inspector_denied" and kind == "policy":
                raise SimulatedError("AccessDenied", 403)
            if (kind, t["name"]) in self.remaining:
                return {
                    "Role": {
                        "RoleName": t["name"],
                        "Arn": t["arn"],
                        "Path": "/service-role/",
                        "RoleId": "AROA" + "D" * 17,
                        "CreateDate": STAMP,
                    }
                }
            raise SimulatedError("NoSuchEntity", 404)
        if service == "athena" and method == "list_work_groups":
            self.expect(p, {"MaxResults": 50})
            return {"WorkGroups": []}
        if service == "logs" and method == "describe_log_groups":
            self.expect(
                p,
                {
                    "logGroupIdentifiers": sorted(
                        t["name"] for t in targets if t["kind"] == "log_group"
                    ),
                    "includeLinkedAccounts": False,
                },
            )
            return {"logGroups": []}
        raise ValueError("Unsupported synthetic infrastructure request")
