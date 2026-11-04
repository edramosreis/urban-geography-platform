"""Bind a first-trial stop scope to explicit pinned controller receipts; never call AWS."""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from athena_control import canonical, verified_plan
from athena_read_plan import same
from athena_runner import pinned_record
from dbt_view_plan import VIEWS, compiled_models, normalized_sql
from trial_cleanup_inventory import _text, cleanup_scope
from trial_service_inventory import MAX_CALLS as INVENTORY_CALLS

MAX_QUERIES = 3
MAX_ROUNDS = 8
POLL_SECONDS = 5
STOP_SECONDS = 180
MAX_OWN_CALLS = 4 + 1 + 4 + MAX_ROUNDS * 5
MAX_CALLS = 2 * INVENTORY_CALLS + MAX_OWN_CALLS


@dataclass(frozen=True)
class Record:
    path: Path
    sha256: str


@dataclass(frozen=True)
class QueryEvidence:
    intent: Record
    receipt: Record
    parent: Record | None = None


def evidence_records(job: Record | None, queries: tuple[QueryEvidence, ...]) -> list[Record]:
    records = [] if job is None else [job]
    for query in queries:
        records.extend([query.intent, query.receipt])
        if query.parent is not None:
            records.append(query.parent)
    return records


def recheck_evidence(
    release: Path, pin: str, job: Record | None, queries: tuple[QueryEvidence, ...]
) -> None:
    # The full release was checked at preparation. Stop identity depends on these
    # immutable manifest/receipt bytes, not on repeatedly hashing large JAR payloads.
    pinned_record(release / "release.json", pin)
    for record in evidence_records(job, queries):
        pinned_record(record.path, record.sha256)


def _quiescence_plan(
    release: Path, pin: str, job: Record | None, queries: tuple[QueryEvidence, ...]
) -> dict[str, Any]:
    scope = cleanup_scope(release, pin)
    if not isinstance(queries, tuple) or len(queries) > MAX_QUERIES:
        raise ValueError("First trial permits at most three explicitly bound query receipts")
    source_summary = pinned_record(release / "release.json", pin)["identity"]["summary_sha256"]
    job_binding = None
    if job is not None:
        receipt = pinned_record(job.path, job.sha256)
        if (
            receipt.get("job_name") != scope["expected_services"]["job"]
            or receipt.get("release_id") != scope["release_id"]
            or receipt.get("release_sha256") != pin
            or type(receipt.get("terminal_confirmed")) is not bool
        ):
            raise ValueError("Glue controller receipt differs from the pinned trial release")
        run_id = _text(receipt.get("run_id"), 255)
        if any(ord(c) < 32 for c in run_id):
            raise ValueError("Invalid saved Glue run identity")
        job_binding = {"id": run_id, "receipt_sha256": job.sha256}
    bindings = []
    identities: set[str] = set()
    kinds: set[str] = set()
    snapshot = None
    for evidence in queries:
        intent = pinned_record(evidence.intent.path, evidence.intent.sha256)
        receipt = pinned_record(evidence.receipt.path, evidence.receipt.sha256)
        if intent.get("contract_version") != 1:
            raise ValueError("Unsupported saved query intent")
        if evidence.parent is None:
            source = verified_plan(intent["plan"])
            kind = "fact_select"
        else:
            parent = pinned_record(evidence.parent.path, evidence.parent.sha256)
            if (
                parent.get("contract_version") != 1
                or intent.get("parent_intent_sha256") != evidence.parent.sha256
            ):
                raise ValueError("View query does not bind its explicit parent plan")
            source = verified_plan(parent["source_plan"])
            models = compiled_models(source["binding"])
            schema = source["binding"]["source_schema"]
            expected = [
                {
                    "name": name,
                    "sql": f'create or replace view "AwsDataCatalog"."{schema}"."{name}" as\n'
                    + models[name],
                }
                for name in VIEWS
            ]
            for row in expected:
                row["sql_sha256"] = hashlib.sha256(row["sql"].encode()).hexdigest()
            if not same(parent.get("queries"), expected):
                raise ValueError("View parent differs from the fixed two-view recipe")
            matches = [
                row["name"]
                for row in expected
                if normalized_sql(intent["request"]["QueryString"]) == normalized_sql(row["sql"])
            ]
            if len(matches) != 1:
                raise ValueError("Saved view query is outside the fixed two-view recipe")
            kind = matches[0]
        if (
            not same(source["target"], scope["target"])
            or source["binding"]["source_run_sha256"] != source_summary
        ):
            raise ValueError("Saved query source differs from the frozen trial")
        current = source["binding"]["snapshot_id"]
        if snapshot is not None and snapshot != current:
            raise ValueError("Saved trial queries disagree on their writer snapshot")
        snapshot = current
        request = intent["request"]
        sql = _text(request.get("QueryString"), 16384)
        token = request.get("ClientRequestToken")
        if not isinstance(token, str) or re.fullmatch(r"[0-9a-f]{64}", token) is None:
            raise ValueError("Saved query request token is invalid")
        expected_request = source["request_template"] | {
            "QueryString": sql,
            "ClientRequestToken": token,
        }
        if not same(request, expected_request) or (
            kind == "fact_select" and sql != source["request_template"]["QueryString"]
        ):
            raise ValueError("Saved query request differs from the dedicated trial recipe")
        sql_pin = hashlib.sha256(sql.encode()).hexdigest()
        if evidence.parent is not None and not same(
            intent["plan"], {"query_sha256": sql_pin, "workgroup": source["workgroup"]}
        ):
            raise ValueError("View intent SQL/workgroup differs")
        identity = receipt.get("query_id")
        if (
            not isinstance(identity, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", identity) is None
            or receipt.get("intent_sha256") != evidence.intent.sha256
            or receipt.get("query_sha256") != sql_pin
            or type(receipt.get("terminal_confirmed")) is not bool
            or identity in identities
            or kind in kinds
        ):
            raise ValueError("Saved query receipt identity, hashes or uniqueness differs")
        identities.add(identity)
        kinds.add(kind)
        bindings.append(
            {
                "id": identity,
                "kind": kind,
                "query_sha256": sql_pin,
                "intent_sha256": evidence.intent.sha256,
                "receipt_sha256": evidence.receipt.sha256,
                "parent_sha256": None if evidence.parent is None else evidence.parent.sha256,
            }
        )
    return {
        "version": 1,
        "status": "prepared_first_trial_quiescence",
        "release_manifest_sha256": pin,
        "scope": scope,
        "job_run": job_binding,
        "queries": bindings,
        "snapshot_id": snapshot,
        "disable_request": {
            "WorkGroup": scope["expected_services"]["workgroup"],
            "State": "DISABLED",
        },
        "limits": {
            "max_queries": MAX_QUERIES,
            "max_job_runs": 1,
            "max_rounds": MAX_ROUNDS,
            "poll_seconds": POLL_SECONDS,
            "stop_seconds": STOP_SECONDS,
            "max_own_calls": MAX_OWN_CALLS,
            "max_total_calls": MAX_CALLS,
        },
        "execution_authorized": False,
        "deletion_authorized": False,
    }


def plan_sha256(plan: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(plan)).hexdigest()


def quiescence_plan(
    release: Path, pin: str, job: Record | None, queries: tuple[QueryEvidence, ...]
) -> dict[str, Any]:
    try:
        return _quiescence_plan(release, pin, job, queries)
    except (KeyError, TypeError, AttributeError):
        raise ValueError("Malformed saved quiescence evidence") from None
