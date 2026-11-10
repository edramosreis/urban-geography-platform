"""Observe only the named post-teardown resources; missing access never proves absence."""

import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from athena_runner import pinned_record
from aws_role_session import verify_source_identity
from trial_cleanup_inventory import _save, validate_inventory_output
from trial_infrastructure_plan import (
    MAX_CALLS,
    MAX_PAGES,
    MAX_RECORD_BYTES,
    OBSERVATION_SECONDS,
    PAGE_SIZE,
    SERVICES,
    absence_plan,
    plan_sha256,
)


class _Reader:
    def __init__(self, clients: dict[str, Any], release: Path, pin: str, now: Callable[[], float]):
        self.clients, self.release, self.pin, self.now = clients, release, pin, now
        self.deadline = now() + OBSERVATION_SECONDS
        self.calls = 0

    def check(self) -> None:
        if self.now() >= self.deadline:
            raise TimeoutError("Infrastructure observation deadline reached")
        pinned_record(self.release / "release.json", self.pin)

    def call(self, service: str, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self.check()
        if self.calls >= MAX_CALLS:
            raise TimeoutError("Infrastructure observation API ceiling reached")
        self.calls += 1
        value = getattr(self.clients[service], method)(**params)
        if not isinstance(value, dict):
            raise ValueError("Malformed infrastructure reply")
        return value


def _error(error: Exception) -> tuple[str | None, int | None, bool]:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None, None, False
    details = response.get("Error", {})
    code = details.get("Code") if isinstance(details, dict) else None
    code = code if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", code) else None
    metadata = response.get("ResponseMetadata", {})
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
    federated = response.get("FromFederationSource")
    return (
        code,
        status if type(status) is int else None,
        federated is not None and federated is not False,
    )


def _outcome(
    target: dict[str, str], state: str, reason: str, error: Exception | None = None
) -> dict[str, Any]:
    return {
        "kind": target["kind"],
        "name": target["name"],
        "state": state,
        "reason": reason,
        "error_type": None if error is None else type(error).__name__,
        "error_code": None if error is None else _error(error)[0],
    }


def _direct(reader: _Reader, target: dict[str, str], owner: str) -> dict[str, Any]:
    kind, name = target["kind"], target["name"]
    if kind == "bucket":
        service, method, params, field = (
            "s3",
            "list_objects_v2",
            {"Bucket": name, "ExpectedBucketOwner": owner, "MaxKeys": 1},
            None,
        )
        missing = ("NoSuchBucket", 404)
    elif kind == "database":
        service, method, params, field = (
            "glue",
            "get_database",
            {"CatalogId": owner, "Name": name},
            "Database",
        )
        missing = ("EntityNotFoundException", 400)
    elif kind == "job":
        service, method, params, field = "glue", "get_job", {"JobName": name}, "Job"
        missing = ("EntityNotFoundException", 400)
    else:
        service, method, params, field = (
            "iam",
            "get_" + kind,
            {"RoleName": name} if kind == "role" else {"PolicyArn": target["arn"]},
            kind.title(),
        )
        missing = ("NoSuchEntity", 404)
    try:
        response = reader.call(service, method, params)
        item = response if field is None else response[field]
        if not isinstance(item, dict):
            raise ValueError("Malformed infrastructure identity")
        key = kind.title() + "Name" if kind in {"role", "policy"} else "Name"
        if item.get(key) != name or ("arn" in target and item.get("Arn") != target["arn"]):
            raise ValueError("Infrastructure identity differs")
        if kind == "database" and item.get("CatalogId", owner) != owner:
            raise ValueError("Infrastructure catalog differs")
        return _outcome(target, "present", "matching_identity")
    except TimeoutError:
        raise
    except Exception as error:
        code, status, federated = _error(error)
        absent = (code, status) == missing and not (kind == "database" and federated)
        return _outcome(
            target,
            "absent" if absent else "unconfirmed",
            "typed_not_found" if absent else "read_unconfirmed",
            None if absent else error,
        )


def _listing(reader: _Reader, targets: list[dict[str, str]], service: str) -> list[dict[str, Any]]:
    wanted = {t["name"] for t in targets}
    params: dict[str, Any]
    if service == "athena":
        method, params, rows_key, name_key, token_key = (
            "list_work_groups",
            {"MaxResults": PAGE_SIZE},
            "WorkGroups",
            "Name",
            "NextToken",
        )
    else:
        method, params, rows_key, name_key, token_key = (
            "describe_log_groups",
            {"logGroupIdentifiers": sorted(wanted), "includeLinkedAccounts": False},
            "logGroups",
            "logGroupName",
            "nextToken",
        )
    found: set[str] = set()
    seen_names: set[str] = set()
    seen_tokens: set[str] = set()
    complete = False
    failure = None
    reason = "pagination_ceiling"
    try:
        for _ in range(MAX_PAGES):
            response = reader.call(service, method, params)
            rows = response[rows_key]
            if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
                raise ValueError("Infrastructure listing size differs")
            names = []
            for row in rows:
                name = row.get(name_key) if isinstance(row, dict) else None
                if (
                    not isinstance(name, str)
                    or not name
                    or len(name.encode()) > 512
                    or any(ord(c) < 32 for c in name)
                ):
                    raise ValueError("Infrastructure listing identity differs")
                names.append(name)
            if len(set(names)) != len(names) or seen_names.intersection(names):
                raise ValueError("Infrastructure listing repeats an identity")
            if service == "logs" and not set(names) <= wanted:
                raise ValueError("Exact log-group filter returned another name")
            seen_names.update(names)
            found.update(wanted.intersection(names))
            if found == wanted:
                break
            token = response.get(token_key)
            if token is None:
                complete = True
                break
            if (
                not isinstance(token, str)
                or not token
                or len(token.encode()) > 1024
                or token in seen_tokens
            ):
                raise ValueError("Infrastructure listing pagination differs")
            seen_tokens.add(token)
            params = params | {token_key: token}
    except TimeoutError:
        raise
    except Exception as error:
        failure, reason = error, "listing_unconfirmed"
    return [
        _outcome(t, "present", "matching_entry")
        if t["name"] in found
        else _outcome(t, "absent", "complete_listing")
        if complete
        else _outcome(t, "unconfirmed", reason, failure)
        for t in targets
    ]


def observe_absence(
    clients: dict[str, Any],
    release: Path,
    pin: str,
    principal: str,
    expected_plan_sha256: str,
    output: Path,
    *,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Clients are borrowed and must share the same source identity and fixed endpoints.

    The owned source-session connection enforces those bindings; this injected
    collector never creates credentials, assumes a role, closes clients or deletes.
    """
    plan = absence_plan(release, pin, principal)
    actual_pin = plan_sha256(plan)
    if expected_plan_sha256 != actual_pin or set(clients) != set(SERVICES):
        raise ValueError("Use the exact prepared absence plan and its six bound service clients")
    validate_inventory_output(release, output)
    reader = _Reader(clients, release, pin, now)
    output.mkdir(parents=True)
    _save(output / "plan.json", plan, MAX_RECORD_BYTES)
    started = datetime.now(UTC).isoformat()
    try:
        identity = reader.call("sts", "get_caller_identity", {})
        verify_source_identity(
            identity, {"owner": plan["target"]["account_id"], "source_role_arn": principal}
        )
        owner = plan["target"]["account_id"]
        results = {}
        for target in plan["targets"]:
            if target["kind"] not in {"workgroup", "log_group"}:
                result = _direct(reader, target, owner)
                results[(target["kind"], target["name"])] = result
        for kind, service in (("workgroup", "athena"), ("log_group", "logs")):
            selected = [t for t in plan["targets"] if t["kind"] == kind]
            for result in _listing(reader, selected, service):
                results[(result["kind"], result["name"])] = result
        observations = [results[(t["kind"], t["name"])] for t in plan["targets"]]
        reader.check()
        absent = all(row["state"] == "absent" for row in observations)
        for index, result in enumerate(observations):
            _save(output / f"target-{index:02}.json", result, MAX_RECORD_BYTES)
        report = {
            "version": 1,
            "status": "named_infrastructure_absence_observed"
            if absent
            else "infrastructure_absence_requires_review",
            "plan_sha256": actual_pin,
            "release_manifest_sha256": pin,
            "source_identity_verified": True,
            "targets": observations,
            "api_calls": reader.calls,
            "started_at_utc": started,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "all_targets_confirmed": all(r["state"] != "unconfirmed" for r in observations),
            "named_infrastructure_absence_observed": absent,
            "requires_review": not absent,
            "scope": "explicit_resource_names_only",
            "deletion_performed": False,
            "terraform_state_verified": False,
            "billing_zero_verified": False,
            "unmanaged_resources_checked": False,
            "orphaned_service_metadata_checked": False,
            "future_submissions_fenced": False,
        }
        _save(output / "inventory.json", report, MAX_RECORD_BYTES)
        return report
    except BaseException as error:
        try:
            _save(
                output / "failure.json",
                {
                    "status": "infrastructure_absence_failed",
                    "plan_sha256": actual_pin,
                    "error_type": type(error).__name__,
                    "api_calls": reader.calls,
                    "named_infrastructure_absence_observed": False,
                },
                MAX_RECORD_BYTES,
            )
        except Exception:
            pass
        raise
