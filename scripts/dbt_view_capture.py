"""Preserve two bounded Glue table definitions for review without accepting their format."""

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from athena_control import canonical, verified_plan, write_once
from athena_preflight import expected_caller
from athena_read_plan import ARTIFACTS, same
from aws_login_bridge import decode, read_record
from dbt_view_catalog import CatalogClient, view_state
from dbt_view_plan import VIEWS

MAX_CAPTURE_BYTES = 256 * 1024
MANIFEST = "capture.json"


def _date(value: Any) -> str:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Unsupported metadata value or timezone")
    return value.astimezone(UTC).isoformat()


def _encode(value: dict[str, Any]) -> bytes:
    content = bytearray()
    try:
        encoder = json.JSONEncoder(
            sort_keys=True, ensure_ascii=False, allow_nan=False, default=_date
        )
        for part in encoder.iterencode(value):
            encoded = part.encode("utf-8")
            if len(content) + len(encoded) + 1 > MAX_CAPTURE_BYTES:
                raise ValueError("View metadata exceeds the capture ceiling")
            content.extend(encoded)
    except (TypeError, RecursionError):
        raise ValueError("Unsupported metadata JSON value") from None
    return bytes(content) + b"\n"


def _intent(plan: dict[str, Any], pin: str, caller: dict[str, Any]) -> dict[str, Any]:
    source = verified_plan(plan["source_plan"])
    target = source["target"]
    arn, user_id = caller.get("Arn"), caller.get("UserId")
    if not isinstance(arn, str) or not isinstance(user_id, str):
        raise ValueError("Use an explicit builder caller")
    expected = expected_caller(target["account_id"], arn, user_id)
    if (
        not isinstance(pin, str)
        or re.fullmatch(r"[0-9a-f]{64}", pin) is None
        or not same(caller, expected)
        or re.fullmatch(
            re.escape(
                f"arn:aws:sts::{target['account_id']}:assumed-role/"
                f"{target['deployment_name']}-dbt-view-builder/"
            )
            + r"dbt-builder-[0-9a-f]{32}",
            expected["Arn"],
        )
        is None
    ):
        raise ValueError("Use the pinned plan and its scoped builder caller")
    return {
        "version": 1,
        "view_plan_sha256": pin,
        "project_receipt_sha256": plan["project_receipt_sha256"],
        "target": target,
        "binding": source["binding"],
        "caller": expected,
        "views": list(VIEWS),
        "max_get_table_calls": 2,
        "max_record_bytes": MAX_CAPTURE_BYTES,
        "timestamp_encoding": "utc_iso8601",
    }


def _request(plan: dict[str, Any], name: str) -> dict[str, str]:
    source = plan["source_plan"]
    return {
        "CatalogId": source["target"]["account_id"],
        "DatabaseName": source["binding"]["source_schema"],
        "Name": name,
    }


def _table(record: dict[str, Any], plan: dict[str, Any], name: str) -> dict[str, Any]:
    expected = _request(plan, name)
    table = record.get("table")
    if (
        set(record) != {"version", "request", "table"}
        or not same(record["version"], 1)
        or not same(record["request"], expected)
        or not isinstance(table, dict)
        or not same({k: table.get(k) for k in expected}, expected)
    ):
        raise ValueError("Captured catalog identity differs from the requested Gold view")
    _encode(record)  # Reject non-finite/unsupported values even when loading changed files.
    return table


def _review(table: dict[str, Any], plan: dict[str, Any], name: str) -> dict[str, Any]:
    try:
        state = view_state(table, plan, name)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        return {"compatibility": "requires_review", "definition_sha256": None}
    return {
        "compatibility": "matches_current_checker",
        "definition_sha256": state["definition_sha256"],
    }


def _manifest(plan_pin: str, intent_pin: str, records: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": 1,
        "status": "view_metadata_captured",
        "view_plan_sha256": plan_pin,
        "intent_sha256": intent_pin,
        "records": records,
        "compatibility": "requires_review"
        if any(r["compatibility"] == "requires_review" for r in records.values())
        else "matches_current_checker",
        "view_catalog_verified": False,
        "result_verified": False,
    }


def capture_views(
    client: CatalogClient,
    plan: dict[str, Any],
    pin: str,
    caller: dict[str, Any],
    output: Path,
) -> str:
    """Two exact GetTable calls, no retry/list/mutation; preserve unsupported definitions."""
    intent = _intent(plan, pin, caller)
    destination = output.resolve()
    if (
        output.exists()
        or output.is_symlink()
        or not output.parent.is_dir()
        or output.parent.is_symlink()
        or destination == ARTIFACTS.resolve()
        or not destination.is_relative_to(ARTIFACTS.resolve())
    ):
        raise ValueError("Use fresh metadata output under artifacts with an existing parent")
    output.mkdir()
    content = canonical(intent)
    write_once(output / "intent.json", content)
    records: dict[str, Any] = {}
    name = VIEWS[0]
    written = 0
    try:
        for name in VIEWS:
            response = client.get_table(**_request(plan, name))
            record = {"version": 1, "request": _request(plan, name), "table": response.get("Table")}
            content = _encode(record)
            normalized = decode(content, MAX_CAPTURE_BYTES)
            write_once(output / (name + ".json"), content)
            written += 1
            table = _table(normalized, plan, name)
            records[name] = {
                "file": name + ".json",
                "sha256": hashlib.sha256(content).hexdigest(),
                "bytes": len(content),
                **_review(table, plan, name),
            }
        content = canonical(_manifest(pin, hashlib.sha256(canonical(intent)).hexdigest(), records))
        if len(content) > 64 * 1024:
            raise ValueError("Capture manifest exceeds its ceiling")
        write_once(output / MANIFEST, content)
        return hashlib.sha256(content).hexdigest()
    except BaseException as error:
        try:
            write_once(
                output / "failure.json",
                canonical(
                    {
                        "status": "view_metadata_capture_failed",
                        "view_plan_sha256": pin,
                        "view": name,
                        "records_written": written,
                        "error_type": type(error).__name__,
                        "view_catalog_verified": False,
                        "result_verified": False,
                    }
                ),
            )
        except Exception:
            pass
        raise


def load_capture(
    output: Path,
    plan: dict[str, Any],
    pin: str,
    caller: dict[str, Any],
) -> dict[str, Any]:
    if output.is_symlink() or not output.is_dir():
        raise ValueError("Capture directory must be regular")
    intent, intent_pin = read_record(output / "intent.json")
    if not same(intent, _intent(plan, pin, caller)):
        raise ValueError("Captured intent differs")
    manifest, _ = read_record(output / MANIFEST)
    records = {}
    for name in VIEWS:
        path = output / (name + ".json")
        if path.is_symlink() or not path.is_file():
            raise ValueError("Captured table record must be regular")
        with path.open("rb") as stream:
            content = stream.read(MAX_CAPTURE_BYTES + 1)
        record = decode(content, MAX_CAPTURE_BYTES)
        table = _table(record, plan, name)
        records[name] = {
            "file": name + ".json",
            "sha256": hashlib.sha256(content).hexdigest(),
            "bytes": len(content),
            **_review(table, plan, name),
        }
    if not same(manifest, _manifest(pin, intent_pin, records)):
        raise ValueError("Capture manifest or compatibility result differs")
    return {"metadata_captured": True, "view_metadata_compatibility": manifest["compatibility"]}
