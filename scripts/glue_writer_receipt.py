"""Capture one writer claim from bounded, run-scoped CloudWatch stdout pages.

The caller supplies and owns the Logs client. This module creates no client,
submits no job/query, and does not independently read the Iceberg table.
"""

import hashlib
import json
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from aws_login_bridge import BridgeError, decode
from glue_control import load_plan, same, trial_output
from writer_readback import MAX_WRITER_RECEIPT_BYTES, validate_claim

from urban_geography.glue_input import InputPlan, load_prepared_input
from urban_geography.glue_release import load_release
from urban_geography.glue_runtime import runtime_plan
from urban_geography.ingest import digest

MAX_PAGES = 8
PAGE_EVENTS = 100
MAX_LOG_BYTES = 1024 * 1024
MAX_EVENT_BYTES = 128 * 1024
CAPTURE_SECONDS = 60


class CaptureError(ValueError):
    """Constant reason codes only; never include raw log messages in failures."""


class LogsClient(Protocol):
    def filter_log_events(self, **kwargs: Any) -> dict[str, Any]: ...


def receipt_plan(release: Path, pin: str, controller: Path, controller_pin: str) -> dict[str, Any]:
    plan = load_plan(release, pin)
    if not re.fullmatch(r"[0-9a-f]{64}", controller_pin) or controller.is_symlink():
        raise CaptureError("invalid_controller_receipt_pin_or_path")
    with controller.open("rb") as stream:
        content = stream.read(MAX_WRITER_RECEIPT_BYTES + 1)
    if hashlib.sha256(content).hexdigest() != controller_pin:
        raise CaptureError("controller_receipt_checksum_differs")
    result = decode(content, MAX_WRITER_RECEIPT_BYTES)
    expected = {
        "outcome": "job_succeeded",
        "terminal_confirmed": True,
        "job_name": plan["job"]["Name"],
        "release_id": plan["release_id"],
        "release_sha256": pin,
    }
    run_id = result.get("run_id")
    if (
        any(not same(result.get(key), value) for key, value in expected.items())
        or not isinstance(run_id, str)
        or not re.fullmatch(r"jr_[A-Za-z0-9_]{1,252}", run_id)
    ):
        raise CaptureError("controller_does_not_bind_a_successful_run")
    deployment = plan["job"]["Name"].removesuffix("-pilot-writer")
    return {
        "release_sha256": pin,
        "release_id": plan["release_id"],
        "controller_receipt_sha256": controller_pin,
        "job_name": result["job_name"],
        "run_id": run_id,
        "log_group": (
            f"arn:aws:logs:{plan['region']}:{plan['owner']}:log-group:"
            f"/urban-geography/{deployment}/glue/output"
        ),
        "target": {
            "deployment_name": deployment,
            "account_id": plan["owner"],
            "region": plan["region"],
        },
        "bucket": plan["bucket"],
    }


def publication(value: dict[str, Any], prepared: InputPlan, plan: dict[str, Any]) -> dict[str, Any]:
    expected = {
        "status": "complete",
        "sdk_client_closed_before_writer": True,
        "scratch_cleaned": True,
    }
    if set(value) != set(expected) | {"transfer", "publication"} or not same(
        {key: value.get(key) for key in expected}, expected
    ):
        raise CaptureError("writer_envelope_differs")
    transfer = value["transfer"]
    versions = transfer.get("object_versions") if isinstance(transfer, dict) else None
    if (
        not isinstance(versions, dict)
        or set(versions) != set(prepared.files) | {"input-manifest.json"}
        or any(not isinstance(v, str) or not 0 < len(v) <= 1024 for v in versions.values())
    ):
        raise CaptureError("writer_transfer_versions_differ")
    manifest = prepared.run.path / "input-manifest.json"
    expected_transfer = {
        "status": "complete",
        "source_run_sha256": prepared.run.summary_sha256,
        "input_manifest_sha256": digest(manifest),
        "bucket": plan["bucket"],
        "expected_bucket_owner": plan["target"]["account_id"],
        "prefix": "inputs/" + prepared.run.summary_sha256 + "/",
        "object_versions": versions,
        "get_requests": len(versions),
        "downloaded_body_bytes": prepared.payload_bytes + manifest.stat().st_size,
        "payload_bytes": prepared.payload_bytes,
        "mapped_buildings": prepared.run.summary["mapped_buildings"],
    }
    if not same(transfer, expected_transfer):
        raise CaptureError("writer_transfer_differs")
    recipe = runtime_plan(**plan["target"])
    table = recipe["catalog"] + "." + recipe["database"] + ".building_proximity"
    try:
        return validate_claim(value["publication"], prepared, table, recipe["spark_settings"])
    except (ValueError, TypeError, KeyError):
        raise CaptureError("writer_publication_differs") from None


def capture_writer_receipt(
    client: LogsClient,
    release: Path,
    pin: str,
    controller: Path,
    controller_pin: str,
    output: Path,
    *,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    plan = receipt_plan(release, pin, controller, controller_pin)
    output = trial_output(output, release)
    if output.is_relative_to(controller.parent.resolve()) or controller.resolve().is_relative_to(
        output
    ):
        raise CaptureError("capture_output_overlaps_controller_evidence")
    manifest = load_release(release, pin)
    prepared = load_prepared_input(release / "input", manifest["identity"]["summary_sha256"])
    output.mkdir(parents=True)
    started = now()
    counters = {"pages": 0, "events": 0, "message_bytes": 0}
    seen: dict[str, str] = {}
    tokens: set[str] = set()
    token = None
    selected: list[tuple[dict[str, Any], dict[str, Any]]] = []
    try:
        for _ in range(MAX_PAGES):
            if now() - started >= CAPTURE_SECONDS:
                raise CaptureError("log_capture_time_limit")
            request: dict[str, Any] = {
                "logGroupIdentifier": plan["log_group"],
                "logStreamNamePrefix": plan["run_id"],
                "limit": PAGE_EVENTS,
                "unmask": False,
            }
            if token is not None:
                request["nextToken"] = token
            response = client.filter_log_events(**request)
            counters["pages"] += 1
            if now() - started >= CAPTURE_SECONDS:
                raise CaptureError("log_capture_time_limit")
            events = response.get("events")
            if not isinstance(events, list) or len(events) > PAGE_EVENTS:
                raise CaptureError("invalid_log_page")
            for event in events:
                counters["events"] += 1
                if not isinstance(event, dict):
                    raise CaptureError("invalid_log_event")
                message, stream, event_id = (
                    event.get(key) for key in ("message", "logStreamName", "eventId")
                )
                if (
                    not isinstance(message, str)
                    or not isinstance(stream, str)
                    or not re.fullmatch(
                        re.escape(plan["run_id"]) + r"(?:-[A-Za-z0-9_-]{1,128})?", stream
                    )
                    or not isinstance(event_id, str)
                    or not 0 < len(event_id) <= 1024
                    or any(
                        type(event.get(key)) is not int or event[key] < 0
                        for key in ("timestamp", "ingestionTime")
                    )
                ):
                    raise CaptureError("log_event_identity_differs")
                content = message.encode("utf-8")
                counters["message_bytes"] += len(content)
                if len(content) > MAX_EVENT_BYTES or counters["message_bytes"] > MAX_LOG_BYTES:
                    raise CaptureError("log_capture_byte_limit")
                identity = hashlib.sha256(
                    json.dumps(event, sort_keys=True, allow_nan=False).encode()
                ).hexdigest()
                if event_id in seen:
                    if seen[event_id] != identity:
                        raise CaptureError("log_event_id_conflict")
                    continue
                seen[event_id] = identity
                for line in message.splitlines():
                    if '"publication"' not in line:
                        continue
                    try:
                        value = decode(line.strip().encode(), MAX_EVENT_BYTES)
                    except BridgeError:
                        raise CaptureError("malformed_writer_envelope") from None
                    claim = publication(value, prepared, plan)
                    selected.append(
                        (
                            claim,
                            {
                                "stream": stream,
                                "event_id": event_id,
                                "timestamp": event["timestamp"],
                                "ingestion_time": event["ingestionTime"],
                                "message_sha256": hashlib.sha256(content).hexdigest(),
                            },
                        )
                    )
                    if len(selected) > 1:
                        raise CaptureError("ambiguous_writer_receipts")
            token = response.get("nextToken")
            if token is None:
                break
            if not isinstance(token, str) or not 0 < len(token) <= 8192 or token in tokens:
                raise CaptureError("invalid_or_repeated_log_token")
            tokens.add(token)
        else:
            raise CaptureError("log_capture_page_limit")
        if not selected:
            raise CaptureError("writer_receipt_missing")
        claim, event_proof = selected[0]
        content = (json.dumps(claim, sort_keys=True, allow_nan=False) + "\n").encode()
        if len(content) > MAX_WRITER_RECEIPT_BYTES:
            raise CaptureError("writer_receipt_byte_limit")
        # Recheck the retained evidence before publishing the extracted claim.
        load_release(release, pin)
        if digest(controller) != controller_pin:
            raise CaptureError("controller_receipt_changed_during_capture")
        if now() - started >= CAPTURE_SECONDS:
            raise CaptureError("log_capture_time_limit")
        with (output / "writer.json").open("xb") as destination:
            destination.write(content)
        result = {
            "status": "writer_claim_captured",
            "binding": plan,
            "log_event": event_proof,
            "writer_receipt_sha256": hashlib.sha256(content).hexdigest(),
            "snapshot_id": claim["snapshot_id"],
            "source_run_sha256": claim["source_run_sha256"],
            "statistics": counters,
            "row_readback_performed": False,
        }
    except Exception as error:
        reason = str(error) if isinstance(error, CaptureError) else "api_or_local_failure"
        with (output / "capture.json").open("x", encoding="utf-8") as destination:
            json.dump(
                {"status": "capture_failed", "reason": reason, "statistics": counters}, destination
            )
        raise CaptureError(reason) from None
    with (output / "capture.json").open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, allow_nan=False)
    return result
