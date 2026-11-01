"""Explicit one-replay reconciliation and cancellation of an uncertain original submission."""

import hashlib
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from athena_control import (
    Attempt,
    SubmissionUnknown,
    canonical,
    controller_lease,
    monitor_query,
    verified_plan,
    write_once,
)
from athena_preflight import caller_identity, catalog_state, expected_caller
from athena_read_plan import ARTIFACTS, preflight_workgroup, same

MAX_RECORD_BYTES = 64 * 1024
CLAIM = ".recovery-replay.json"
KNOWN = ("query.json", "execution.json", "receipt.json")


def present(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def record_bytes(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Recovery requires a regular saved record: " + path.name)
    with path.open("rb") as stream:
        content = stream.read(MAX_RECORD_BYTES + 1)
    if len(content) > MAX_RECORD_BYTES:
        raise ValueError("Saved recovery record exceeds its byte ceiling")
    return content


@dataclass(frozen=True)
class SavedSubmission:
    original: Path
    intent_sha256: str
    records: dict[str, bytes] = field(repr=False)

    def intent(self) -> dict[str, Any]:
        return json.loads(self.records["intent.json"])

    def source(self) -> dict[str, Any]:
        return json.loads(self.records["source-verification.json"])

    def unchanged(self) -> None:
        if any(present(self.original / name) for name in KNOWN):
            raise ValueError(
                "The query ID or outcome is already known; do not replay this submission"
            )
        if any(
            record_bytes(self.original / name) != content for name, content in self.records.items()
        ):
            raise ValueError("Original submission records changed")

    def plan(self) -> dict[str, Any]:
        intent = self.intent()
        return {
            "mode": "recovery_plan_only",
            "original_intent_sha256": self.intent_sha256,
            "target": intent["plan"]["target"],
            "binding": intent["plan"]["binding"],
            "query_sha256": intent["plan"]["query_sha256"],
            "replay_already_reserved": present(self.original / CLAIM),
            "requires_exclusive_controller_lease": True,
            "replay_can_submit_if_original_was_not_received": True,
            "action": "Replay once; confirm terminal state or cancel",
            "execution_authorized": False,
            "result_verified": False,
        }


def inspect_submission(original: Path, pin: str) -> SavedSubmission:
    if original.is_symlink() or not original.is_dir():
        raise ValueError("Use the original regular attempt directory")
    original = original.resolve()
    if original == ARTIFACTS.resolve() or not original.is_relative_to(ARTIFACTS.resolve()):
        raise ValueError("Original attempt must be under ignored artifacts")
    if not isinstance(pin, str) or re.fullmatch(r"[0-9a-f]{64}", pin) is None:
        raise ValueError("Use an independent SHA256 pin of the original intent")
    records = {
        name: record_bytes(original / name)
        for name in ("intent.json", "submission.json", "source-verification.json")
    }
    if hashlib.sha256(records["intent.json"]).hexdigest() != pin:
        raise ValueError("Original intent checksum differs")
    intent = json.loads(records["intent.json"])
    if (
        not isinstance(intent, dict)
        or set(intent) != {"contract_version", "plan", "request"}
        or type(intent["contract_version"]) is not int
        or intent["contract_version"] != 1
    ):
        raise ValueError("Unsupported original intent")
    plan = verified_plan(intent["plan"])
    if not isinstance(intent["request"], dict):
        raise ValueError("Original query request must be an object")
    token = intent["request"].get("ClientRequestToken")
    if (
        not isinstance(token, str)
        or re.fullmatch(r"[0-9a-f]{64}", token) is None
        or not same(intent["request"], plan["request_template"] | {"ClientRequestToken": token})
    ):
        raise ValueError("Original request/token differs from the fixed recipe")
    if not same(json.loads(records["submission.json"]), {"intent_sha256": pin}):
        raise ValueError("Submission reservation does not bind the original intent")
    source = json.loads(records["source-verification.json"])
    required = {
        "contract_version": 1,
        "intent_sha256": pin,
        "status": "query_source_checked",
        "binding": plan["binding"],
        "query_sha256": plan["query_sha256"],
        "caller_identity_verified": True,
        "source_readback_verified": True,
        "catalog_stable": True,
    }
    if (
        not isinstance(source, dict)
        or not same({key: source.get(key) for key in required}, required)
        or not isinstance(source.get("catalog"), dict)
    ):
        raise ValueError("Original source verification is missing or belongs to another intent")
    saved = SavedSubmission(original, pin, records)
    saved.unchanged()
    return saved


def recovery_output(output: Path, saved: SavedSubmission) -> Path:
    resolved = output.resolve()
    if (
        present(output)
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
        or resolved.is_relative_to(saved.original)
        or saved.original.is_relative_to(resolved)
    ):
        raise ValueError("Use fresh recovery output separate from the original attempt")
    return resolved


class RecoveryJournal(Attempt):
    """Reuse exact intent bytes without generating a new token."""

    def __init__(self, output: Path, saved: SavedSubmission) -> None:
        resolved = recovery_output(output, saved)
        self.output = resolved
        self._content = saved.records["intent.json"]
        self.intent_sha256 = saved.intent_sha256
        resolved.mkdir(parents=True)
        write_once(resolved / "intent.json", self._content)
        write_once(resolved / "original-submission.json", saved.records["submission.json"])
        write_once(
            resolved / "original-source-verification.json",
            saved.records["source-verification.json"],
        )
        self.record({"event": "recovery_prepared", "original_intent_sha256": saved.intent_sha256})


def recover_submission(
    clients: dict[str, Any],
    original: Path,
    pin: str,
    output: Path,
    *,
    caller_arn: str,
    caller_user_id: str,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """One separately authorized replay; stop the returned query and retain every original proof."""
    saved = inspect_submission(original, pin)
    with controller_lease(saved.original):
        saved.unchanged()
        if present(saved.original / CLAIM):
            raise ValueError(
                "A recovery replay was already reserved; inspect its evidence manually"
            )
        journal = RecoveryJournal(output, saved)
        intent = journal.intent()
        plan, request = intent["plan"], intent["request"]
        expected = expected_caller(plan["target"]["account_id"], caller_arn, caller_user_id)
        stage = "recovery_preflight"
        try:
            caller_identity(clients["sts"], expected)
            if not same(catalog_state(clients["glue"], plan), saved.source()["catalog"]):
                raise ValueError("Source catalog differs from the original verified source")
            preflight_workgroup(clients["athena"], plan)
            saved.unchanged()
            journal.intent()
            write_once(
                saved.original / CLAIM,
                canonical({"intent_sha256": pin, "recovery_output": str(journal.output)}),
            )
            journal.record({"event": "recovery_replay_intent", "intent_sha256": pin})
            stage = "recovery_replay"
            started = now()
            try:
                response = clients["athena"].start_query_execution(**request)
                query_id = response.get("QueryExecutionId")
                if (
                    not isinstance(query_id, str)
                    or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query_id) is None
                ):
                    raise ValueError("Replay returned no usable query ID")
            except BaseException as exc:
                raise SubmissionUnknown(
                    "Recovery replay outcome is unknown; manual reconciliation required"
                ) from exc
            stage = "recovery_cleanup"
            execution = monitor_query(
                clients["athena"],
                journal,
                query_id,
                intent=intent,
                started=started,
                now=now,
                pause=pause,
                stop_reason="recovery_cancel",
            )
            write_once(journal.output / "recovery-execution.json", canonical(execution))
            saved.unchanged()
            report = {
                "status": "reconciled_terminal"
                if execution["terminal_confirmed"]
                else "stop_unconfirmed",
                "query_id": query_id,
                "original_intent_sha256": pin,
                "terminal_confirmed": execution["terminal_confirmed"],
                "last_observation": execution["last_observation"],
                "stop_attempted": execution["stop_attempted"],
                "stop_acknowledged": execution["stop_acknowledged"],
                "replay_may_have_submitted_the_original_request": True,
                "caller_identity_verified": True,
                "result_verified": False,
                "scope": "Original-token reconciliation and terminal cleanup only",
            }
            journal.receipt(report)
            return report
        except BaseException as exc:
            try:
                write_once(
                    journal.output / "failure.json",
                    canonical(
                        {"stage": stage, "error_type": type(exc).__name__, "result_verified": False}
                    ),
                )
            except Exception:
                pass
            raise
