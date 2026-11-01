"""Durable Athena attempt records; no execution CLI or credential/client construction."""

import hashlib
import json
import os
import secrets
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from athena_read_plan import ARTIFACTS, read_plan, same


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def verified_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Check plan integrity against the shared recipe; this does not prove provenance."""
    try:
        expected = read_plan(
            **plan["target"],
            snapshot_id=plan["binding"]["snapshot_id"],
            source_run_sha256=plan["binding"]["source_run_sha256"],
            max_query_scan_bytes=plan["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"],
        )
    except (KeyError, TypeError) as exc:
        raise ValueError("Malformed Athena preparation") from exc
    if not same(plan, expected):
        raise ValueError("Athena preparation differs from the fixed query recipe")
    return expected


def write_once(path: Path, content: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


class Attempt:
    """Persist one new token and exact request before any API call; never overwrite."""

    def __init__(self, output: Path, plan: dict[str, Any]) -> None:
        checked = verified_plan(plan)
        resolved, artifacts = output.resolve(), ARTIFACTS.resolve()
        if (
            output.exists()
            or output.is_symlink()
            or resolved == artifacts
            or not resolved.is_relative_to(artifacts)
        ):
            raise ValueError("Use a fresh Athena attempt directory under ignored artifacts")
        request = checked["request_template"] | {"ClientRequestToken": secrets.token_hex(32)}
        self.output = resolved
        self._content = canonical({"contract_version": 1, "plan": checked, "request": request})
        self.intent_sha256 = hashlib.sha256(self._content).hexdigest()
        resolved.mkdir(parents=True)
        write_once(resolved / "intent.json", self._content)
        self.record({"event": "attempt_prepared", "intent_sha256": self.intent_sha256})

    def intent(self) -> dict[str, Any]:
        path = self.output / "intent.json"
        # A changed record must never lead to a changed query or a replacement token.
        if path.is_symlink() or path.stat().st_size != len(self._content):
            raise ValueError("Athena attempt intent changed")
        if path.read_bytes() != self._content:
            raise ValueError("Athena attempt intent changed")
        return json.loads(self._content)

    def record(self, event: dict[str, Any]) -> None:
        row = event | {"recorded_at_utc": datetime.now(UTC).isoformat()}
        with (self.output / "events.jsonl").open("ab") as stream:
            stream.write(canonical(row))
            stream.flush()
            os.fsync(stream.fileno())

    def receipt(self, value: dict[str, Any]) -> None:
        write_once(self.output / "receipt.json", canonical(value))
