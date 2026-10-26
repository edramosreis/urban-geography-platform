"""Processing-time precision and source-schema identity for evolving Iceberg history."""

from datetime import UTC, datetime
from typing import Any

PROCESSING_TIME = "processed_at_utc"


def processing_time(summary: dict[str, Any]) -> datetime:
    """Normalize completion time to a UTC instant with millisecond precision."""
    value = summary.get("completed_at_utc")
    if not isinstance(value, str):
        raise ValueError("A timezone-aware completed_at_utc is required")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("A timezone-aware completed_at_utc is required") from error
    if parsed.utcoffset() is None:
        raise ValueError("A timezone-aware completed_at_utc is required")
    parsed = parsed.astimezone(UTC)
    return parsed.replace(microsecond=(parsed.microsecond // 1000) * 1000)


def source_snapshot(
    metadata: dict[str, Any], base: int, source_hash: str, summary_json: str
) -> dict[str, Any]:
    """Find this run's latest write in the pinned snapshot's ancestry."""
    snapshots = {snapshot["snapshot-id"]: snapshot for snapshot in metadata["snapshots"]}
    visited = set()
    cursor: int | None = base
    while cursor is not None and cursor not in visited:
        visited.add(cursor)
        snapshot = snapshots.get(cursor)
        if snapshot is None:
            break
        summary = snapshot["summary"]
        if summary.get("source-run-sha256") == source_hash:
            if summary.get("source-run-summary") != summary_json:
                raise ValueError("Stored release source provenance differs")
            return snapshot
        cursor = snapshot.get("parent-snapshot-id")
    raise ValueError("Stored release has no retained source provenance in the current ancestry")


def source_has_processing_time(metadata: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    for schema in metadata["schemas"]:
        if schema["schema-id"] == snapshot.get("schema-id"):
            return any(field["name"] == PROCESSING_TIME for field in schema["fields"])
    raise ValueError("Source snapshot schema is unavailable")
