"""Offline contracts for UTC processing time and snapshot-schema provenance."""

from datetime import UTC, datetime

import pytest

from urban_geography.history_schema import (
    processing_time,
    source_has_processing_time,
    source_snapshot,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-10-24T21:01:38.049848+00:00", datetime(2026, 10, 24, 21, 1, 38, 49000, UTC)),
        ("2026-10-24T22:01:38.999999+01:00", datetime(2026, 10, 24, 21, 1, 38, 999000, UTC)),
        ("2026-10-24T21:01:38Z", datetime(2026, 10, 24, 21, 1, 38, tzinfo=UTC)),
        ("2026-10-24T21:01:38.000001+00:00", datetime(2026, 10, 24, 21, 1, 38, tzinfo=UTC)),
    ],
)
def test_processing_time_normalizes_timezone_and_truncates_to_milliseconds(value, expected):
    assert processing_time({"completed_at_utc": value}) == expected


@pytest.mark.parametrize("value", [None, 1, "not-a-date", "2026-10-24T21:01:38"])
def test_processing_time_rejects_missing_malformed_or_naive_values(value):
    with pytest.raises(ValueError, match="timezone-aware completed_at_utc"):
        processing_time({"completed_at_utc": value})


def lineage():
    return {
        "snapshots": [
            {
                "snapshot-id": 1,
                "schema-id": 0,
                "summary": {"source-run-sha256": "A", "source-run-summary": "summary-A"},
            },
            {
                "snapshot-id": 2,
                "parent-snapshot-id": 1,
                "schema-id": 1,
                "summary": {"source-run-sha256": "B", "source-run-summary": "summary-B"},
            },
            {
                "snapshot-id": 3,
                "parent-snapshot-id": 2,
                "schema-id": 1,
                "summary": {"source-run-sha256": "A", "source-run-summary": "summary-A"},
            },
        ],
        "schemas": [
            {"schema-id": 0, "fields": [{"name": "building_id"}]},
            {"schema-id": 1, "fields": [{"name": "building_id"}, {"name": "processed_at_utc"}]},
        ],
    }


def test_latest_source_write_uses_its_own_schema_and_ignores_other_branches():
    metadata = lineage()
    old = source_snapshot(metadata, 2, "A", "summary-A")
    assert old["snapshot-id"] == 1
    assert not source_has_processing_time(metadata, old)
    latest = source_snapshot(metadata, 3, "A", "summary-A")
    assert latest["snapshot-id"] == 3
    assert source_has_processing_time(metadata, latest)


def test_a_newer_wrong_source_summary_cannot_be_hidden_by_an_older_match():
    metadata = lineage()
    metadata["snapshots"][2]["summary"]["source-run-summary"] = "changed-provenance"
    with pytest.raises(ValueError, match="source provenance differs"):
        source_snapshot(metadata, 3, "A", "summary-A")


def test_unretained_source_or_missing_schema_is_rejected():
    metadata = lineage()
    with pytest.raises(ValueError, match="no retained source provenance"):
        source_snapshot(metadata, 1, "B", "summary-B")
    metadata["schemas"] = []
    with pytest.raises(ValueError, match="Source snapshot schema is unavailable"):
        source_has_processing_time(metadata, metadata["snapshots"][0])
