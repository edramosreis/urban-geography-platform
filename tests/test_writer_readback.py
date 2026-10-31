"""Preflight, metadata bounds and reader cleanup without Spark startup."""

import copy
import json
from unittest.mock import Mock

import pytest
from writer_readback_fixtures import READBACK as reader
from writer_readback_fixtures import make_case

from urban_geography.ingest import digest


@pytest.fixture
def case(history_run_factory, tmp_path):
    return make_case(history_run_factory, tmp_path)


def options(case, factory):
    return dict(
        catalog_name="local",
        namespace="readback_test",
        expected_settings=case.settings,
        expected_location=case.location,
        session_factory=factory,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "failed"),
        ("table", "local.other.building_proximity"),
        ("format_version", True),
        ("rows", True),
        ("rows", 3),
        ("source_run_sha256", "b" * 64),
        ("readback_matches", False),
        ("readback_matches", 1),
        ("snapshot_id", True),
        ("snapshot_id", 0),
        ("snapshot_id", 2**63),
        ("input_files", 99),
        ("input_manifest_sha256", "c" * 64),
        ("verified_spark_settings", {}),
        ("extra", "unexpected"),
    ],
)
def test_wrong_writer_claim_fails_before_reader_start(case, field, value):
    case.receipt.write_text(json.dumps(case.claim | {field: value}), encoding="utf-8")
    factory = Mock()
    with pytest.raises(ValueError):
        reader.verify_writer_output(
            case.path,
            case.run.summary_sha256,
            case.receipt,
            digest(case.receipt),
            **options(case, factory),
        )
    factory.assert_not_called()


@pytest.mark.parametrize("tamper", ["pin", "oversized", "target", "warehouse"])
def test_bad_receipt_or_target_fails_before_start(case, tamper):
    factory = Mock()
    kwargs = options(case, factory)
    if tamper == "pin":
        case.pin = "d" * 64
    elif tamper == "oversized":
        case.receipt.write_bytes(b" " * (reader.MAX_WRITER_RECEIPT_BYTES + 1))
        case.pin = digest(case.receipt)
    elif tamper == "target":
        kwargs["namespace"] = "bad;sql"
    else:
        kwargs["expected_location"] = "file:///outside/building_proximity"
    with pytest.raises(ValueError):
        reader.verify_writer_output(
            case.path, case.run.summary_sha256, case.receipt, case.pin, **kwargs
        )
    factory.assert_not_called()


@pytest.mark.parametrize(
    "tamper",
    [
        "snapshot",
        "location",
        "source_hash",
        "source_summary",
        "extra_snapshot",
        "missing_snapshot",
        "snapshot_hash",
        "schema",
        "row_count",
        "file_count",
        "byte_count",
        "deletes",
        "invalid_stat",
        "operation",
        "uuid",
    ],
)
def test_wrong_or_oversized_metadata_is_rejected_before_data_reads(case, tamper):
    metadata = copy.deepcopy(case.metadata)
    stats = metadata["snapshots"][0]["summary"]
    if tamper == "snapshot":
        metadata["current-snapshot-id"] = 124
    elif tamper == "location":
        metadata["location"] += "_other"
    elif tamper == "source_hash":
        metadata["properties"]["source-run-sha256"] = "b" * 64
    elif tamper == "source_summary":
        metadata["properties"]["source-run-summary"] = "{}"
    elif tamper == "extra_snapshot":
        metadata["snapshots"].append(copy.deepcopy(metadata["snapshots"][0]))
    elif tamper == "missing_snapshot":
        metadata["snapshots"] = []
    elif tamper == "snapshot_hash":
        stats["source-run-sha256"] = "b" * 64
    elif tamper == "schema":
        metadata["schemas"][0]["fields"][0]["type"] = "long"
    elif tamper == "row_count":
        stats["total-records"] = "3"
    elif tamper == "file_count":
        stats["total-data-files"] = str(reader.MAX_PARQUET_FILES + 1)
    elif tamper == "byte_count":
        stats["total-files-size"] = str(reader.MAX_PAYLOAD_BYTES + 1)
    elif tamper == "deletes":
        stats["total-delete-files"] = "1"
    elif tamper == "invalid_stat":
        stats["total-records"] = True
    elif tamper == "operation":
        stats["operation"] = "delete"
    else:
        metadata["table-uuid"] = "invalid"
    with pytest.raises(ValueError):
        reader.check_metadata(metadata, case.plan, case.claim, case.location)


@pytest.mark.parametrize(
    "failure",
    [
        "runtime",
        "metadata",
        "rows",
        "drift",
        "receipt_change",
        "input_change",
        "manifest_change",
        "exception",
        "interrupt",
    ],
)
def test_reader_always_stops_and_rejects_changed_evidence(case, monkeypatch, failure):
    spark = Mock(version="3.5.6")
    spark.conf.get.side_effect = lambda key, default="": case.settings.get(key, default)
    metadata = copy.deepcopy(case.metadata)
    after = copy.deepcopy(metadata)
    monkeypatch.setattr(reader, "_table_metadata", Mock(side_effect=[metadata, after]))
    monkeypatch.setattr(reader, "_validate_facts", Mock(return_value=2))
    source = Mock()
    stored = spark.read.option.return_value.table.return_value
    source.select.return_value.exceptAll.return_value.limit.return_value.count.return_value = 0
    stored.select.return_value.exceptAll.return_value.limit.return_value.count.return_value = 0
    monkeypatch.setattr(reader, "_driver_facts", Mock(return_value=source))
    if failure == "runtime":
        spark.version = "wrong"
    elif failure == "metadata":
        metadata["current-snapshot-id"] = 999
    elif failure == "rows":
        source.select.return_value.exceptAll.return_value.limit.return_value.count.return_value = 1
    elif failure == "drift":
        after["last-updated-ms"] = 1
    elif failure == "exception":
        reader._validate_facts.side_effect = RuntimeError("read error")
    elif failure == "interrupt":
        reader._validate_facts.side_effect = KeyboardInterrupt()
    elif failure == "receipt_change":
        reader._validate_facts.side_effect = lambda *args: case.receipt.write_text("changed") or 2
    elif failure == "input_change":

        def changed(*args):
            summary = case.path / "summary.json"
            summary.write_bytes(summary.read_bytes() + b" ")
            return 2

        reader._validate_facts.side_effect = changed
    elif failure == "manifest_change":

        def changed_manifest(*args):
            manifest = case.path / "input-manifest.json"
            manifest.write_bytes(manifest.read_bytes() + b" ")
            return 2

        reader._validate_facts.side_effect = changed_manifest
    with pytest.raises((ValueError, RuntimeError, KeyboardInterrupt)):
        reader.verify_writer_output(
            case.path,
            case.run.summary_sha256,
            case.receipt,
            case.pin,
            **options(case, lambda: spark),
        )
    spark.stop.assert_called_once_with()
