"""Opt-in capture uses current scoped clients and cannot downgrade completion proofs."""

import json
import sys
from datetime import UTC, datetime, timedelta

import pytest
from dbt_catalog_fixtures import fake_view
from dbt_host_fixtures import CALLER, RECEIPT, prepared_case
from test_dbt_builder_operation import assert_closed, run
from test_dbt_builder_operation import case as builder_case

case = builder_case


def capture_tables(case, monkeypatch, unsupported=False):
    monkeypatch.setattr(sys.modules["dbt_view_capture"], "ARTIFACTS", case.output.parent)
    original = case.base.clients["glue"].get_table.return_value
    seen = []

    def observed(**kwargs):
        name = kwargs["Name"]
        if name == "building_proximity":
            return original
        seen.append(name)
        table, _ = fake_view(case.plan, name)
        if unsupported and name == "grid_proximity":
            table["ViewOriginalText"] = "unrecognised encoding"
        return {"Table": table}

    case.base.clients["glue"].get_table.side_effect = observed
    return seen


@pytest.mark.parametrize("unsupported", [False, True])
def test_two_view_reads_preserve_metadata_without_full_acceptance(case, monkeypatch, unsupported):
    seen = capture_tables(case, monkeypatch, unsupported)
    report = run(case, capture_view_metadata=True)
    assert seen == ["grid_proximity", "release_proximity"]
    assert not report["view_catalog_verified"] and not report["result_verified"]
    assert (
        json.loads((case.output / "builder-operation.json").read_bytes())["capture_view_metadata"]
        is True
    )
    loaded = sys.modules["dbt_view_capture"].load_capture(
        case.output / "view-metadata", case.plan, case.pin, CALLER
    )
    assert loaded["view_metadata_compatibility"] == (
        "requires_review" if unsupported else "matches_current_checker"
    )
    assert_closed(case)


def test_failed_second_read_preserves_inner_receipt_and_partial_capture(case, monkeypatch):
    capture_tables(case, monkeypatch)
    original = case.base.clients["glue"].get_table.side_effect

    def observed(**kwargs):
        if kwargs["Name"] == "release_proximity":
            raise RuntimeError("synthetic metadata failure")
        return original(**kwargs)

    case.base.clients["glue"].get_table.side_effect = observed
    with pytest.raises(RuntimeError, match="metadata failure"):
        run(case, capture_view_metadata=True)
    assert (case.output / "receipt.json").exists()
    assert (case.output / "view-metadata/grid_proximity.json").exists()
    assert not (case.output / "builder-operation.json").exists()
    assert len(case.base.requests) == 2
    assert_closed(case)


def test_source_drift_after_capture_blocks_outer_completion(case, monkeypatch):
    seen = capture_tables(case, monkeypatch)
    original = case.base.clients["glue"].get_table.side_effect

    def observed(**kwargs):
        value = original(**kwargs)
        if kwargs["Name"] == "building_proximity" and len(seen) == 2:
            value = {"Table": value["Table"] | {"VersionId": "2"}}
        return value

    case.base.clients["glue"].get_table.side_effect = observed
    with pytest.raises(ValueError, match="Source catalog changed"):
        run(case, capture_view_metadata=True)
    assert (case.output / "view-metadata/capture.json").exists()
    assert not (case.output / "view-metadata/source-gate.json").exists()
    assert not (case.output / "builder-operation.json").exists()
    assert_closed(case)


def test_capture_requires_the_remaining_final_reserve(case, monkeypatch):
    seen = capture_tables(case, monkeypatch)
    original = case.base.executor.side_effect

    def execute(*args, **kwargs):
        result = original(*args, **kwargs)
        case.snapshot.expiration = datetime.now(UTC) + timedelta(seconds=179)
        return result

    case.base.executor.side_effect = execute
    with pytest.raises(ValueError, match="too_short_for_operation"):
        run(case, capture_view_metadata=True)
    assert seen == [] and not (case.output / "view-metadata").exists()
    assert (case.output / "receipt.json").exists()
    assert_closed(case)


def test_nonboolean_capture_option_never_acquires_credentials(case):
    with pytest.raises(ValueError, match="explicit boolean"):
        run(case, capture_view_metadata="yes")
    case.acquisition.assert_not_called()


def test_capture_proof_requires_its_selected_inventory(tmp_path, monkeypatch):
    c = prepared_case(tmp_path, monkeypatch, capture=True)
    args = (c.evidence, c.recipe, c.pin, c.input, c.writer_pin, c.reply)
    result = RECEIPT.load_completion(*args, capture_view_metadata=True)
    assert result["metadata_captured"] and result["result_verified"] is False
    with pytest.raises(ValueError):
        RECEIPT.load_completion(*args)
    (c.evidence / "view-metadata/grid_proximity.json").write_text("{}")
    with pytest.raises(ValueError):
        RECEIPT.load_completion(*args, capture_view_metadata=True)


def test_selected_capture_rejects_a_standard_completion_proof(tmp_path, monkeypatch):
    c = prepared_case(tmp_path, monkeypatch)
    with pytest.raises((ValueError, FileNotFoundError)):
        RECEIPT.load_completion(
            c.evidence, c.recipe, c.pin, c.input, c.writer_pin, c.reply, capture_view_metadata=True
        )
