"""Durable request/token integrity with real local files; no Spark, credentials or AWS."""

import json
from unittest.mock import Mock

import pytest
from athena_control_fixtures import CONTROL as control
from athena_control_fixtures import make_attempt
from athena_plan_fixtures import ATHENA, OPTIONS


@pytest.fixture
def attempt(tmp_path, monkeypatch):
    return make_attempt(tmp_path, monkeypatch)


def test_intent_is_durable_before_use_and_token_is_unique_per_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "ARTIFACTS", tmp_path)
    fsync = Mock(wraps=control.os.fsync)
    monkeypatch.setattr(control.os, "fsync", fsync)
    first = control.Attempt(tmp_path / "first", ATHENA.read_plan(**OPTIONS))
    second = control.Attempt(tmp_path / "second", ATHENA.read_plan(**OPTIONS))
    assert fsync.call_count == 4  # Intent and initial event for each attempt.
    saved = json.loads((first.output / "intent.json").read_bytes())
    assert saved == first.intent()
    token = saved["request"].pop("ClientRequestToken")
    assert len(token) == 64 and int(token, 16) >= 0
    assert saved["request"] == saved["plan"]["request_template"]
    assert token != second.intent()["request"]["ClientRequestToken"]
    assert saved["plan"]["execution_authorized"] is False
    assert saved["plan"]["provenance_verified"] is False


def test_existing_attempt_is_never_replaced(attempt):
    before = {p.name: p.read_bytes() for p in attempt.output.iterdir()}
    with pytest.raises(ValueError, match="fresh"):
        control.Attempt(attempt.output, attempt.intent()["plan"])
    assert before == {p.name: p.read_bytes() for p in attempt.output.iterdir()}


@pytest.mark.parametrize("change", ["query", "profile", "binding", "authority"])
def test_changed_preparation_is_rejected_before_creating_output(tmp_path, monkeypatch, change):
    monkeypatch.setattr(control, "ARTIFACTS", tmp_path)
    plan = ATHENA.read_plan(**OPTIONS)
    if change == "query":
        plan["request_template"]["QueryString"] = "DROP TABLE anything"
    elif change == "profile":
        plan["profile"]["urban_geo_athena"]["target"] = "different"
    elif change == "binding":
        plan["binding"]["source_schema"] = "different"
    else:
        plan["execution_authorized"] = True
    with pytest.raises(ValueError, match="recipe"):
        control.Attempt(tmp_path / "attempt", plan)
    assert not list(tmp_path.iterdir())


def test_outside_or_artifacts_root_output_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "ARTIFACTS", tmp_path / "artifacts")
    for output in (tmp_path / "outside", tmp_path / "artifacts"):
        with pytest.raises(ValueError, match="artifacts"):
            control.Attempt(output, ATHENA.read_plan(**OPTIONS))
    assert not list(tmp_path.iterdir())


def test_changed_intent_is_detected_and_returned_copy_cannot_change_it(attempt):
    first = attempt.intent()
    first["request"]["ClientRequestToken"] = "changed-in-memory"
    assert attempt.intent()["request"]["ClientRequestToken"] != "changed-in-memory"
    path = attempt.output / "intent.json"
    path.write_bytes(path.read_bytes().replace(b"SELECT", b"DELETE", 1))
    with pytest.raises(ValueError, match="intent changed"):
        attempt.intent()


def test_failed_flush_cannot_produce_a_usable_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(control.os, "fsync", Mock(side_effect=OSError("disk failed")))
    with pytest.raises(OSError, match="disk failed"):
        control.Attempt(tmp_path / "attempt", ATHENA.read_plan(**OPTIONS))
    with pytest.raises(ValueError, match="fresh"):
        control.Attempt(tmp_path / "attempt", ATHENA.read_plan(**OPTIONS))


def test_receipt_is_flushed_and_never_overwritten(attempt):
    attempt.receipt({"result_verified": False})
    with pytest.raises(FileExistsError):
        attempt.receipt({"result_verified": True})
    assert json.loads((attempt.output / "receipt.json").read_bytes()) == {"result_verified": False}
