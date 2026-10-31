"""Private attempt journals and owned-client cleanup without AWS execution."""

import json
import runpy
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from glue_control_fixtures import CONTROL as control
from glue_control_fixtures import ROOT


def test_journal_refuses_reuse_and_preserves_existing_attempt(tmp_path, frozen, monkeypatch):
    monkeypatch.setattr(control, "ROOT", tmp_path)
    output = tmp_path / "artifacts/trial"
    journal = control.Journal(output, frozen.path)
    journal.record({"event": "start_intent", "request": {"JobName": "example"}})
    journal.receipt({"outcome": "test"})
    content = (output / "events.jsonl").read_bytes()
    events = [json.loads(line) for line in content.splitlines()]
    assert [event["event"] for event in events] == ["attempt_created", "start_intent"]
    assert all(event["recorded_at_utc"].endswith("+00:00") for event in events)
    with pytest.raises(ValueError, match="fresh"):
        control.Journal(output, frozen.path)
    with pytest.raises(FileExistsError):
        journal.receipt({"outcome": "cannot overwrite"})
    assert (output / "events.jsonl").read_bytes() == content


@pytest.mark.parametrize("kind", ["outside_artifacts", "inside_release"])
def test_journal_refuses_unsafe_or_frozen_locations(tmp_path, monkeypatch, kind):
    monkeypatch.setattr(control, "ROOT", tmp_path)
    release = tmp_path / "artifacts/release"
    release.mkdir(parents=True)
    output = tmp_path / "data/trial" if kind == "outside_artifacts" else release / "trial"
    with pytest.raises(ValueError):
        control.Journal(output, release)
    assert not output.exists()


@pytest.mark.parametrize(
    "mode", ["success", "failed_run", "controller_error", "s3_factory_error", "existing_output"]
)
def test_cli_closes_only_its_owned_clients_on_every_path(
    frozen, tmp_path, monkeypatch, capsys, mode
):
    monkeypatch.setattr(control, "ROOT", tmp_path)
    output = tmp_path / "artifacts/trial"
    if mode == "existing_output":
        output.mkdir(parents=True)
    glue = SimpleNamespace(close=Mock())
    s3 = SimpleNamespace(close=Mock())
    glue_factory = Mock(return_value=glue)
    s3_factory = Mock(return_value=s3)
    if mode == "s3_factory_error":
        s3_factory.side_effect = RuntimeError("SDK construction failed")
    result = {"outcome": "job_succeeded" if mode == "success" else "job_failed"}
    execute = Mock(return_value=result)
    if mode == "controller_error":
        execute.side_effect = RuntimeError("controller failure")
    main = runpy.run_path(str(ROOT / "scripts/run_glue_pilot.py"))["main"]
    monkeypatch.setitem(main.__globals__, "create_glue_client", glue_factory)
    monkeypatch.setitem(main.__globals__, "create_input_client", s3_factory)
    monkeypatch.setitem(main.__globals__, "execute_trial", execute)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_glue_pilot.py",
            "--release",
            str(frozen.path),
            "--release-sha256",
            frozen.pin,
            "--execute",
            "--output",
            str(output),
        ],
    )
    if mode == "success":
        main()
        assert json.loads(capsys.readouterr().out)["outcome"] == "job_succeeded"
    elif mode == "failed_run":
        with pytest.raises(SystemExit) as captured:
            main()
        assert captured.value.code == 2
    else:
        with pytest.raises((RuntimeError, ValueError)):
            main()
    if mode == "existing_output":
        glue_factory.assert_not_called()
        s3_factory.assert_not_called()
    else:
        glue.close.assert_called_once()
        if mode != "s3_factory_error":
            s3.close.assert_called_once()
    assert (output / "receipt.json").exists() == (mode in {"success", "failed_run"})
