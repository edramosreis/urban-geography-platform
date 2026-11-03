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
    "mode", ["success", "failed_run", "controller_error", "existing_output", "missing_stdin"]
)
def test_cli_requires_explicit_credentials_and_delegates_one_attempt(
    frozen, tmp_path, monkeypatch, capsys, mode
):
    monkeypatch.setattr(control, "ROOT", tmp_path)
    output = tmp_path / "artifacts/trial"
    if mode == "existing_output":
        output.mkdir(parents=True)
    main = runpy.run_path(str(ROOT / "scripts/run_glue_pilot.py"))["main"]
    result = {"outcome": "job_succeeded" if mode == "success" else "job_failed"}
    run = Mock(return_value=result)
    if mode == "controller_error":
        run.side_effect = RuntimeError("controller failure")
    monkeypatch.setitem(main.__globals__, "run_with_credentials", run)
    stream = Mock()
    stream.read.return_value = b"explicit dummy handoff"
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=stream))
    command = [
        "run_glue_pilot.py",
        "--release",
        str(frozen.path),
        "--release-sha256",
        frozen.pin,
        "--execute",
        "--output",
        str(output),
    ]
    if mode != "missing_stdin":
        command.append("--credentials-stdin")
    monkeypatch.setattr(sys, "argv", command)
    if mode == "success":
        main()
        assert json.loads(capsys.readouterr().out)["outcome"] == "job_succeeded"
    elif mode in {"failed_run", "missing_stdin"}:
        with pytest.raises(SystemExit) as error:
            main()
        assert error.value.code == 2
    else:
        with pytest.raises((RuntimeError, ValueError)):
            main()
    if mode in {"existing_output", "missing_stdin"}:
        stream.read.assert_not_called()
        run.assert_not_called()
    else:
        stream.read.assert_called_once_with(main.__globals__["MAX_BYTES"] + 1)
        assert run.call_count == 1 and run.call_args.args[-1] == b"explicit dummy handoff"
