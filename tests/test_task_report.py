"""Fresh immutable-log reports and CLI execution with Java unavailable."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import urban_geography.task_diagnostics as diagnostics


@pytest.fixture
def event_dir(tmp_path):
    directory = tmp_path / "events"
    directory.mkdir()
    events = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.6"},
        {"Event": "SparkListenerApplicationStart", "App ID": "app-cli"},
        {
            "Event": "SparkListenerJobStart",
            "Stage IDs": [0],
            "Properties": {"spark.jobGroup.id": "trial"},
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 0,
            "Stage Attempt ID": 0,
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {
                "Index": 0,
                "Executor ID": "driver",
                "Launch Time": 10,
                "Finish Time": 20,
            },
        },
    ]
    (directory / "app-cli").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    (directory / ".crc").write_bytes(b"not a JSON event file")
    return directory


def test_cli_runs_without_java_and_fingerprints_unchanged_logs(event_dir, tmp_path):
    source = event_dir / "app-cli"
    before = source.read_bytes()
    output = tmp_path / "fresh/report.json"
    script = Path(__file__).resolve().parents[1] / "scripts/report_task_stages.py"
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--event-dir",
            str(event_dir),
            "--group",
            "trial",
            "--group",
            "missing",
            "--output",
            str(output),
        ],
        env={**os.environ, "JAVA_HOME": "/unavailable-java", "PATH": ""},
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    assert json.loads(result.stdout)["groups_without_task_events"] == ["missing"]
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["input_hashes"] == {str(source): hashlib.sha256(before).hexdigest()}
    assert source.read_bytes() == before
    assert report["groups"]["trial"]["stages"][0]["distributions"]["input_records"] is None


@pytest.mark.parametrize("destination", ["events/report.json", "data/report.json"])
def test_reports_cannot_write_into_inputs_or_live_data(
    event_dir, tmp_path, monkeypatch, destination
):
    monkeypatch.chdir(tmp_path)
    output = tmp_path / destination
    with pytest.raises(ValueError, match="outside event logs"):
        diagnostics.write_stage_report(event_dir, ["trial"], output)
    assert not output.exists()


def test_existing_report_is_not_overwritten(event_dir, tmp_path):
    output = tmp_path / "report.json"
    output.write_text("previous report", encoding="utf-8")
    with pytest.raises(ValueError, match="output exists"):
        diagnostics.write_stage_report(event_dir, ["trial"], output)
    assert output.read_text(encoding="utf-8") == "previous report"


def test_bad_log_does_not_create_an_output(event_dir, tmp_path):
    (event_dir / "app-cli").write_text("{broken", encoding="utf-8")
    output = tmp_path / "new-parent/report.json"
    with pytest.raises(ValueError, match="Invalid Spark event JSON"):
        diagnostics.write_stage_report(event_dir, ["trial"], output)
    assert not output.parent.exists()


@pytest.mark.parametrize("change_inventory", [False, True])
def test_logs_changing_during_inspection_cannot_publish(
    event_dir, tmp_path, monkeypatch, change_inventory
):
    original = diagnostics.summarize_stage_tasks

    def changing(paths, groups):
        report = original(paths, groups)
        if change_inventory:
            (event_dir / "new-log").write_text("{}", encoding="utf-8")
        else:
            with (event_dir / "app-cli").open("a", encoding="utf-8") as stream:
                stream.write("\n")
        return report

    monkeypatch.setattr(diagnostics, "summarize_stage_tasks", changing)
    output = tmp_path / "report.json"
    with pytest.raises(ValueError, match="changed while"):
        diagnostics.write_stage_report(event_dir, ["trial"], output)
    assert not output.exists()
