"""A hung dbt worker and its descendants are stopped without forwarding credentials."""

import json
from unittest.mock import Mock

import pytest
from dbt_executor_fixtures import EXECUTOR, WORKER, broker_case


def test_worker_watchdog_stops_its_owned_process_group(tmp_path, monkeypatch):
    fixture, clients, _ = broker_case(tmp_path, monkeypatch)
    monkeypatch.setattr(EXECUTOR, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(EXECUTOR, "tooling_ready", Mock())
    helper = tmp_path / "sleep.py"
    helper.write_text(
        "import subprocess,sys,time\n"
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n"
        "print(p.pid,flush=True)\ntime.sleep(30)\n"
    )
    monkeypatch.setattr(EXECUTOR, "WORKER_SCRIPT", helper)
    output = tmp_path / "dbt"
    with fixture.control.session():
        with pytest.raises(TimeoutError, match="deadline"):
            EXECUTOR.ModelExecutor(clients, max_seconds=1)(fixture.control, output)
    process = json.loads((output / "process.json").read_text())
    assert process["owned_group_stopped"] and not process["credentials_sent"]
    assert process["failure_type"] == "TimeoutError" and process["returncode"] != 0
    fixture.client.start_query_execution.assert_not_called()


def test_worker_request_is_independently_pinned_and_bound_to_the_project(tmp_path, monkeypatch):
    fixture, _, _ = broker_case(tmp_path, monkeypatch)
    monkeypatch.setattr(WORKER, "ARTIFACTS", tmp_path)
    output = tmp_path / "dbt"
    output.mkdir()
    request = {
        "version": 1,
        "project": str(fixture.project),
        "output": str(output),
        "view_plan": fixture.control.plan(),
        "controller_intent_sha256": fixture.control.intent_sha256,
    }
    path = output / "request.json"
    content = WORKER.canonical(request)
    path.write_bytes(content)
    pin = WORKER.hashlib.sha256(content).hexdigest()
    assert WORKER.load_request(path, pin) == request
    with pytest.raises(ValueError, match="checksum"):
        WORKER.load_request(path, "0" * 64)
    (fixture.project / "macros/require_view.sql").write_text("changed")
    with pytest.raises(ValueError):
        WORKER.load_request(path, pin)
