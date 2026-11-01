"""Worker protocol, credential isolation and owned-process cleanup without Spark startup."""

import json
import subprocess
import sys

import pytest
from athena_runner_fixtures import READER as reader
from athena_runner_fixtures import WORKER as worker
from athena_runner_fixtures import reader_case


@pytest.fixture
def case(tmp_path, monkeypatch):
    return reader_case(tmp_path, monkeypatch)


def run(case):
    adapter, args, options, process, popen, cleanup, captured = case
    return adapter(*args, **options)


def test_worker_uses_fixed_interpreter_clean_environment_and_private_records(case, monkeypatch):
    monkeypatch.setenv("JAVA_TOOL_OPTIONS", "unreviewed")
    monkeypatch.setenv("AWS_ENDPOINT_URL_GLUE", "https://example.invalid")
    result = run(case)
    adapter, args, options, process, popen, cleanup, captured = case
    assert result == {"proof": "reader"}
    assert captured["command"][0] == "/opt/venv/bin/python"
    assert captured["options"]["start_new_session"] is True
    assert captured["environment"]["AWS_SECRET_ACCESS_KEY"] == "offline-secret"
    assert (
        "JAVA_TOOL_OPTIONS" not in captured["environment"]
        and "AWS_ENDPOINT_URL_GLUE" not in captured["environment"]
    )
    assert (
        captured["options"]["env"] == {}
    )  # The temporary mapping is cleared after launch/cleanup.
    records = b"".join(p.read_bytes() for p in adapter.output.glob("*.json"))
    assert b"offline-secret" not in records and b"offline-token" not in records
    cleanup.assert_called_once_with(process)
    process.wait.assert_called_once_with(timeout=reader.READER_SECONDS)


@pytest.mark.parametrize("problem", ["timeout", "interrupt", "exit_code", "cleanup"])
def test_reader_failures_never_return_a_verified_source(case, problem):
    adapter, args, options, process, popen, cleanup, captured = case
    if problem == "timeout":
        process.wait.side_effect = subprocess.TimeoutExpired("reader", 180)
        expected = subprocess.TimeoutExpired
    elif problem == "interrupt":
        process.wait.side_effect = KeyboardInterrupt()
        expected = KeyboardInterrupt
    elif problem == "exit_code":
        process.wait.return_value = 3
        expected = RuntimeError
    else:
        cleanup.return_value = False
        expected = RuntimeError
    with pytest.raises(expected):
        run(case)
    cleanup.assert_called_once_with(process)
    assert json.loads((adapter.output / "process.json").read_text())["owned_group_stopped"] is (
        problem != "cleanup"
    )
    assert captured["options"]["env"] == {}


@pytest.mark.parametrize("mode", ["existing", "outside", "inside_input"])
def test_unsafe_output_stops_before_creating_a_process(case, tmp_path, mode):
    adapter, args, options, process, popen, cleanup, captured = case
    if mode == "existing":
        adapter.output.mkdir()
    elif mode == "outside":
        adapter.output = tmp_path.parent / "outside-reader"
    else:
        adapter.output = args[0] / "reader"
    with pytest.raises(ValueError):
        run(case)
    popen.assert_not_called()


@pytest.mark.parametrize("change", ["missing_credentials", "extra_environment", "wrong_catalog"])
def test_bad_cloud_reader_contract_fails_before_launch(case, change):
    adapter, args, options, process, popen, cleanup, captured = case
    if change == "missing_credentials":
        adapter._credentials.clear()
    elif change == "extra_environment":
        adapter._credentials["JAVA_TOOL_OPTIONS"] = "unreviewed"
    else:
        options["catalog_name"] = "local"
    with pytest.raises(ValueError):
        run(case)
    popen.assert_not_called()


def test_request_hash_is_verified_before_reader_import_or_session_start(tmp_path):
    path = tmp_path / "request.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="checksum"):
        worker.run(path, "a" * 64)


def test_cleanup_stops_only_the_owned_process_group():
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True
    )
    try:
        assert reader.cleanup_group(process)
        assert process.poll() is not None
        assert not reader.group_alive(process.pid)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
