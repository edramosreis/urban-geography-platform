"""Real orchestration with fake services/reader, plus plan-only CLI and failure ordering."""

import copy
import json
import sys
from unittest.mock import Mock

import pytest
from athena_control_fixtures import CONTROL
from athena_runner_fixtures import CLI, trial_case
from athena_runner_fixtures import RUNNER as runner


@pytest.fixture
def case(tmp_path, monkeypatch):
    return trial_case(tmp_path, monkeypatch)


def run(case):
    return runner.run_trial(case.clients, case.attempt, **case.options)


def test_full_order_requires_source_execution_result_and_final_stability(case):
    order = Mock()
    for name, client in case.clients.items():
        order.attach_mock(client, name)
    order.attach_mock(case.reader, "reader")
    report = run(case)
    assert report["status"] == "verified_trial" and report["result_verified"]
    assert report["caller_identity_verified"] and report["source_readback_verified"]
    assert report["writer_job_identity_verified"] is False and case.body.closed
    calls = [c[0] for c in order.mock_calls]
    assert calls == [
        "sts.get_caller_identity",
        "glue.get_table",
        "reader",
        "glue.get_table",
        "sts.get_caller_identity",
        "athena.get_work_group",
        "athena.start_query_execution",
        "athena.get_query_execution",
        "athena.get_query_execution",
        "s3.head_object",
        "s3.get_object",
        "athena.get_query_execution",
        "glue.get_table",
        "sts.get_caller_identity",
    ]
    for name in (
        "source-verification.json",
        "execution.json",
        "result-verification.json",
        "receipt.json",
    ):
        assert (case.attempt.output / name).is_file()
    assert report["row"] == case.expected


@pytest.mark.parametrize("failure", ["receipt", "caller", "reader", "catalog"])
def test_pre_submission_failures_never_start_a_query(case, failure):
    if failure == "receipt":
        case.options["writer_receipt_sha256"] = "0" * 64
    elif failure == "caller":
        case.clients["sts"].get_caller_identity.return_value["Account"] = "999999999999"
    elif failure == "reader":
        case.reader.side_effect = RuntimeError("reader failed")
    else:
        case.clients["glue"].get_table.return_value["Table"]["Name"] = "other_table"
    with pytest.raises((ValueError, RuntimeError)):
        run(case)
    case.clients["athena"].start_query_execution.assert_not_called()
    assert (
        json.loads((case.attempt.output / "failure.json").read_text())["result_verified"] is False
    )


def test_disabled_workgroup_after_readback_still_prevents_submission(case):
    case.clients["athena"].get_work_group.return_value["WorkGroup"]["State"] = "DISABLED"
    with pytest.raises(ValueError, match="disabled"):
        run(case)
    case.clients["athena"].start_query_execution.assert_not_called()
    assert not json.loads((case.attempt.output / "failure.json").read_text())["recovery_required"]


def test_failed_query_is_recorded_without_reading_results(case):
    case.clients["athena"].get_query_execution.return_value["QueryExecution"]["Status"]["State"] = (
        "FAILED"
    )
    report = run(case)
    assert report["status"] == "query_not_verified" and not report["result_verified"]
    case.clients["s3"].head_object.assert_not_called()
    assert (case.attempt.output / "execution.json").is_file()


def test_unknown_submission_keeps_the_original_token_and_never_retries(case):
    token = case.attempt.intent()["request"]["ClientRequestToken"]
    case.clients["athena"].start_query_execution.side_effect = TimeoutError()
    with pytest.raises(CONTROL.SubmissionUnknown):
        run(case)
    case.clients["athena"].start_query_execution.assert_called_once()
    assert case.attempt.intent()["request"]["ClientRequestToken"] == token
    assert json.loads((case.attempt.output / "failure.json").read_text())["recovery_required"]
    case.clients["s3"].head_object.assert_not_called()


def test_changed_final_catalog_keeps_raw_query_and_result_evidence_but_no_success_receipt(case):
    before = case.clients["glue"].get_table.return_value
    after = copy.deepcopy(before)
    after["Table"]["VersionId"] = "2"
    case.clients["glue"].get_table.side_effect = [before, before, after]
    with pytest.raises(ValueError, match="catalog"):
        run(case)
    assert (case.attempt.output / "result-verification.json").exists()
    assert not (case.attempt.output / "receipt.json").exists()
    case.clients["athena"].stop_query_execution.assert_not_called()


def test_disk_failure_before_submission_stops_without_starting(case, monkeypatch):
    original = runner.write_once

    def write(path, content):
        if path.name == "source-verification.json":
            raise OSError("disk failed")
        original(path, content)

    monkeypatch.setattr(runner, "write_once", write)
    with pytest.raises(OSError):
        run(case)
    case.clients["athena"].start_query_execution.assert_not_called()


def test_plan_cli_does_not_discover_credentials_or_start_reader(
    case, tmp_path, monkeypatch, capsys
):
    path = tmp_path / "plan.json"
    path.write_bytes(CONTROL.canonical(case.plan))
    pin = CONTROL.hashlib.sha256(path.read_bytes()).hexdigest()
    resources = Mock(side_effect=AssertionError("No SDK in plan mode"))
    monkeypatch.setattr(CLI, "query_resources", resources)
    monkeypatch.setattr(
        sys, "argv", ["run_athena_pilot.py", "--plan", str(path), "--plan-sha256", pin]
    )
    CLI.main()
    assert json.loads(capsys.readouterr().out)["mode"] == "plan_only"
    resources.assert_not_called()


def test_execute_cli_requires_all_inputs_before_credentials(case, tmp_path, monkeypatch):
    path = tmp_path / "plan.json"
    path.write_bytes(CONTROL.canonical(case.plan))
    pin = CONTROL.hashlib.sha256(path.read_bytes()).hexdigest()
    resources = Mock(side_effect=AssertionError("No SDK without explicit inputs"))
    monkeypatch.setattr(CLI, "query_resources", resources)
    monkeypatch.setattr(
        sys, "argv", ["run_athena_pilot.py", "--plan", str(path), "--plan-sha256", pin, "--execute"]
    )
    with pytest.raises(SystemExit) as caught:
        CLI.main()
    assert caught.value.code == 2
    resources.assert_not_called()
