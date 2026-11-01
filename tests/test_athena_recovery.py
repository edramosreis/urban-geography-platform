"""Exact saved-token replay, local ownership, preflight gates and terminal-only recovery."""

import json
import sys
from unittest.mock import Mock

import pytest
from athena_control_fixtures import CONTROL, execution
from athena_recovery_fixtures import CLI, unknown_case
from athena_recovery_fixtures import RECOVERY as recovery


@pytest.fixture
def case(tmp_path, monkeypatch):
    return unknown_case(tmp_path, monkeypatch)


def run(case):
    return recovery.recover_submission(
        case.clients, case.original, case.pin, case.output, **case.options
    )


def proofs(case):
    return {
        name: (case.original / name).read_bytes()
        for name in ("intent.json", "submission.json", "source-verification.json", "failure.json")
    }


def test_replay_keeps_exact_request_and_token_and_preserves_original_proofs(case, monkeypatch):
    before = proofs(case)
    original = case.trial.attempt.intent()["request"]
    monkeypatch.setattr(
        CONTROL.secrets, "token_hex", Mock(side_effect=AssertionError("No new token"))
    )
    report = run(case)
    assert report["status"] == "reconciled_terminal" and report["terminal_confirmed"]
    assert not report["result_verified"]
    assert report["last_observation"]["state"] == "CANCELLED"
    case.clients["athena"].start_query_execution.assert_called_once_with(**original)
    case.clients["athena"].stop_query_execution.assert_called_once_with(
        QueryExecutionId="query-001"
    )
    assert proofs(case) == before
    assert (case.output / "intent.json").read_bytes() == before["intent.json"]
    assert (case.original / recovery.CLAIM).exists()
    assert json.loads((case.output / "receipt.json").read_text())["result_verified"] is False


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "CANCELLED"])
def test_already_terminal_query_does_not_trigger_cancellation_or_gold_success(case, state):
    case.clients["athena"].get_query_execution.side_effect = None
    case.clients["athena"].get_query_execution.return_value = execution(case.trial.attempt, state)
    report = run(case)
    assert report["terminal_confirmed"] and not report["result_verified"]
    case.clients["athena"].stop_query_execution.assert_not_called()


@pytest.mark.parametrize(
    "change",
    ["intent_pin", "known_query", "known_execution", "source_proof", "reservation", "token"],
)
def test_invalid_or_known_original_is_rejected_before_any_sdk_call(case, change):
    if change == "intent_pin":
        case.pin = "0" * 64
    elif change == "known_query":
        (case.original / "query.json").write_text('{"query_id":"query-001"}')
    elif change == "known_execution":
        (case.original / "execution.json").write_text("{}")
    elif change == "source_proof":
        path = case.original / "source-verification.json"
        source = json.loads(path.read_text())
        source["source_readback_verified"] = False
        path.write_text(json.dumps(source))
    elif change == "reservation":
        (case.original / "submission.json").write_text('{"intent_sha256":"wrong"}')
    else:
        path = case.original / "intent.json"
        value = json.loads(path.read_text())
        del value["request"]["ClientRequestToken"]
        data = CONTROL.canonical(value)
        path.write_bytes(data)
        case.pin = CONTROL.hashlib.sha256(data).hexdigest()
        (case.original / "submission.json").write_bytes(
            CONTROL.canonical({"intent_sha256": case.pin})
        )
    with pytest.raises(ValueError):
        run(case)
    case.clients["sts"].get_caller_identity.assert_not_called()
    case.clients["athena"].start_query_execution.assert_not_called()


@pytest.mark.parametrize("change", ["caller", "catalog", "disabled_workgroup"])
def test_current_gates_fail_before_consuming_the_one_replay_reservation(case, change):
    if change == "caller":
        case.clients["sts"].get_caller_identity.return_value["Account"] = "999999999999"
    elif change == "catalog":
        case.clients["glue"].get_table.return_value["Table"]["VersionId"] = "2"
    else:
        case.clients["athena"].get_work_group.return_value["WorkGroup"]["State"] = "DISABLED"
    with pytest.raises(ValueError):
        run(case)
    assert not (case.original / recovery.CLAIM).exists()
    case.clients["athena"].start_query_execution.assert_not_called()


def test_active_original_controller_blocks_recovery_before_api_calls(case):
    with CONTROL.controller_lease(case.original):
        with pytest.raises(RuntimeError, match="active controller"):
            run(case)
    case.clients["sts"].get_caller_identity.assert_not_called()
    case.clients["athena"].start_query_execution.assert_not_called()


def test_second_replay_is_refused_even_if_its_previous_response_was_uncertain(case):
    case.clients["athena"].start_query_execution.side_effect = TimeoutError()
    with pytest.raises(CONTROL.SubmissionUnknown, match="Recovery replay"):
        run(case)
    case.output = case.output.with_name("another-recovery")
    with pytest.raises(ValueError, match="already reserved"):
        run(case)
    case.clients["athena"].start_query_execution.assert_called_once()
    assert not case.output.exists()


def test_unconfirmed_stop_stays_unconfirmed_with_bounded_observations(case):
    case.clients["athena"].get_query_execution.side_effect = None
    case.clients["athena"].get_query_execution.return_value = execution(
        case.trial.attempt, "RUNNING"
    )
    report = run(case)
    assert report["status"] == "stop_unconfirmed" and not report["terminal_confirmed"]
    assert case.clock.value == CONTROL.STOP_SECONDS
    assert (
        case.clients["athena"].get_query_execution.call_count == 1 + CONTROL.MAX_STOP_OBSERVATIONS
    )
    case.clients["athena"].stop_query_execution.assert_called_once()


def test_original_changed_during_preflight_is_rejected_before_replay(case):
    group = case.clients["athena"].get_work_group.return_value

    def changed(**kwargs):
        path = case.original / "source-verification.json"
        path.write_bytes(path.read_bytes() + b" ")
        return group

    case.clients["athena"].get_work_group.side_effect = changed
    with pytest.raises(ValueError, match="changed"):
        run(case)
    case.clients["athena"].start_query_execution.assert_not_called()


def test_source_and_original_output_directories_are_not_overwritten(case):
    case.output = case.original / "recovery"
    with pytest.raises(ValueError, match="separate"):
        run(case)
    case.clients["sts"].get_caller_identity.assert_not_called()


def test_plan_inspection_does_not_write_files_or_discover_credentials(case, monkeypatch, capsys):
    before = {p.name: p.read_bytes() for p in case.original.iterdir() if p.is_file()}
    resources = Mock(side_effect=AssertionError("No credential discovery in plan mode"))
    monkeypatch.setattr(CLI, "query_resources", resources)
    monkeypatch.setattr(
        sys,
        "argv",
        ["recover_athena_pilot.py", "--attempt", str(case.original), "--intent-sha256", case.pin],
    )
    CLI.main()
    plan = json.loads(capsys.readouterr().out)
    assert (
        plan["mode"] == "recovery_plan_only"
        and plan["replay_can_submit_if_original_was_not_received"]
    )
    assert plan["execution_authorized"] is False
    assert before == {p.name: p.read_bytes() for p in case.original.iterdir() if p.is_file()}
    resources.assert_not_called()
