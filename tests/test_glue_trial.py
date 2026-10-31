"""One-submission run control with fake time and service clients; no AWS traffic."""

from unittest.mock import Mock

import pytest
from glue_control_fixtures import CONTROL as control


class Clock:
    def __init__(self):
        self.value = 0.0

    def now(self):
        return self.value

    def pause(self, seconds):
        self.value += seconds


@pytest.fixture
def trial(control_case, monkeypatch):
    monkeypatch.setattr(control, "MAX_OBSERVATIONS", 2)
    monkeypatch.setattr(control, "MAX_STOP_OBSERVATIONS", 2)
    monkeypatch.setattr(control, "WATCH_SECONDS", 20)
    monkeypatch.setattr(control, "STOP_SECONDS", 20)
    case = control_case
    case.run_id = "jr-owned-test-run"
    case.events = []
    case.clock = Clock()
    case.glue.start_job_run = Mock(return_value={"JobRunId": case.run_id})
    case.glue.batch_stop_job_run = Mock(
        return_value={
            "SuccessfulSubmissions": [
                {"JobName": case.plan["job"]["Name"], "JobRunId": case.run_id}
            ],
            "Errors": [],
        }
    )
    case.glue.get_job_run = Mock(return_value=observation(case, "SUCCEEDED"))
    return case


def observation(case, state, **changes):
    run = {
        key: case.plan["job"][key]
        for key in ("GlueVersion", "WorkerType", "NumberOfWorkers", "Timeout", "ExecutionClass")
    }
    return {
        "JobRun": run
        | {"Id": case.run_id, "JobName": case.plan["job"]["Name"], "JobRunState": state}
        | changes
    }


def execute(case, **changes):
    return control.execute_trial(
        case.glue,
        case.s3,
        case.plan,
        case.events.append,
        now=case.clock.now,
        pause=case.clock.pause,
        **changes,
    )


@pytest.mark.parametrize("state", sorted(control.TERMINAL))
def test_terminal_states_do_not_trigger_retries_or_unnecessary_stops(trial, state):
    trial.glue.get_job_run.return_value = observation(trial, state)
    result = execute(trial)
    assert result["terminal_confirmed"] and result["last_observation"]["state"] == state
    assert result["outcome"] == ("job_succeeded" if state == "SUCCEEDED" else "job_failed")
    assert result["output_readback_verified"] is False
    trial.glue.start_job_run.assert_called_once_with(**trial.plan["start_request"])
    trial.glue.batch_stop_job_run.assert_not_called()


def test_intent_and_run_identity_are_recorded_before_the_next_api_call(trial):
    def submit(**kwargs):
        assert trial.events[-1]["event"] == "start_intent"
        return {"JobRunId": trial.run_id}

    def observe(**kwargs):
        assert trial.events[-1]["event"] == "run_identified"
        return observation(trial, "SUCCEEDED", ExecutionTime=12, DPUSeconds=24.5)

    trial.glue.start_job_run.side_effect = submit
    trial.glue.get_job_run.side_effect = observe
    result = execute(trial)
    assert result["last_observation"]["metrics"] == {"ExecutionTime": 12, "DPUSeconds": 24.5}
    assert [row["event"] for row in trial.events][:4] == [
        "preflight_started",
        "preflight_complete",
        "start_intent",
        "run_identified",
    ]


@pytest.mark.parametrize("error", [RuntimeError("transport failure"), KeyboardInterrupt()])
def test_uncertain_submission_never_resubmits_or_stops_an_unknown_id(trial, error):
    trial.glue.start_job_run.side_effect = error
    with pytest.raises(control.SubmissionUnknown, match="reconcile"):
        execute(trial)
    assert trial.events[-1]["event"] == "submission_unknown"
    trial.glue.start_job_run.assert_called_once()
    trial.glue.get_job_run.assert_not_called()
    trial.glue.batch_stop_job_run.assert_not_called()


def test_missing_submission_id_is_unknown_rather_than_retried(trial):
    trial.glue.start_job_run.return_value = {}
    with pytest.raises(control.SubmissionUnknown):
        execute(trial)
    assert trial.glue.start_job_run.call_count == 1
    trial.glue.batch_stop_job_run.assert_not_called()


def test_preflight_mismatch_never_starts_a_run(trial):
    trial.glue.get_job.return_value["Job"]["WorkerType"] = "G.8X"
    with pytest.raises(ValueError):
        execute(trial)
    trial.glue.start_job_run.assert_not_called()


def test_unreviewed_start_overrides_are_rejected_before_preflight(trial):
    trial.plan["start_request"]["Arguments"] = {"--extra-py-files": "unreviewed"}
    with pytest.raises(ValueError, match="overrides"):
        execute(trial)
    trial.glue.get_job.assert_not_called()
    trial.glue.start_job_run.assert_not_called()


def test_watchdog_stops_only_its_run_and_waits_for_terminal_confirmation(trial):
    trial.glue.get_job_run.side_effect = [
        observation(trial, state) for state in ("RUNNING", "RUNNING", "STOPPING", "STOPPED")
    ]
    result = execute(trial)
    assert result["outcome"] == "watchdog_limit" and result["terminal_confirmed"]
    assert result["stop_accepted"] and trial.glue.get_job_run.call_count == 4
    trial.glue.batch_stop_job_run.assert_called_once_with(
        JobName=trial.plan["job"]["Name"], JobRunIds=[trial.run_id]
    )


def test_observation_counts_bound_a_stalled_clock_too(trial):
    trial.clock.pause = lambda seconds: None
    trial.glue.get_job_run.return_value = observation(trial, "RUNNING")
    result = execute(trial)
    assert result["outcome"] == "stop_unconfirmed" and not result["terminal_confirmed"]
    assert trial.glue.get_job_run.call_count == 4 and trial.glue.start_job_run.call_count == 1
    trial.glue.batch_stop_job_run.assert_called_once()


@pytest.mark.parametrize(
    "problem", ["drift", "missing", "identity", "unknown_state", "observation_error"]
)
def test_unverified_observations_request_cleanup_without_accepting_success(trial, problem):
    first = observation(trial, "RUNNING")
    if problem == "drift":
        first["JobRun"]["NumberOfWorkers"] = 20
    elif problem == "missing":
        first["JobRun"].pop("Timeout")
    elif problem == "identity":
        first["JobRun"]["Id"] = "jr-someone-else"
    elif problem == "unknown_state":
        first["JobRun"]["JobRunState"] = "FUTURE_STATE"
    else:
        first = RuntimeError("observation failed")
    trial.glue.get_job_run.side_effect = [first, observation(trial, "STOPPED")]
    result = execute(trial)
    assert result["outcome"] in {"runtime_unverified", "observation_failed"}
    assert result["terminal_confirmed"] and result["run_id"] == trial.run_id
    trial.glue.batch_stop_job_run.assert_called_once_with(
        JobName=trial.plan["job"]["Name"], JobRunIds=[trial.run_id]
    )


@pytest.mark.parametrize("state", ["WAITING", "STOPPING"])
def test_unexpected_wait_or_stop_gets_bounded_confirmation(trial, state):
    trial.glue.get_job_run.side_effect = [observation(trial, state), observation(trial, "STOPPED")]
    result = execute(trial)
    assert result["outcome"] == "unexpected_wait_or_stop" and result["terminal_confirmed"]
    trial.glue.batch_stop_job_run.assert_called_once()


def test_success_without_runtime_metadata_is_not_accepted_as_verified(trial):
    trial.glue.get_job_run.return_value["JobRun"].pop("WorkerType")
    result = execute(trial)
    assert result["outcome"] == "runtime_unverified" and result["terminal_confirmed"]
    trial.glue.batch_stop_job_run.assert_not_called()


@pytest.mark.parametrize("stop_error", ["response_error", "exception", "wrong_submission"])
def test_stop_submission_is_distinct_from_confirmed_termination(trial, stop_error):
    trial.glue.get_job_run.side_effect = [
        RuntimeError("lost observation"),
        observation(trial, "STOPPED"),
    ]
    if stop_error == "response_error":
        trial.glue.batch_stop_job_run.return_value = {"Errors": [{"JobRunId": trial.run_id}]}
    elif stop_error == "exception":
        trial.glue.batch_stop_job_run.side_effect = RuntimeError("stop denied")
    else:
        trial.glue.batch_stop_job_run.return_value = {
            "SuccessfulSubmissions": [{"JobName": trial.plan["job"]["Name"], "JobRunId": "other"}]
        }
    result = execute(trial)
    assert not result["stop_accepted"] and result["terminal_confirmed"]
    assert result["outcome"] == "observation_failed"


def test_failed_cleanup_observations_remain_explicitly_unconfirmed(trial):
    trial.glue.get_job_run.side_effect = RuntimeError("cannot observe")
    trial.glue.batch_stop_job_run.side_effect = RuntimeError("cannot stop")
    result = execute(trial)
    assert result["outcome"] == "stop_unconfirmed" and not result["terminal_confirmed"]
    assert trial.glue.get_job_run.call_count == 3
    trial.glue.batch_stop_job_run.assert_called_once()


def test_start_call_time_counts_toward_the_watchdog(trial):
    def slow_start(**kwargs):
        trial.clock.value += 30
        return {"JobRunId": trial.run_id}

    trial.glue.start_job_run.side_effect = slow_start
    trial.glue.get_job_run.return_value = observation(trial, "STOPPED")
    result = execute(trial)
    assert result["outcome"] == "watchdog_limit" and result["terminal_confirmed"]
    assert trial.events[4]["event"] == "stop_intent"


def test_journal_failure_before_submission_never_starts(trial):
    def record(row):
        if row["event"] == "start_intent":
            raise OSError("disk failure")

    with pytest.raises(OSError):
        control.execute_trial(
            trial.glue, trial.s3, trial.plan, record, now=trial.clock.now, pause=trial.clock.pause
        )
    trial.glue.start_job_run.assert_not_called()


def test_journal_failure_after_submission_still_stops_the_known_run(trial):
    def record(row):
        if row["event"] == "run_identified":
            raise OSError("disk failure")

    trial.glue.get_job_run.return_value = observation(trial, "STOPPED")
    with pytest.raises(OSError) as captured:
        control.execute_trial(
            trial.glue, trial.s3, trial.plan, record, now=trial.clock.now, pause=trial.clock.pause
        )
    assert trial.run_id in captured.value.__notes__[0]
    trial.glue.batch_stop_job_run.assert_called_once()


def test_keyboard_interrupt_after_submission_requests_stop_then_propagates(trial):
    trial.glue.get_job_run.side_effect = [KeyboardInterrupt(), observation(trial, "STOPPED")]
    with pytest.raises(KeyboardInterrupt):
        execute(trial)
    trial.glue.batch_stop_job_run.assert_called_once()


def test_interrupt_during_cleanup_does_not_reset_stop_budget_or_submit_twice(trial):
    trial.glue.get_job_run.side_effect = [
        observation(trial, "RUNNING"),
        observation(trial, "RUNNING"),
        KeyboardInterrupt(),
        observation(trial, "STOPPED"),
    ]
    with pytest.raises(KeyboardInterrupt):
        execute(trial)
    assert trial.glue.get_job_run.call_count == 4
    trial.glue.batch_stop_job_run.assert_called_once()
