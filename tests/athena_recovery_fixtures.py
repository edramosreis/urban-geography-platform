"""Create a genuinely uncertain joined-runner attempt using fake services only."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from athena_control_fixtures import CONTROL, Clock, execution
from athena_preflight_fixtures import CALLER, synthetic_table
from athena_runner_fixtures import RUNNER, load, trial_case

RECOVERY = load("athena_recovery")
CLI = load("recover_athena_pilot")


def unknown_case(tmp_path, monkeypatch):
    trial = trial_case(tmp_path, monkeypatch)
    trial.clients["athena"].start_query_execution.side_effect = TimeoutError()
    with pytest.raises(CONTROL.SubmissionUnknown):
        RUNNER.run_trial(trial.clients, trial.attempt, **trial.options)
    trial.body.close()
    monkeypatch.setattr(RECOVERY, "ARTIFACTS", tmp_path)
    clients = {name: Mock() for name in ("sts", "glue", "athena")}
    clients["sts"].get_caller_identity.return_value = dict(CALLER)
    clients["glue"].get_table.return_value = synthetic_table(trial.plan)
    clients["athena"].get_work_group.return_value = trial.clients[
        "athena"
    ].get_work_group.return_value
    clients["athena"].start_query_execution.return_value = {"QueryExecutionId": "query-001"}
    clients["athena"].get_query_execution.side_effect = [
        execution(trial.attempt, "RUNNING"),
        execution(trial.attempt, "CANCELLED"),
    ]
    clock = Clock()
    options = dict(
        caller_arn=CALLER["Arn"], caller_user_id=CALLER["UserId"], now=clock.now, pause=clock.pause
    )
    return SimpleNamespace(
        trial=trial,
        original=trial.attempt.output,
        pin=trial.attempt.intent_sha256,
        output=tmp_path / "recovery",
        clients=clients,
        options=options,
        clock=clock,
    )
