"""Durable request/token integrity with real local files; no Spark, credentials or AWS."""

import json
from unittest.mock import Mock

import pytest
from athena_control_fixtures import CONTROL as control
from athena_control_fixtures import Clock, enabled_group, execution, make_attempt
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


@pytest.fixture
def client(attempt):
    result = Mock()
    result.get_work_group.return_value = enabled_group(attempt)
    result.start_query_execution.return_value = {"QueryExecutionId": "query-001"}
    result.get_query_execution.return_value = execution(attempt)
    return result


def run(client, attempt, clock=None):
    clock = clock or Clock()
    return control.execute_attempt(client, attempt, now=clock.now, pause=clock.pause)


def test_single_submission_saves_id_before_observation_and_never_reads_results(client, attempt):
    def observed(**kwargs):
        assert json.loads((attempt.output / "query.json").read_bytes())["query_id"] == "query-001"
        assert (attempt.output / "submission.json").exists()
        return execution(attempt)

    client.get_query_execution.side_effect = observed
    result = run(client, attempt)
    assert result["outcome"] == "query_succeeded" and result["terminal_confirmed"]
    assert result["result_verified"] is result["caller_identity_verified"] is False
    assert result["provenance_verified"] is False
    client.start_query_execution.assert_called_once_with(**attempt.intent()["request"])
    client.get_query_execution.assert_called_once_with(QueryExecutionId="query-001")
    client.get_query_results.assert_not_called()
    client.stop_query_execution.assert_not_called()
    with pytest.raises(FileExistsError):
        run(client, attempt)
    assert client.start_query_execution.call_count == 1


def test_disabled_preflight_stops_before_submission_reservation(client, attempt):
    client.get_work_group.return_value["WorkGroup"]["State"] = "DISABLED"
    with pytest.raises(ValueError, match="disabled"):
        run(client, attempt)
    assert not (attempt.output / "submission.json").exists()
    client.start_query_execution.assert_not_called()


def test_intent_changed_during_preflight_never_submits(client, attempt):
    def changed(**kwargs):
        (attempt.output / "intent.json").write_text("{}")
        return enabled

    enabled = enabled_group(attempt)
    client.get_work_group.side_effect = changed
    with pytest.raises(ValueError, match="intent changed"):
        run(client, attempt)
    client.start_query_execution.assert_not_called()


@pytest.mark.parametrize(
    "response", [TimeoutError(), KeyboardInterrupt(), {}, {"QueryExecutionId": "bad/id"}]
)
def test_unknown_submission_preserves_token_without_retry_or_guessed_stop(
    client, attempt, response
):
    original = attempt.intent()["request"]
    if isinstance(response, BaseException):
        client.start_query_execution.side_effect = response
    else:
        client.start_query_execution.return_value = response
    with pytest.raises(control.SubmissionUnknown, match="saved request/token"):
        run(client, attempt)
    assert attempt.intent()["request"] == original
    client.start_query_execution.assert_called_once()
    client.stop_query_execution.assert_not_called()
    with pytest.raises(FileExistsError):
        run(client, attempt)
    assert client.start_query_execution.call_count == 1


def test_running_can_return_to_queued_without_a_new_submission(client, attempt):
    client.get_query_execution.side_effect = [
        execution(attempt, state) for state in ("QUEUED", "RUNNING", "QUEUED", "SUCCEEDED")
    ]
    clock = Clock()
    assert run(client, attempt, clock)["outcome"] == "query_succeeded"
    assert clock.value == 15 and client.start_query_execution.call_count == 1


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_terminal_failure_never_retries_even_when_athena_marks_retryable(client, attempt, state):
    client.get_query_execution.return_value = execution(
        attempt, state, Status={"State": state, "AthenaError": {"Retryable": True}}
    )
    result = run(client, attempt)
    assert result["outcome"] == "query_failed" and result["terminal_confirmed"]
    assert client.start_query_execution.call_count == 1
    client.stop_query_execution.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("Query", "SELECT 1"),
        ("WorkGroup", "other-workgroup"),
        ("QueryExecutionContext", {"Catalog": "other", "Database": "other"}),
        ("StatementType", "DDL"),
        (
            "EngineVersion",
            {"SelectedEngineVersion": "AUTO", "EffectiveEngineVersion": "Athena engine version 3"},
        ),
        ("ResultConfiguration", {"OutputLocation": "s3://other/query-001.csv"}),
        ("ResultReuseConfiguration", {"ResultReuseByAgeConfiguration": {"Enabled": True}}),
        ("Statistics", {"DataScannedInBytes": True}),
        ("Statistics", {"DataScannedInBytes": -1}),
        (
            "Statistics",
            {"DataScannedInBytes": 1, "ResultReuseInformation": {"ReusedPreviousResult": True}},
        ),
        ("ManagedQueryResultsConfiguration", {"Enabled": True}),
        ("QueryResultsS3AccessGrantsConfiguration", {"EnableS3AccessGrants": True}),
        ("ExecutionParameters", ["unreviewed"]),
        ("SubstatementType", "INSERT"),
    ],
)
def test_changed_execution_contract_cancels_only_owned_query(client, attempt, field, value):
    client.get_query_execution.side_effect = [
        execution(attempt, "RUNNING", **{field: value}),
        execution(attempt, "CANCELLED"),
    ]
    result = run(client, attempt)
    assert result["outcome"] == "execution_contract_unverified"
    assert result["terminal_confirmed"] and result["stop_acknowledged"]
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="query-001")


def test_scan_overshoot_is_not_accepted_even_if_query_succeeded(client, attempt):
    cap = attempt.intent()["plan"]["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"]
    response = execution(attempt)
    response["QueryExecution"]["Statistics"]["DataScannedInBytes"] = cap + 1
    client.get_query_execution.return_value = response
    result = run(client, attempt)
    assert result["outcome"] == "scan_limit_exceeded" and result["terminal_confirmed"]
    client.stop_query_execution.assert_not_called()


def test_missing_final_statistics_cannot_claim_success(client, attempt):
    response = execution(attempt)
    del response["QueryExecution"]["Statistics"]
    client.get_query_execution.return_value = response
    result = run(client, attempt)
    assert result["outcome"] == "execution_contract_unverified"
    assert "DataScannedInBytes" in result["last_observation"]["missing"]


def test_queue_can_lack_metrics_and_have_prefix_until_result_is_ready(client, attempt):
    response = execution(attempt, "QUEUED")
    response["QueryExecution"]["ResultConfiguration"] = attempt.intent()["request"][
        "ResultConfiguration"
    ]
    del response["QueryExecution"]["Statistics"]
    client.get_query_execution.side_effect = [response, execution(attempt)]
    assert run(client, attempt)["outcome"] == "query_succeeded"
    client.stop_query_execution.assert_not_called()


@pytest.mark.parametrize("error", [TimeoutError(), ValueError("bad response")])
def test_observation_error_stops_and_success_race_does_not_erase_reason(client, attempt, error):
    client.get_query_execution.side_effect = [error, execution(attempt)]
    result = run(client, attempt)
    assert result["outcome"] == "observation_failed" and result["terminal_confirmed"]
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="query-001")


def test_wrong_observed_id_never_cancels_that_other_query(client, attempt):
    client.get_query_execution.side_effect = [
        execution(attempt, "RUNNING", QueryExecutionId="somebody-else"),
        execution(attempt, "CANCELLED"),
    ]
    assert run(client, attempt)["outcome"] == "observation_failed"
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="query-001")


def test_watchdog_and_stop_confirmation_have_bounded_time_and_call_counts(client, attempt):
    client.get_query_execution.return_value = execution(attempt, "RUNNING")
    clock = Clock()
    result = run(client, attempt, clock)
    assert result["outcome"] == "stop_unconfirmed" and result["reason"] == "watchdog_limit"
    assert result["stop_acknowledged"] and not result["terminal_confirmed"]
    assert clock.value == control.WATCH_SECONDS + control.STOP_SECONDS
    assert (
        client.get_query_execution.call_count
        == control.MAX_OBSERVATIONS + control.MAX_STOP_OBSERVATIONS
    )
    client.stop_query_execution.assert_called_once()


def test_start_duration_counts_against_watchdog(client, attempt):
    clock = Clock()

    def slow_start(**kwargs):
        clock.value = control.WATCH_SECONDS + 1
        return {"QueryExecutionId": "query-001"}

    client.start_query_execution.side_effect = slow_start
    client.get_query_execution.return_value = execution(attempt, "CANCELLED")
    result = run(client, attempt, clock)
    assert result["outcome"] == "watchdog_limit" and result["terminal_confirmed"]
    client.stop_query_execution.assert_called_once()


def test_late_terminal_observation_does_not_claim_on_time_success(client, attempt):
    clock = Clock()

    def slow_observation(**kwargs):
        clock.value = control.WATCH_SECONDS + 1
        return execution(attempt)

    client.get_query_execution.side_effect = slow_observation
    result = run(client, attempt, clock)
    assert result["outcome"] == "watchdog_limit" and result["terminal_confirmed"]
    client.stop_query_execution.assert_not_called()


def test_stop_error_still_observes_terminal_state_without_another_stop(client, attempt):
    client.get_query_execution.side_effect = [TimeoutError(), execution(attempt, "CANCELLED")]
    client.stop_query_execution.side_effect = TimeoutError()
    result = run(client, attempt)
    assert result["terminal_confirmed"] and not result["stop_acknowledged"]
    client.stop_query_execution.assert_called_once()


def test_interrupt_after_submission_still_confirms_stop(client, attempt):
    client.get_query_execution.side_effect = [KeyboardInterrupt(), execution(attempt, "CANCELLED")]
    with pytest.raises(KeyboardInterrupt) as caught:
        run(client, attempt)
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="query-001")
    assert "terminal confirmed: True" in caught.value.__notes__[0]


@pytest.mark.parametrize("fail_at", ["submission_intent", "query_identified", "observation"])
def test_journal_failures_prevent_submission_or_trigger_owned_cleanup(
    client, attempt, monkeypatch, fail_at
):
    original = attempt.record

    def record(event):
        if event["event"] == fail_at:
            raise OSError("disk failure")
        original(event)

    monkeypatch.setattr(attempt, "record", record)
    client.get_query_execution.return_value = execution(attempt, "CANCELLED")
    with pytest.raises(OSError, match="disk failure"):
        run(client, attempt)
    if fail_at == "submission_intent":
        client.start_query_execution.assert_not_called()
        client.stop_query_execution.assert_not_called()
    elif fail_at == "query_identified":
        client.stop_query_execution.assert_called_once_with(QueryExecutionId="query-001")
    else:
        # The observed query was already terminal; no cancellation is necessary.
        client.stop_query_execution.assert_not_called()


def test_query_id_disk_failure_still_stops_known_query(client, attempt, monkeypatch):
    original = control.write_once

    def write(path, content):
        if path.name == "query.json":
            raise OSError("disk failure")
        original(path, content)

    monkeypatch.setattr(control, "write_once", write)
    client.get_query_execution.return_value = execution(attempt, "CANCELLED")
    with pytest.raises(OSError, match="disk failure"):
        run(client, attempt)
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="query-001")


def test_unknown_state_stops_instead_of_waiting_indefinitely(client, attempt):
    client.get_query_execution.side_effect = [
        execution(attempt, "UNKNOWN"),
        execution(attempt, "CANCELLED"),
    ]
    assert run(client, attempt)["outcome"] == "observation_failed"
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="query-001")


def test_cancellation_outage_never_claims_termination(client, attempt):
    client.get_query_execution.side_effect = TimeoutError()
    result = run(client, attempt)
    assert result["outcome"] == "stop_unconfirmed" and not result["terminal_confirmed"]
    assert client.get_query_execution.call_count == 1 + control.MAX_STOP_OBSERVATIONS
    client.stop_query_execution.assert_called_once()


def test_frozen_clock_is_still_bounded_by_observation_counts(client, attempt):
    client.get_query_execution.return_value = execution(attempt, "RUNNING")
    result = control.execute_attempt(client, attempt, now=lambda: 0, pause=lambda seconds: None)
    assert result["outcome"] == "stop_unconfirmed"
    assert (
        client.get_query_execution.call_count
        == control.MAX_OBSERVATIONS + control.MAX_STOP_OBSERVATIONS
    )


@pytest.mark.parametrize("change", ["owner", "encryption", "other_object"])
def test_same_result_bucket_is_insufficient_without_exact_owner_encryption_and_object(
    client, attempt, change
):
    response = execution(attempt)
    config = response["QueryExecution"]["ResultConfiguration"]
    if change == "owner":
        config["ExpectedBucketOwner"] = "999999999999"
    elif change == "encryption":
        config["EncryptionConfiguration"] = {"EncryptionOption": "SSE_KMS"}
    else:
        config["OutputLocation"] = config["OutputLocation"].replace(
            "query-001.csv", "another-query.csv"
        )
    client.get_query_execution.return_value = response
    assert run(client, attempt)["outcome"] == "execution_contract_unverified"


def test_disk_failure_during_stop_is_reported_without_blocking_cancellation(
    client, attempt, monkeypatch
):
    original = attempt.record

    def record(event):
        if event["event"].startswith("stop_"):
            raise OSError("disk failed during cleanup")
        original(event)

    monkeypatch.setattr(attempt, "record", record)
    client.get_query_execution.side_effect = [TimeoutError(), execution(attempt, "CANCELLED")]
    result = run(client, attempt)
    assert result["terminal_confirmed"] and result["journal_errors"] == ["OSError"] * 3
    client.stop_query_execution.assert_called_once()
