"""Stage-local measurements, retry isolation and missing evidence without Java."""

import json

import pytest

from urban_geography.task_diagnostics import summarize_stage_tasks


def task(stage=0, attempt=0, index=0, executor="0", reason="Success", duration=10, records=20):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage,
        "Stage Attempt ID": attempt,
        "Task End Reason": {"Reason": reason},
        "Task Info": {
            "Index": index,
            "Executor ID": executor,
            "Launch Time": 100,
            "Finish Time": 100 + duration,
        },
        "Task Metrics": {
            "Input Metrics": {"Records Read": records},
            "Shuffle Read Metrics": {"Total Records Read": records},
        },
    }


def stage(stage=0, attempt=0, tasks=2):
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": stage,
            "Stage Attempt ID": attempt,
            "Number of Tasks": tasks,
            "Stage Name": "test collect",
        },
    }


def job(stages=(0,), group="trial"):
    return {
        "Event": "SparkListenerJobStart",
        "Stage IDs": list(stages),
        "Properties": {"spark.jobGroup.id": group},
    }


def log(tmp_path, events):
    path = tmp_path / "events"
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    return path


def report(tmp_path, events, groups=("trial",)):
    header = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.6"},
        {"Event": "SparkListenerApplicationStart", "App ID": "app-one"},
    ]
    return summarize_stage_tasks([log(tmp_path, [*header, *events])], groups)


def test_stage_distributions_do_not_mix_incomparable_phases(tmp_path):
    result = report(
        tmp_path,
        [
            job((0, 1)),
            task(records=5),
            task(index=1, executor="1", duration=30, records=95),
            stage(),
            task(stage=1, duration=100, records=1000),
            stage(stage=1, tasks=1),
        ],
    )
    first, second = result["groups"]["trial"]["stages"]
    assert first["successful_task_coverage_complete"]
    assert first["task_attempts_by_executor"] == {"0": 1, "1": 1}
    assert first["distributions"]["duration_ms"] == {
        "tasks": 2,
        "total": 40,
        "min": 10,
        "median": 20,
        "p95_nearest_rank": 30,
        "max": 30,
        "zero_tasks": 0,
        "max_over_median": 1.5,
    }
    assert first["distributions"]["shuffle_read_records"]["max_over_median"] == 1.9
    assert second["distributions"]["duration_ms"]["median"] == 100
    assert second["declared_tasks"] == 1


def test_cached_parent_events_stay_with_the_original_group(tmp_path):
    result = report(
        tmp_path,
        [
            job(group="prepare"),
            task(),
            stage(tasks=1),
            job((0, 1)),
            task(stage=1),
            stage(stage=1, tasks=1),
        ],
        groups=("prepare", "trial", "missing"),
    )
    assert [s["stage_id"] for s in result["groups"]["prepare"]["stages"]] == [0]
    assert [s["stage_id"] for s in result["groups"]["trial"]["stages"]] == [1]
    assert result["groups"]["missing"] == {"task_events_available": False, "stages": []}


def test_retries_and_failed_attempts_are_separate_from_successful_task_distributions(tmp_path):
    result = report(
        tmp_path,
        [
            job(),
            task(reason="ExceptionFailure", duration=900),
            stage(tasks=1),
            task(attempt=1),
            task(attempt=1, index=1, reason="TaskKilled"),
            task(attempt=1, index=1, duration=30),
            stage(attempt=1),
        ],
    )
    failed, completed = result["groups"]["trial"]["stages"]
    assert failed["stage_attempt_id"] == 0 and failed["failed_task_attempts"] == 1
    assert failed["distributions"]["duration_ms"] is None
    assert failed["successful_task_coverage_complete"] is False
    assert completed["stage_attempt_id"] == 1 and completed["task_attempts"] == 3
    assert completed["killed_task_attempts"] == 1
    assert completed["distributions"]["duration_ms"]["tasks"] == 2
    assert completed["distributions"]["duration_ms"]["total"] == 40


def test_missing_counters_do_not_become_zero_and_zero_median_has_no_ratio(tmp_path):
    missing = task(index=2)
    missing.pop("Task Metrics")
    result = report(
        tmp_path, [job(), task(records=0), task(index=1, records=0), missing, stage(tasks=3)]
    )
    metrics = result["groups"]["trial"]["stages"][0]["distributions"]
    assert metrics["input_records"] is None and metrics["shuffle_read_records"] is None
    assert metrics["duration_ms"]["tasks"] == 3
    result = report(
        tmp_path,
        [
            job(),
            task(records=0),
            task(index=1, records=0),
            task(index=2, records=99),
            stage(tasks=3),
        ],
    )
    metric = result["groups"]["trial"]["stages"][0]["distributions"]["shuffle_read_records"]
    assert metric["median"] == 0 and metric["max"] == 99 and metric["zero_tasks"] == 2
    assert metric["max_over_median"] is None


@pytest.mark.parametrize("index", [0, None, True])
def test_duplicate_or_unknown_task_indices_suppress_distributions(tmp_path, index):
    result = report(tmp_path, [job(), task(), task(index=index), stage()])
    summary = result["groups"]["trial"]["stages"][0]
    assert all(value is None for value in summary["distributions"].values())
    assert summary["successful_task_coverage_complete"] is not True
    if index == 0 and index is not None:
        assert summary["duplicate_successful_task_indices"] == [0]


def test_partial_stage_unknown_outcome_and_executor_remain_explicit(tmp_path):
    result = report(tmp_path, [job(), task(executor=None), task(index=1, reason=None), stage()])
    summary = result["groups"]["trial"]["stages"][0]
    assert summary["task_attempts_by_executor"] is None
    assert summary["failed_task_attempts"] is None and summary["killed_task_attempts"] is None
    assert summary["successful_task_coverage_complete"] is False
    assert summary["distributions"]["duration_ms"]["tasks"] == 1


def test_missing_stage_metadata_does_not_infer_coverage(tmp_path):
    result = report(tmp_path, [job(), task()])
    summary = result["groups"]["trial"]["stages"][0]
    assert summary["declared_tasks"] is None
    assert summary["stage_completed_event"] is False
    assert summary["successful_task_coverage_complete"] is None
    # Very large declared counts must not allocate a range/set proportional to the count.
    result = report(tmp_path, [job(), task(), stage(tasks=10**12)])
    assert result["groups"]["trial"]["stages"][0]["successful_task_coverage_complete"] is False


@pytest.mark.parametrize("starts", [[], [None], ["app-a", "app-b"], ["app-a", "app-a"]])
def test_missing_or_multiple_applications_cannot_merge_stage_ids(tmp_path, starts):
    events = [{"Event": "SparkListenerLogStart", "Spark Version": "3.5.6"}]
    events += [{"Event": "SparkListenerApplicationStart", "App ID": value} for value in starts]
    with pytest.raises(ValueError, match="exactly one identified"):
        summarize_stage_tasks([log(tmp_path, events)], ["trial"])


@pytest.mark.parametrize("attempt", [-1, None, True])
def test_invalid_attempt_identity_is_rejected(tmp_path, attempt):
    with pytest.raises(ValueError, match="identities"):
        report(tmp_path, [job(), task(attempt=attempt)])


@pytest.mark.parametrize("groups", [[], [""], ["trial", "trial"]])
def test_invalid_group_selection_rejected_before_reading(groups):
    with pytest.raises(ValueError, match="distinct nonempty"):
        summarize_stage_tasks([], groups)


def test_absent_version_header_and_oversized_log_rejected(tmp_path):
    path = log(tmp_path, [{"Event": "SparkListenerApplicationStart", "App ID": "app-one"}])
    with pytest.raises(ValueError, match="version header"):
        summarize_stage_tasks([path], ["trial"])
    with path.open("wb") as stream:
        stream.truncate(128 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="128 MiB"):
        summarize_stage_tasks([path], ["trial"])
