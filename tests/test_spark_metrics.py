"""Pure event-log attribution tests; no Spark runtime or network required."""

import json

import pytest

from urban_geography.spark_metrics import summarize_event_logs


def job(stage, group):
    return {
        "Event": "SparkListenerJobStart",
        "Stage IDs": stage,
        "Properties": {"spark.jobGroup.id": group},
    }


def task(stage, cpu=3000000, reason="Success", metrics=True, duration=30):
    event = {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage,
        "Stage Attempt ID": 0,
        "Task End Reason": {"Reason": reason},
        "Task Info": {"Launch Time": 100, "Finish Time": 100 + duration},
    }
    if metrics:
        event["Task Metrics"] = {
            "Executor Run Time": 20,
            "Executor CPU Time": cpu,
            "JVM GC Time": 0,
            "Memory Bytes Spilled": 0,
            "Disk Bytes Spilled": 0,
            "Input Metrics": {"Bytes Read": 10, "Records Read": 2},
            "Shuffle Read Metrics": {"Remote Bytes Read": 5, "Local Bytes Read": 7},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 11},
        }
    return event


def log(tmp_path, events):
    path = tmp_path / "events"
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    return path


def test_metrics_do_not_retroactively_count_cached_parent_tasks(tmp_path):
    path = log(tmp_path, [job([0], "prepare"), task(0, cpu=2000000), job([0, 1], "trial"), task(1)])
    report = summarize_event_logs([path], ["prepare", "trial", "missing"])
    assert report["prepare"]["executor_cpu_seconds"] == 0.002
    assert report["trial"]["executor_cpu_seconds"] == 0.003
    assert report["trial"]["task_attempts"] == 1
    assert report["trial"]["shuffle_read_bytes"] == 12
    assert report["trial"]["shuffle_write_bytes"] == 11
    assert report["trial"]["memory_spill_bytes"] == 0
    assert report["trial"]["task_duration_ms"] == {"median": 30, "p95_nearest_rank": 30, "max": 30}
    assert not report["missing"]["task_metrics_available"]
    assert report["missing"]["task_attempts"] is None and report["missing"]["gc_ms"] is None


def test_missing_task_metrics_are_unavailable_instead_of_zero(tmp_path):
    path = log(tmp_path, [job([1], "trial"), task(1, metrics=False)])
    report = summarize_event_logs([path], ["trial"])["trial"]
    assert report["task_attempts"] == 1
    assert report["executor_cpu_seconds"] is None
    assert report["input_bytes"] is None and report["disk_spill_bytes"] is None


def test_failed_and_killed_attempts_and_duration_distribution_are_retained(tmp_path):
    events = [
        job([1], "trial"),
        task(1, duration=10),
        task(1, reason="ExceptionFailure", metrics=False, duration=50),
        task(1, reason="TaskKilled", duration=100),
    ]
    report = summarize_event_logs([log(tmp_path, events)], ["trial"])["trial"]
    assert report["task_attempts"] == 3
    assert report["failed_task_attempts"] == report["killed_task_attempts"] == 1
    assert report["executor_run_ms"] is None
    assert report["task_duration_ms"] == {"median": 50, "p95_nearest_rank": 100, "max": 100}


def test_unsupported_event_version_fails_explicitly(tmp_path):
    path = log(tmp_path, [{"Event": "SparkListenerLogStart", "Spark Version": "different"}])
    with pytest.raises(ValueError, match="Unsupported Spark"):
        summarize_event_logs([path], ["trial"])


def test_malformed_event_log_fails_explicitly(tmp_path):
    path = tmp_path / "events"
    path.write_text('{"Event":', encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid Spark event JSON"):
        summarize_event_logs([path], ["trial"])
