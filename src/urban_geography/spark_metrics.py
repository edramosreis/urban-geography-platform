"""Selected Spark 3.5.6 task metrics attributed in event order to sequential job groups."""

import json
import math
import statistics
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

METRICS = {
    "executor_run_ms": ("Executor Run Time",),
    "executor_cpu_ns": ("Executor CPU Time",),
    "gc_ms": ("JVM GC Time",),
    "memory_spill_bytes": ("Memory Bytes Spilled",),
    "disk_spill_bytes": ("Disk Bytes Spilled",),
    "input_bytes": ("Input Metrics", "Bytes Read"),
    "input_records": ("Input Metrics", "Records Read"),
    "shuffle_remote_bytes": ("Shuffle Read Metrics", "Remote Bytes Read"),
    "shuffle_local_bytes": ("Shuffle Read Metrics", "Local Bytes Read"),
    "shuffle_write_bytes": ("Shuffle Write Metrics", "Shuffle Bytes Written"),
}


def _value(event: dict[str, Any], path: tuple[str, ...]) -> int | float | None:
    value: Any = event.get("Task Metrics")
    for name in path:
        if not isinstance(value, dict):
            return None
        value = value.get(name)
    return (
        value
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0
        else None
    )


def summarize_event_logs(paths: Sequence[Path], groups: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Keep missing metrics unavailable and attribute tasks using event order."""
    if sum(path.stat().st_size for path in paths) > 128 * 1024 * 1024:
        raise ValueError("Pilot event logs exceed 128 MiB")
    tasks: dict[str, list[dict[str, Any]]] = {group: [] for group in groups}
    stages: dict[int, str | None] = {}
    for path in paths:
        with path.open(encoding="utf-8") as stream:
            for number, line in enumerate(stream, 1):
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"Invalid Spark event JSON at {path.name}:{number}") from error
                kind = event.get("Event")
                if kind == "SparkListenerLogStart" and event.get("Spark Version") != "3.5.6":
                    raise ValueError("Unsupported Spark event-log version")
                if kind == "SparkListenerApplicationStart":
                    stages.clear()
                elif kind == "SparkListenerJobStart":
                    group = (event.get("Properties") or {}).get("spark.jobGroup.id")
                    for stage in event["Stage IDs"]:
                        stages[stage] = group
                elif kind == "SparkListenerTaskEnd":
                    group = stages.get(event["Stage ID"])
                    if group is not None and group in tasks:
                        tasks[group].append(event)
    results = {}
    for group, events in tasks.items():
        metrics: dict[str, Any] = {"task_metrics_available": bool(events)}
        metrics["task_attempts"] = len(events) if events else None
        identities = [(e.get("Task Info") or {}).get("Executor ID") for e in events]
        metrics["task_attempts_by_executor"] = (
            dict(sorted(Counter(identities).items()))
            if events and all(isinstance(identity, str) and identity for identity in identities)
            else None
        )
        metrics["stage_attempts"] = (
            len({(e["Stage ID"], e.get("Stage Attempt ID")) for e in events}) if events else None
        )
        reasons = [(e.get("Task End Reason") or {}).get("Reason") for e in events]
        known_reasons = bool(events) and all(isinstance(reason, str) for reason in reasons)
        metrics["failed_task_attempts"] = (
            sum(r not in {"Success", "TaskKilled"} for r in reasons) if known_reasons else None
        )
        metrics["killed_task_attempts"] = reasons.count("TaskKilled") if known_reasons else None
        for name, key_path in METRICS.items():
            values = [_value(event, key_path) for event in events]
            metrics[name] = (
                sum(v for v in values if v is not None)
                if events and all(v is not None for v in values)
                else None
            )
        cpu = metrics.pop("executor_cpu_ns")
        metrics["executor_cpu_seconds"] = cpu / 1e9 if cpu is not None else None
        remote, local = metrics["shuffle_remote_bytes"], metrics["shuffle_local_bytes"]
        metrics["shuffle_read_bytes"] = (
            remote + local if remote is not None and local is not None else None
        )
        durations = []
        for event in events:
            info = event.get("Task Info", {})
            start, finish = info.get("Launch Time"), info.get("Finish Time")
            if (
                isinstance(start, (int, float))
                and isinstance(finish, (int, float))
                and finish >= start
            ):
                durations.append(finish - start)
        available = bool(events) and len(durations) == len(events)
        ordered = sorted(durations)
        metrics["task_duration_ms"] = {
            "median": statistics.median(durations) if available else None,
            "p95_nearest_rank": ordered[math.ceil(0.95 * len(ordered)) - 1] if available else None,
            "max": max(durations) if available else None,
        }
        results[group] = metrics
    return results
