"""Observed task distributions per stage attempt; no Spark startup or inferred row counts."""

import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TypeGuard

from urban_geography.spark_metrics import MAX_EVENT_LOG_BYTES, _value, read_task_log


def _integer(value: Any) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _distribution(values: Sequence[int | float | None]) -> dict[str, Any] | None:
    if not values or any(v is None or not math.isfinite(v) or v < 0 for v in values):
        return None
    known = sorted(v for v in values if v is not None)
    median = statistics.median(known)
    return {
        "tasks": len(known),
        "total": sum(known),
        "min": min(known),
        "median": median,
        "p95_nearest_rank": known[math.ceil(0.95 * len(known)) - 1],
        "max": max(known),
        "zero_tasks": known.count(0),
        "max_over_median": max(known) / median if median > 0 else None,
    }


def _duration(event: dict[str, Any]) -> int | float | None:
    info = event.get("Task Info") or {}
    start, finish = info.get("Launch Time"), info.get("Finish Time")
    if not _integer(start) or not _integer(finish) or finish < start:
        return None
    return finish - start


def _executors(events: Sequence[dict[str, Any]]) -> dict[str, int] | None:
    ids = [(event.get("Task Info") or {}).get("Executor ID") for event in events]
    return (
        dict(sorted(Counter(str(value) for value in ids).items()))
        if all(isinstance(v, str) and v for v in ids)
        else None
    )


def _stage(events: list[dict[str, Any]], info: dict[str, Any]) -> dict[str, Any]:
    reasons = [(event.get("Task End Reason") or {}).get("Reason") for event in events]
    known_outcomes = all(isinstance(reason, str) for reason in reasons)
    successful = [
        event for event, reason in zip(events, reasons, strict=True) if reason == "Success"
    ]
    raw_indices = [(event.get("Task Info") or {}).get("Index") for event in successful]
    known_indices = all(_integer(index) for index in raw_indices)
    indices = [index for index in raw_indices if _integer(index)]
    duplicates = (
        sorted(index for index, count in Counter(indices).items() if count > 1)
        if known_indices
        else None
    )
    unambiguous = known_indices and not duplicates
    expected = info.get("tasks")
    coverage = (
        unambiguous and len(indices) == expected and all(index < expected for index in indices)
        if _integer(expected) and known_indices
        else None
    )
    distributions: dict[str, Any] = {
        name: None for name in ("duration_ms", "input_records", "shuffle_read_records")
    }
    if unambiguous:
        distributions = {
            "duration_ms": _distribution([_duration(event) for event in successful]),
            "input_records": _distribution(
                [_value(event, ("Input Metrics", "Records Read")) for event in successful]
            ),
            "shuffle_read_records": _distribution(
                [
                    _value(event, ("Shuffle Read Metrics", "Total Records Read"))
                    for event in successful
                ]
            ),
        }
    return {
        "stage_name": info.get("name"),
        "stage_completed_event": info.get("completed", False),
        "declared_tasks": expected if _integer(expected) else None,
        "task_attempts": len(events),
        "successful_task_attempts": len(successful),
        "failed_task_attempts": sum(reason not in {"Success", "TaskKilled"} for reason in reasons)
        if known_outcomes
        else None,
        "killed_task_attempts": reasons.count("TaskKilled") if known_outcomes else None,
        "task_attempts_by_executor": _executors(events),
        "successful_task_attempts_by_executor": _executors(successful),
        "successful_task_indices": sorted(indices) if known_indices else None,
        "duplicate_successful_task_indices": duplicates,
        "successful_task_coverage_complete": coverage,
        "distribution_basis": "observed successful tasks with unique stage-local indices",
        "distributions": distributions,
    }


def summarize_stage_tasks(paths: Sequence[Path], groups: Sequence[str]) -> dict[str, Any]:
    """Keep stages/retries separate; unavailable metrics and zero medians stay explicit."""
    if not groups or len(set(groups)) != len(groups) or any(not group for group in groups):
        raise ValueError("Choose distinct nonempty job groups")
    log = read_task_log(paths, groups)
    if (
        len(log.applications) != 1
        or not isinstance(log.applications[0], str)
        or not log.applications[0]
    ):
        raise ValueError("Stage diagnostics require exactly one identified Spark application")
    if not log.versions:
        raise ValueError("Stage diagnostics require a Spark version header")
    results = {}
    for group, events in log.tasks.items():
        attempts = defaultdict(list)
        for event in events:
            stage, attempt = event.get("Stage ID"), event.get("Stage Attempt ID")
            if not _integer(stage) or not _integer(attempt):
                raise ValueError("Stage and attempt identities must be nonnegative integers")
            attempts[(stage, attempt)].append(event)
        results[group] = {
            "task_events_available": bool(events),
            "stages": [
                {
                    "stage_id": stage,
                    "stage_attempt_id": attempt,
                    **_stage(tasks, log.stages.get((stage, attempt), {})),
                }
                for (stage, attempt), tasks in sorted(attempts.items())
            ],
        }
    return {
        "report_type": "spark_stage_task_diagnostics_v1",
        "spark_version": "3.5.6",
        "application_id": log.applications[0],
        "groups": results,
        "interpretation": (
            "Distributions describe observed successful tasks within each stage attempt. "
            "Input/shuffle records are Spark counters, "
            "not inferred geometry rows or distance calls. "
            "Task duration includes waits and does not isolate CPU or prove spatial data skew."
        ),
    }


def _log_paths(directory: Path) -> list[Path]:
    return sorted(
        path for path in directory.iterdir() if path.is_file() and not path.name.startswith(".")
    )


def _log_hashes(paths: Sequence[Path]) -> dict[str, str]:
    hashes = {}
    for path in paths:
        with path.open("rb") as stream:
            hashes[str(path)] = hashlib.file_digest(stream, "sha256").hexdigest()
    return hashes


def write_stage_report(event_dir: Path, groups: Sequence[str], output: Path) -> dict[str, Any]:
    """Inspect retained logs and create one fresh JSON file outside inputs/live data."""
    event_dir, output = event_dir.resolve(), output.resolve()
    if not event_dir.is_dir():
        raise ValueError("Choose a retained Spark event-log directory")
    if output.exists():
        raise ValueError("Report output exists; choose a fresh JSON path")
    if output.is_relative_to(event_dir) or output.is_relative_to(Path("data").resolve()):
        raise ValueError("Report output must be outside event logs and the data directory")
    paths = _log_paths(event_dir)
    if sum(path.stat().st_size for path in paths) > MAX_EVENT_LOG_BYTES:
        raise ValueError("Pilot event logs exceed 128 MiB")
    hashes = _log_hashes(paths)
    report = summarize_stage_tasks(paths, groups)
    if _log_paths(event_dir) != paths or _log_hashes(paths) != hashes:
        raise ValueError("Event logs changed while being inspected; no report published")
    report["input_hashes"] = hashes
    payload = json.dumps(report, indent=2, allow_nan=False) + "\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        stream.write(payload)
    return report
