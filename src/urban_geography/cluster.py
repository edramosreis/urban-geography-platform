"""Opt-in two-executor acceptance on retained data; never starts Docker services."""

import io
import json
import statistics
import time
from collections.abc import Sequence
from contextlib import redirect_stdout
from functools import partial
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from urban_geography.benchmark import COLUMNS, BaselinePlan, time_action, verify_results
from urban_geography.demo import _hash_paths
from urban_geography.spark import create_session, execution_settings, executor_ids
from urban_geography.spark_metrics import summarize_event_logs
from urban_geography.transform import geometry_frame, proximity_distance_join, select_buildings


def _save(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def verify_executor_work(
    metrics: dict[str, dict[str, Any]], groups: Sequence[str], registered: Sequence[str]
) -> dict[str, int]:
    """Require observed successful measured tasks on both original executor JVMs."""
    if len(set(registered)) != 2 or "driver" in registered:
        raise ValueError("Expected two distinct non-driver executors")
    totals = dict.fromkeys(registered, 0)
    for group in groups:
        sample = metrics[group]
        counts = sample["task_attempts_by_executor"]
        if not counts or any(identity not in totals for identity in counts):
            raise ValueError("Missing or unexpected executor task identity")
        if sample["failed_task_attempts"] != 0 or sample["killed_task_attempts"] != 0:
            raise ValueError("Failed, killed or unavailable task outcomes")
        for identity, count in counts.items():
            totals[identity] += count
    if not all(count > 0 for count in totals.values()):
        raise ValueError("Measured work did not run on both executors")
    return totals


def _collect(frame: DataFrame) -> list[dict[str, Any]]:
    return [row.asDict() for row in frame.collect()]


def _measure(
    spark: SparkSession, plan: BaselinePlan
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Materialize two point partitions, then warm up and collect five fresh queries."""
    context = spark.sparkContext
    prepared: dict[str, Any] = {}

    def prepare() -> list[dict[str, Any]]:
        buildings = (
            select_buildings(
                geometry_frame(spark.read.parquet(str(plan.raw / "building.parquet")), plan.config),
                plan.config,
                plan.boundary.geometry_wkb if plan.boundary else None,
            )
            .repartition(2)
            .cache()
        )
        parks = geometry_frame(
            spark.read.parquet(str(plan.raw / "land_use.parquet")), plan.config
        ).cache()
        count, park_count = buildings.count(), parks.count()
        if (
            count != len(plan.expected)
            or not 0 < park_count
            or count * park_count > plan.config.limits.max_pairs
        ):
            raise ValueError("Cluster selection differs or exceeds the pilot pair limit")
        partitions = buildings.rdd.getNumPartitions()
        if partitions != 2:
            raise ValueError("Expected two materialized building-point partitions")
        prepared.update(buildings=buildings, parks=parks)
        return [{"buildings": count, "parks": park_count, "point_partitions": partitions}]

    _save(plan.output / "progress.json", {"phase": "preparing_inputs"})
    context.setJobGroup("cluster.prepare", "Project, repartition and cache retained inputs", True)
    counts, preparation = time_action(
        prepare, partial(context.cancelJobGroup, "cluster.prepare"), 60
    )
    _save(plan.output / "preparation.json", preparation)
    if preparation["status"] != "complete" or counts is None:
        raise RuntimeError("Input preparation failed or exceeded 60 seconds")
    details = {**counts[0], "input_preparation_seconds": preparation["collect_wall_seconds"]}
    samples = []
    for index in range(6):
        group = "cluster.warmup" if index == 0 else f"cluster.trial-{index:02d}"
        _save(plan.output / "progress.json", {"phase": "measuring", "group": group})
        frame = proximity_distance_join(
            prepared["buildings"], prepared["parks"], plan.config.threshold_m
        ).select(*COLUMNS)
        context.setJobGroup(group, group, interruptOnCancel=True)
        rows, sample = time_action(
            partial(_collect, frame),
            partial(context.cancelJobGroup, group),
            60,
        )
        sample.update(group=group, warmup=index == 0)
        if sample["status"] == "complete" and rows is not None:
            try:
                sample["equivalence"] = verify_results(rows, plan.expected, plan.config.threshold_m)
            except ValueError as error:
                sample.update(status="equivalence_failed", error=str(error))
        _save(plan.output / f"{group}.json", sample)
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            frame.explain(mode="extended")
        (plan.output / f"{group}.plan.txt").write_text(buffer.getvalue(), encoding="utf-8")
        samples.append(sample)
        if sample["status"] != "complete":
            break
    return samples, details


def run_two_executor(plan: BaselinePlan, *, no_locality_wait: bool = False) -> dict[str, Any]:
    """Requires separately approved running services and an external hard deadline."""
    profile = "two_executor_no_locality_wait" if no_locality_wait else "two_executor"
    plan.output.mkdir(parents=True, exist_ok=False)
    events = plan.output / "spark-events"
    _save(plan.output / "progress.json", {"phase": "starting_spark"})
    spark = None
    samples: list[dict[str, Any]] = []
    details: dict[str, Any] = {}
    settings: dict[str, Any] = {}
    registered: list[str] = []
    failure = None
    started = time.perf_counter()
    try:
        spark = create_session(events, execution_profile=profile)
        details["session_startup_seconds"] = time.perf_counter() - started
        registered = executor_ids(spark)
        if len(registered) != 2 or "driver" in registered:
            raise ValueError("Expected two registered non-driver executors")
        settings = {
            name: spark.sparkContext.getConf().get(name)
            for name in (*execution_settings(profile), "spark.driver.memory")
        }
        settings.update(
            {
                name: spark.conf.get(name)
                for name in (
                    "spark.sql.adaptive.enabled",
                    "spark.sql.shuffle.partitions",
                    "spark.sql.autoBroadcastJoinThreshold",
                )
            }
        )
        settings["spark.locality.wait"] = spark.sparkContext.getConf().get(
            "spark.locality.wait", "3s"
        )
        samples, preparation = _measure(spark, plan)
        details.update(preparation)
        if len(samples) != 6 or any(sample["status"] != "complete" for sample in samples):
            raise RuntimeError("Warmup or measured trial failed; timing summary suppressed")
        if executor_ids(spark) != registered:
            raise ValueError("Executor registry changed during the experiment")
    except Exception as error:
        failure = type(error).__name__ + ": " + str(error)[:2000]
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                failure = failure or "Spark shutdown failed: " + str(error)[:1000]
    groups = ["cluster.prepare", "cluster.warmup", *(f"cluster.trial-{i:02d}" for i in range(1, 6))]
    metrics: dict[str, dict[str, Any]] = {}
    task_counts = None
    try:
        paths = sorted(p for p in events.glob("*") if p.is_file() and not p.name.startswith("."))
        metrics = summarize_event_logs(paths, groups)
        task_counts = verify_executor_work(metrics, groups[2:], registered)
        # Preparation and warmup must also have no lost/failed tasks or unknown executors.
        verify_executor_work(metrics, groups, registered)
    except (ValueError, OSError) as error:
        failure = failure or type(error).__name__ + ": " + str(error)[:2000]
    unchanged = _hash_paths(plan.inputs) == plan.hashes
    complete = failure is None and len(samples) == 6 and unchanged
    measured = [sample["collect_wall_seconds"] for sample in samples if not sample["warmup"]]
    report = {
        "status": "complete" if complete else "failed",
        "execution_profile": profile,
        "scope": "single-host two-executor functionality; no scaling or speedup conclusion",
        "timed_operation": "exact distance join and full driver collection over cached inputs",
        "layout": "same building points repartitioned into two partitions; no replicated rows",
        "preflight_seconds": plan.preflight_seconds,
        **details,
        "trial_timeout_seconds": 60,
        "timeout_policy": "cooperative per-action cancellation; external controller hard deadlines",
        "registered_executor_ids": registered,
        "measured_task_attempts_by_executor": task_counts,
        "spark_settings": settings,
        "input_hashes": plan.hashes,
        "reference_summary_sha256": plan.reference.summary_sha256,
        "config": plan.config.as_dict(),
        "workload_limits": plan.config.limits.as_dict(),
        "trials": samples,
        "task_metrics": metrics,
        "measured_collect_seconds": {
            "median": statistics.median(measured),
            "min": min(measured),
            "max": max(measured),
            "sample_stdev": statistics.stdev(measured),
        }
        if complete
        else None,
        "inputs_unchanged": unchanged,
        "failure": failure,
    }
    _save(plan.output / "report.json", report)
    _save(plan.output / "progress.json", {"phase": "finished", "status": report["status"]})
    return report
