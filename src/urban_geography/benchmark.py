"""Bounded pilot measurements with optional interleaved exact spatial-join variants."""

import io
import json
import math
import os
import platform
import random
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass
from functools import partial
from importlib.metadata import version
from pathlib import Path
from threading import Event, Timer
from typing import Any

import pyarrow.parquet as pq
from pyspark.sql import DataFrame

from urban_geography.boundary import RetainedBoundary, load_boundary_spec, load_retained_boundary
from urban_geography.config import RunConfig, load_config
from urban_geography.demo import _hash_paths
from urban_geography.ingest import config_key, digest, validate_raw
from urban_geography.lakehouse import CompletedRun, load_completed_run
from urban_geography.release_comparison import _records
from urban_geography.spark import create_session
from urban_geography.spark_metrics import summarize_event_logs
from urban_geography.transform import (
    envelope_candidates,
    geometry_frame,
    proximity,
    proximity_distance_join,
    proximity_envelope,
    select_buildings,
)

COLUMNS = ("building_id", "park_id", "distance_m", "within_threshold", "grid_id")
TOLERANCE_M = 1e-6
OPERATORS = {
    "current_proximity": proximity,
    "envelope_prefilter": proximity_envelope,
    "distance_join": proximity_distance_join,
}


@dataclass(frozen=True)
class BaselinePlan:
    config: RunConfig
    reference: CompletedRun
    raw: Path
    boundary: RetainedBoundary | None
    expected: dict[str, dict[str, Any]]
    output: Path
    inputs: tuple[Path, ...]
    hashes: dict[str, str]
    preflight_seconds: float


def validate_trial_options(trials: int, timeout_seconds: float) -> None:
    if not 5 <= trials <= 10:
        raise ValueError("Use 5..10 measured trials after the separate warmup")
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 300:
        raise ValueError("Trial timeout must be finite and in (0, 300] seconds")


def prepare_baseline(
    config_path: Path,
    reference_path: Path,
    *,
    output: Path,
    raw_root: Path = Path("data/raw"),
    boundary_config: Path | None = None,
    boundary_root: Path = Path("data/boundaries"),
) -> BaselinePlan:
    started = time.perf_counter()
    output = output.resolve()
    if output.exists():
        raise ValueError("Benchmark output exists; choose a fresh directory")
    protected = [
        raw_root.resolve(),
        boundary_root.resolve(),
        reference_path.resolve(),
        Path("data/lakehouse").resolve(),
    ]
    if any(output.is_relative_to(p) or p.is_relative_to(output) for p in protected):
        raise ValueError("Benchmark output must be disjoint from inputs and the live warehouse")
    config, reference = load_config(config_path), load_completed_run(reference_path)
    if reference.summary["config"] != json.loads(json.dumps(config.as_dict())):
        raise ValueError("Reference configuration differs from the benchmark configuration")
    raw = raw_root / config.release / config.area_id / config_key(config)
    if sum((raw / (kind + ".parquet")).stat().st_size for kind in ("building", "land_use")) > min(
        config.max_output_bytes, 16 * 1024 * 1024
    ):
        raise ValueError("Benchmark retained bytes exceed the pilot limit")
    manifest = validate_raw(raw, config)
    for kind in ("building", "land_use"):
        count = pq.read_metadata(raw / (kind + ".parquet")).num_rows
        if count != manifest["extracts"][kind]["rows"] or not 0 < count <= min(
            config.max_features, 20000
        ):
            raise ValueError("Benchmark retained row counts differ or exceed the pilot limit")
    if reference.summary.get("raw_manifest_sha256") != digest(raw / "manifest.json"):
        raise ValueError("Reference raw manifest differs from the retained benchmark input")
    boundary = (
        load_retained_boundary(load_boundary_spec(boundary_config), boundary_root)
        if boundary_config
        else None
    )
    if reference.summary["selection"]["boundary"] != (boundary.as_dict() if boundary else None):
        raise ValueError("Reference boundary differs from the benchmark selection")
    expected = {
        key: {name: row[name] for name in COLUMNS} for key, row in _records(reference).items()
    }
    inputs = [
        config_path,
        raw,
        reference.path / "summary.json",
        *(reference.path / name for name in reference.summary["files"]),
    ]
    if boundary is not None and boundary_config is not None:
        inputs.extend(
            [
                boundary_config,
                boundary_root
                / boundary.spec.source_release
                / boundary.spec.area_id
                / boundary.spec.expected_geometry_sha256[:16],
            ]
        )
    return BaselinePlan(
        config,
        reference,
        raw,
        boundary,
        expected,
        output,
        tuple(inputs),
        _hash_paths(inputs),
        time.perf_counter() - started,
    )


def verify_results(
    rows: Sequence[dict[str, Any]], expected: dict[str, dict[str, Any]], threshold_m: float
) -> dict[str, Any]:
    actual = {row["building_id"]: row for row in rows}
    if len(actual) != len(rows) or set(actual) != set(expected):
        raise ValueError("Benchmark result key coverage or uniqueness differs")
    maximum = 0.0
    for key, row in actual.items():
        reference = expected[key]
        if set(row) != set(COLUMNS) or any(
            row[name] != reference[name] for name in ("park_id", "within_threshold", "grid_id")
        ):
            raise ValueError("Benchmark matched status, park or grid differs")
        distance, target = row["distance_m"], reference["distance_m"]
        if distance is None and target is None:
            continue
        if (
            distance is None
            or target is None
            or not math.isfinite(distance)
            or not 0 <= distance <= threshold_m
        ):
            raise ValueError("Benchmark distance semantics differ")
        error = abs(distance - target)
        maximum = max(maximum, error)
        if error > TOLERANCE_M:
            raise ValueError("Benchmark distance exceeds the equivalence tolerance")
    fingerprint = digest_rows(rows)
    return {
        "rows": len(rows),
        "matched": sum(row["within_threshold"] for row in rows),
        "distance_tolerance_m": TOLERANCE_M,
        "max_distance_error_m": maximum,
        "output_sha256": fingerprint,
    }


def digest_rows(rows: Sequence[dict[str, Any]]) -> str:
    import hashlib

    canonical = sorted(json.dumps(row, sort_keys=True, allow_nan=False) for row in rows)
    return hashlib.sha256(json.dumps(canonical).encode("utf-8")).hexdigest()


def time_action(
    action: Callable[[], list[dict[str, Any]]],
    cancel: Callable[[], None],
    timeout_seconds: float,
) -> tuple[list[dict[str, Any]] | None, dict[str, Any]]:
    expired = Event()
    cancellation_errors = []

    def expire() -> None:
        expired.set()
        try:
            cancel()
        except Exception as error:
            cancellation_errors.append(type(error).__name__ + ": " + str(error)[:1000])

    timer = Timer(timeout_seconds, expire)
    timer.daemon = True
    timer.start()
    started = time.perf_counter()
    rows, error_text = None, None
    try:
        rows = action()
    except Exception as error:
        error_text = type(error).__name__ + ": " + str(error)[:2000]
    finally:
        elapsed = time.perf_counter() - started
        timer.cancel()
        timer.join(timeout=1)
    status = "censored" if expired.is_set() else ("failed" if error_text else "complete")
    return rows, {
        "status": status,
        "collect_wall_seconds": elapsed,
        "error": error_text,
        "cancellation_errors": cancellation_errors,
        "cancellation_still_running": timer.is_alive(),
    }


def _collect(frame: DataFrame) -> list[dict[str, Any]]:
    return [row.asDict() for row in frame.collect()]


def _resource_limits() -> dict[str, Any]:
    values: dict[str, Any] = {
        "machine": platform.machine(),
        "visible_logical_cpus": os.cpu_count(),
        "cgroup_cpu_max": None,
        "cgroup_memory_max": None,
    }
    for key, name in (("cgroup_cpu_max", "cpu.max"), ("cgroup_memory_max", "memory.max")):
        path = Path("/sys/fs/cgroup") / name
        if path.is_file():
            values[key] = path.read_text().strip()
    return values


def trial_schedule(
    trials: int, compare_envelope: bool, seed: int, *, compare_distance_join: bool = False
) -> list[dict[str, Any]]:
    """One warmup each, then seeded/interleaved rounds over identical cached inputs."""
    variants = [("baseline", "current_proximity")]
    if compare_envelope:
        variants.append(("envelope", "envelope_prefilter"))
    if compare_distance_join:
        variants.append(("distance_join", "distance_join"))
    rng = random.Random(seed)
    schedule = []
    for index in range(trials + 1):
        order = list(variants)
        if len(variants) > 1:
            rng.shuffle(order)
        for prefix, variant in order:
            suffix = "warmup" if index == 0 else f"trial-{index:02d}"
            schedule.append(
                {
                    "group": f"{prefix}.{suffix}",
                    "variant": variant,
                    "warmup": index == 0,
                    "round": index,
                }
            )
    return schedule


def run_baseline(
    plan: BaselinePlan,
    *,
    trials: int = 5,
    timeout_seconds: float = 60,
    compare_envelope: bool = False,
    order_seed: int = 1729,
    compare_distance_join: bool = False,
) -> dict[str, Any]:
    validate_trial_options(trials, timeout_seconds)
    plan.output.mkdir(parents=True, exist_ok=False)
    events_dir = plan.output / "spark-events"
    started = time.perf_counter()
    spark = create_session(events_dir)
    startup_seconds = time.perf_counter() - started
    schedule = trial_schedule(
        trials, compare_envelope, order_seed, compare_distance_join=compare_distance_join
    )
    groups = ["baseline.prepare", *(trial["group"] for trial in schedule)]
    if compare_envelope:
        groups.append("benchmark.diagnostics")
    candidate_counts, diagnostic_seconds = None, None
    samples: list[dict[str, Any]] = []
    failure = None
    settings: dict[str, Any] = {}
    preparation_seconds, building_count, park_count = None, None, None
    try:
        settings = {
            name: spark.conf.get(name)
            for name in (
                "spark.sql.shuffle.partitions",
                "spark.sql.adaptive.enabled",
                "spark.sql.autoBroadcastJoinThreshold",
            )
        }
        settings.update(
            master=spark.sparkContext.master,
            default_parallelism=spark.sparkContext.defaultParallelism,
            driver_memory=spark.sparkContext.getConf().get("spark.driver.memory"),
        )
        jvm = spark.sparkContext._jvm
        if jvm is not None:
            settings["java_version"] = jvm.java.lang.System.getProperty("java.version")
            settings["scala_version"] = jvm.scala.util.Properties.versionNumberString()
        settings["spark_version"] = spark.version
        spark.sparkContext.setJobGroup(groups[0], "Prepare and materialize projected input caches")
        started = time.perf_counter()
        buildings = select_buildings(
            geometry_frame(spark.read.parquet(str(plan.raw / "building.parquet")), plan.config),
            plan.config,
            plan.boundary.geometry_wkb if plan.boundary else None,
        ).cache()
        parks = geometry_frame(
            spark.read.parquet(str(plan.raw / "land_use.parquet")), plan.config
        ).cache()
        settings["input_storage_level"] = str(buildings.storageLevel)
        building_count, park_count = buildings.count(), parks.count()
        preparation_seconds = time.perf_counter() - started
        if building_count != len(plan.expected) or building_count * park_count > 2_000_000:
            raise ValueError("Benchmark selection differs or exceeds the pilot pair limit")
        for trial in schedule:
            group = trial["group"]
            print(f"Measuring {group}", file=sys.stderr, flush=True)
            operator = OPERATORS[trial["variant"]]
            frame = operator(buildings, parks, plan.config.threshold_m).select(*COLUMNS)
            spark.sparkContext.setJobGroup(group, group, interruptOnCancel=True)
            rows, sample = time_action(
                partial(_collect, frame),
                partial(spark.sparkContext.cancelJobGroup, group),
                timeout_seconds,
            )
            # Spark accepts null to clear a property; its pinned Python annotation excludes it.
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)  # type: ignore[arg-type]
            spark.sparkContext.setLocalProperty("spark.job.description", None)  # type: ignore[arg-type]
            sample.update(trial)
            if sample["status"] == "complete" and rows is not None:
                try:
                    sample["equivalence"] = verify_results(
                        rows, plan.expected, plan.config.threshold_m
                    )
                except ValueError as error:
                    sample.update(status="equivalence_failed", error=str(error))
            samples.append(sample)
            (plan.output / f"{group}.json").write_text(
                json.dumps(sample, indent=2, allow_nan=False), encoding="utf-8"
            )
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                frame.explain(mode="extended")
            (plan.output / f"{group}.plan.txt").write_text(buffer.getvalue(), encoding="utf-8")
            if sample["status"] != "complete":
                failure = f"{group}: {sample['status']}"
                break
        if compare_envelope and failure is None and len(samples) == len(schedule):
            spark.sparkContext.setJobGroup("benchmark.diagnostics", "Untimed candidate row count")
            started = time.perf_counter()
            screened = envelope_candidates(buildings, parks, plan.config.threshold_m).count()
            diagnostic_seconds = time.perf_counter() - started
            if not 0 <= screened <= building_count * park_count:
                raise ValueError("Envelope candidate count exceeds the all-pairs upper bound")
            candidate_counts = {
                "all_pairs_upper_bound": building_count * park_count,
                "envelope_candidate_rows": screened,
            }
    except Exception as error:
        failure = type(error).__name__ + ": " + str(error)[:2000]
    finally:
        spark.stop()
    event_paths = sorted(
        p for p in events_dir.iterdir() if p.is_file() and not p.name.startswith(".")
    )
    metrics = summarize_event_logs(event_paths, groups)
    unchanged = _hash_paths(plan.inputs) == plan.hashes
    complete = (
        failure is None
        and len(samples) == len(schedule)
        and all(s["status"] == "complete" for s in samples)
        and unchanged
    )
    timings = {}
    for variant in dict.fromkeys(trial["variant"] for trial in schedule):
        measured = [
            s["collect_wall_seconds"]
            for s in samples
            if not s["warmup"] and s["variant"] == variant
        ]
        timings[variant] = (
            {
                "median": statistics.median(measured),
                "min": min(measured),
                "max": max(measured),
                "sample_stdev": statistics.stdev(measured),
            }
            if complete
            else None
        )
    experiment = {
        (False, False): "current_proximity",
        (True, False): "current_vs_envelope",
        (False, True): "current_vs_distance_join",
        (True, True): "current_vs_envelope_vs_distance_join",
    }[compare_envelope, compare_distance_join]
    report = {
        "status": "complete" if complete else "failed",
        "variant": experiment,
        "scope": "single-machine pilot instrumentation; no speedup or scaling conclusion",
        "timed_operation": "proximity calculation plus full driver collection",
        "cache_policy": (
            "projected inputs cached and materialized; results not cached; JVM/OS caches not reset"
        ),
        "trial_timeout_seconds": timeout_seconds,
        "timeout_policy": "cooperative Spark job-group cancellation, not a hard process deadline",
        "preflight_seconds": plan.preflight_seconds,
        "session_startup_seconds": startup_seconds,
        "input_preparation_seconds": preparation_seconds,
        "measured_collect_seconds": timings["current_proximity"],
        "variant_collect_seconds": timings,
        "trial_order_seed": order_seed if compare_envelope or compare_distance_join else None,
        "scheduled_trials": schedule,
        "candidate_counts": candidate_counts,
        "candidate_count_definition": "separate untimed logical candidate rows, not distance calls",
        "candidate_diagnostic_seconds": diagnostic_seconds,
        "input_hashes": plan.hashes,
        "reference_summary_sha256": plan.reference.summary_sha256,
        "config": plan.config.as_dict(),
        "selection": plan.reference.summary["selection"],
        "buildings": building_count,
        "candidate_parks": park_count,
        "all_pairs_upper_bound": building_count * park_count
        if building_count is not None and park_count is not None
        else None,
        "evaluated_pair_count": None,
        "spark_settings": settings,
        "resources": _resource_limits(),
        "versions": {name: version(name) for name in ("pyspark", "apache-sedona", "pyarrow")},
        "python": platform.python_version(),
        "trials": samples,
        "task_metrics": metrics,
        "failure": failure,
        "inputs_unchanged": unchanged,
    }
    (plan.output / "report.json").write_text(
        json.dumps(report, indent=2, allow_nan=False), encoding="utf-8"
    )
    return report
