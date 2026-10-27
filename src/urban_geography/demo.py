"""Offline retained-input demonstration using the existing processing and publication APIs."""

import json
import os
import subprocess
import sys
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pyspark.sql import functions as F

from urban_geography.boundary import RetainedBoundary, load_boundary_spec, load_retained_boundary
from urban_geography.cli import process
from urban_geography.config import RunConfig, load_config
from urban_geography.dimension_publication import publish_dimensions, read_dimension_publication
from urban_geography.gold_storage import StoredGold, publish_gold_run, read_gold_run
from urban_geography.ingest import config_key, digest, validate_raw
from urban_geography.lakehouse import (
    HISTORY_TYPES,
    CompletedRun,
    analysis_definition,
    evolve_history_schema,
    load_completed_run,
    publish_release,
)
from urban_geography.spark import create_session


@dataclass(frozen=True)
class DemoPlan:
    config_paths: tuple[Path, ...]
    configs: tuple[RunConfig, ...]
    raw_paths: tuple[Path, ...]
    raw_root: Path
    boundary_config: Path | None
    boundary_root: Path
    boundary: RetainedBoundary | None
    output: Path
    inputs: tuple[Path, ...]
    input_hashes: dict[str, str]


def _hash_paths(paths: Sequence[Path]) -> dict[str, str]:
    return {
        str(file.resolve()): digest(file)
        for root in paths
        for file in ([root] if root.is_file() else sorted(root.rglob("*")))
        if file.is_file()
    }


def prepare_demo(
    config_paths: Sequence[Path],
    *,
    raw_root: Path,
    output: Path,
    boundary_config: Path | None = None,
    boundary_root: Path = Path("data/boundaries"),
) -> DemoPlan:
    """Validate both retained releases and output isolation without starting Spark or fetching."""
    output = output.resolve()
    if output.exists():
        raise ValueError("Demo output exists; choose a fresh directory")
    if len(config_paths) != 2:
        raise ValueError("Demo requires exactly two distinct retained source releases")
    pairs = sorted(
        ((p.resolve(), load_config(p)) for p in config_paths), key=lambda pair: pair[1].release
    )
    configs = tuple(config for _, config in pairs)
    if len({c.release for c in configs}) != 2:
        raise ValueError("Demo requires exactly two distinct retained source releases")
    if any(
        getattr(configs[0], name) != getattr(configs[1], name)
        for name in ("area_id", "bbox", "metric_crs", "threshold_m")
    ):
        raise ValueError("Demo source configurations must describe comparable analyses")
    protected = [raw_root.resolve(), boundary_root.resolve(), Path("data/lakehouse").resolve()]
    if any(output.is_relative_to(p) or p.is_relative_to(output) for p in protected):
        raise ValueError("Demo output must be disjoint from retained inputs and the live warehouse")
    raws = tuple(raw_root / c.release / c.area_id / config_key(c) for c in configs)
    manifests = [validate_raw(raw, c) for raw, c in zip(raws, configs, strict=True)]
    for raw, config, manifest in zip(raws, configs, manifests, strict=True):
        for kind in ("building", "land_use"):
            count = pq.read_metadata(raw / (kind + ".parquet")).num_rows
            if count != manifest["extracts"][kind]["rows"] or not 0 < count <= min(
                config.max_features, 20000
            ):
                raise ValueError("Retained demo row counts differ or exceed the pilot limit")
        if sum(
            (raw / (kind + ".parquet")).stat().st_size for kind in ("building", "land_use")
        ) > min(config.max_output_bytes, 16 * 1024 * 1024):
            raise ValueError("Retained demo bytes exceed the pilot limit")
    if sum(m["extracts"]["building"]["rows"] for m in manifests) > 40_000:
        raise ValueError("Retained demo building inputs exceed the pilot limit")
    boundary = (
        load_retained_boundary(load_boundary_spec(boundary_config), boundary_root)
        if boundary_config
        else None
    )
    inputs = [*(p for p, _ in pairs), *raws]
    if boundary is not None and boundary_config is not None:
        inputs.extend(
            [
                boundary_config.resolve(),
                boundary_root
                / boundary.spec.source_release
                / boundary.spec.area_id
                / boundary.spec.expected_geometry_sha256[:16],
            ]
        )
    return DemoPlan(
        tuple(p for p, _ in pairs),
        configs,
        raws,
        raw_root,
        boundary_config,
        boundary_root,
        boundary,
        output,
        tuple(inputs),
        _hash_paths(inputs),
    )


def _stage(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _oracle(plan: DemoPlan, config: Path, original: Path, repeated: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        "scripts/verify_pilot.py",
        "--config",
        str(config),
        "--raw-root",
        str(plan.raw_root),
        "--output",
        str(repeated),
        "--compare-output",
        str(original),
    ]
    if plan.boundary_config:
        command.extend(
            [
                "--boundary-config",
                str(plan.boundary_config),
                "--boundary-root",
                str(plan.boundary_root),
            ]
        )
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "PROJ_NETWORK": "OFF", "PYTHONOPTIMIZE": "0"},
    )
    if result.returncode:
        raise RuntimeError("Independent verifier failed: " + result.stderr[-2000:])
    return json.loads(result.stdout)


def _canonical(rows: list[dict[str, Any]]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, allow_nan=False) for row in rows)


def _check_saved(saved: StoredGold, runs: Sequence[CompletedRun]) -> list[dict[str, Any]]:
    columns = [
        "source_release",
        "area_id",
        "source_run_sha256",
        "grid_id",
        "mapped_buildings",
        "buildings_within",
        "share_within",
    ]
    expected = [
        {
            **{name: row[name] for name in columns if name != "source_run_sha256"},
            "source_run_sha256": run.summary_sha256,
        }
        for run in runs
        for row in pq.read_table(run.path / "grid_proximity").to_pylist()
    ]
    actual = [row.asDict() for row in saved.grids.select(*columns).collect()]
    if _canonical(actual) != _canonical(expected):
        raise ValueError("Stored Gold grids differ from the processing references")
    totals = [row.asDict() for row in saved.releases.collect()]
    for run in runs:
        summary = run.summary
        matched_key = (
            "buildings_within_500m"
            if summary["config"]["threshold_m"] == 500
            else "buildings_within"
        )
        wanted = {
            "source_release": summary["config"]["release"],
            "area_id": summary["config"]["area_id"],
            "source_run_sha256": run.summary_sha256,
            "mapped_buildings": summary["mapped_buildings"],
            "buildings_within": summary[matched_key],
            "share_within": summary["share_within"],
        }
        if wanted not in totals:
            raise ValueError("Stored release totals differ from the processing summaries")
    if len(totals) != len(runs) or saved.analysis_definition != analysis_definition(runs[0]):
        raise ValueError("Stored Gold release coverage or analysis definition differs")
    return totals


def run_demo(plan: DemoPlan) -> dict[str, Any]:
    """Reprocess, independently verify, publish and reopen one fresh isolated local slice."""
    plan.output.mkdir(parents=True, exist_ok=False)
    runs: list[CompletedRun] = []
    verification: list[dict[str, Any]] = []
    for config_path, config, raw in zip(
        plan.config_paths, plan.configs, plan.raw_paths, strict=True
    ):
        original = plan.output / "processed" / config.release
        repeated = plan.output / "reprocessed" / config.release
        for destination in (original, repeated):
            _stage(f"Processing retained release {config.release}: {destination.parent.name}")
            process(config, raw, destination, boundary=plan.boundary)
        _stage(f"Independent geometry and repeatability check: {config.release}")
        verification.append(_oracle(plan, config_path, original, repeated))
        runs.append(load_completed_run(original))
    warehouse, publications = plan.output / "warehouse", plan.output / "dimension-publications"
    _stage(
        "Publishing fact history, schema extension, dimensions and Gold in the isolated warehouse"
    )
    spark = create_session(plan.output / "events" / "publication", warehouse=warehouse)
    try:
        first = publish_release(spark, runs[0])
        payloads = {
            str(p): digest(p) for p in warehouse.rglob("*") if p.suffix in {".parquet", ".avro"}
        }
        evolution = evolve_history_schema(spark, expected_schema_id=0)
        if payloads != {
            str(p): digest(p) for p in warehouse.rglob("*") if p.suffix in {".parquet", ".avro"}
        }:
            raise ValueError("Schema evolution changed existing payload files")
        second = publish_release(spark, runs[1], expected_snapshot_id=first["snapshot_id"])
        historical = spark.sql(
            f"SELECT * FROM {first['table']} VERSION AS OF {first['snapshot_id']}"
        )
        expected_first = (
            spark.read.parquet(str(runs[0].path / "building_proximity"))
            .withColumn("source_run_sha256", F.lit(runs[0].summary_sha256))
            .select(*HISTORY_TYPES)
        )
        if (
            len(historical.columns) != len(HISTORY_TYPES)
            or historical.select(*HISTORY_TYPES).exceptAll(expected_first).limit(1).count()
            or expected_first.exceptAll(historical.select(*HISTORY_TYPES)).limit(1).count()
        ):
            raise ValueError("Historical fact snapshot/schema readback differs")
        current = spark.table(first["table"])
        legacy = current.filter(F.col("source_release") == plan.configs[0].release)
        newer = current.filter(F.col("source_release") == plan.configs[1].release)
        if (
            legacy.filter(F.col("processed_at_utc").isNotNull()).limit(1).count()
            or newer.filter(F.col("processed_at_utc").isNull()).limit(1).count()
        ):
            raise ValueError("Processing-time schema extension values differ")
        dimensions = publish_dimensions(spark, runs, publication_root=publications)
        publication_path = Path(dimensions["path"])
        gold = publish_gold_run(spark, publication_path)
        _check_saved(read_gold_run(spark, gold["gold_run_sha256"]), runs)
        _stage("Verifying exact publication retries and unchanged table files")
        before_replay = _hash_paths([warehouse, publications])
        replay = [publish_release(spark, run) for run in runs]
        replay.append(evolve_history_schema(spark, expected_schema_id=0))
        replay.append(publish_dimensions(spark, runs, publication_root=publications))
        replay.append(publish_gold_run(spark, publication_path))
        if any(receipt["status"] != "reused" for receipt in replay):
            raise ValueError("A publication replay was not reused")
        if _hash_paths([warehouse, publications]) != before_replay:
            raise ValueError("Publication replay changed table or completion files")
    finally:
        spark.stop()
    _stage("Reopening saved dimensions and Gold in a fresh Spark session")
    spark = create_session(plan.output / "events" / "readback", warehouse=warehouse)
    try:
        reopened = read_dimension_publication(spark, publication_path)
        saved = read_gold_run(spark, gold["gold_run_sha256"], snapshot_id=gold["snapshot_id"])
        totals = _check_saved(saved, runs)
        if reopened.facts.count() != sum(run.summary["mapped_buildings"] for run in runs):
            raise ValueError("Reopened dimension/fact joins changed cardinality")
    finally:
        spark.stop()
    if _hash_paths(plan.inputs) != plan.input_hashes:
        raise ValueError("Retained inputs changed during the demonstration")
    report = {
        "status": "complete",
        "input_hashes": plan.input_hashes,
        "source_releases": [c.release for c in plan.configs],
        "processing_runs": [
            {
                "path": str(run.path),
                "summary_sha256": run.summary_sha256,
                "reprocessed_summary_sha256": digest(
                    plan.output / "reprocessed" / run.summary["config"]["release"] / "summary.json"
                ),
                "versions": run.summary["versions"],
            }
            for run in runs
        ],
        "independent_verification": verification,
        "fact_publications": [first, second],
        "schema_evolution": evolution,
        "old_snapshot_readback_matches": True,
        "schema_evolution_payloads_unchanged": True,
        "processing_time_values_verified": True,
        "dimensions": dimensions,
        "gold": gold,
        "release_summaries": totals,
        "replay_statuses": [r["status"] for r in replay],
        "replay_files_unchanged": True,
        "fresh_session_readback": True,
        "retained_inputs_unchanged": True,
    }
    temporary = plan.output / (".acceptance-" + uuid.uuid4().hex + ".json")
    temporary.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    temporary.rename(plan.output / "acceptance.json")
    return report
