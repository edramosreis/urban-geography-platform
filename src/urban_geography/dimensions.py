"""Release and study-area dimension contracts for one comparable set of retained runs."""

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from urban_geography.lakehouse import (
    CompletedRun,
    _validate_facts,
    analysis_definition,
    analysis_sha256,
)

RELEASE_SCHEMA = (
    "source_release STRING, source_name STRING, release_label_date DATE, release_revision INT"
)
AREA_SCHEMA = (
    "study_area_sha256 STRING, area_id STRING, west DOUBLE, south DOUBLE, east DOUBLE, "
    "north DOUBLE, metric_crs STRING, selection_version STRING, boundary_id STRING, "
    "boundary_release STRING, boundary_geometry_sha256 STRING"
)


@dataclass(frozen=True)
class ReleaseDimension:
    source_release: str
    source_name: str
    release_label_date: date
    release_revision: int


@dataclass(frozen=True)
class StudyAreaDimension:
    study_area_sha256: str
    area_id: str
    west: float
    south: float
    east: float
    north: float
    metric_crs: str
    selection_version: str
    boundary_id: str | None
    boundary_release: str | None
    boundary_geometry_sha256: str | None


@dataclass(frozen=True)
class SourceRunBinding:
    source_release: str
    area_id: str
    source_run_sha256: str
    study_area_sha256: str


@dataclass(frozen=True)
class DimensionContract:
    analysis_sha256: str
    releases: tuple[ReleaseDimension, ...]
    study_areas: tuple[StudyAreaDimension, ...]
    source_runs: tuple[SourceRunBinding, ...]


def release_dimension(run: CompletedRun) -> ReleaseDimension:
    release = run.summary["config"]["release"]
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\.(\d+)", release)
    if match is None:
        raise ValueError("Release dimension requires an explicit dated source release")
    try:
        label_date = date.fromisoformat(match[1])
    except ValueError as error:
        raise ValueError("Release label requires a valid calendar date") from error
    revision = int(match[2])
    if revision > 2_147_483_647:
        raise ValueError("Release revision exceeds the supported integer range")
    return ReleaseDimension(release, "Overture Maps", label_date, revision)


def study_area_dimension(run: CompletedRun) -> StudyAreaDimension:
    """Keep selection identity independent of release, threshold and processing time."""
    definition = analysis_definition(run)
    west, south, east, north = definition["bbox"]
    boundary = definition["boundary"] or {}
    values: dict[str, Any] = {
        "area_id": definition["area_id"],
        "west": west,
        "south": south,
        "east": east,
        "north": north,
        "metric_crs": definition["metric_crs"],
        "selection_version": definition["selection_version"],
        "boundary_id": boundary.get("area_id"),
        "boundary_release": boundary.get("source_release"),
        "boundary_geometry_sha256": boundary.get("geometry_sha256"),
    }
    identity = json.dumps({"contract_version": 1, **values}, sort_keys=True, separators=(",", ":"))
    key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return StudyAreaDimension(study_area_sha256=key, **values)


def build_dimension_contract(runs: Sequence[CompletedRun]) -> DimensionContract:
    """Deduplicate exact replays; reject incomparable analyses and competing run bindings."""
    if not runs:
        raise ValueError("Dimension contract requires at least one completed run")
    definition_hash = analysis_sha256(runs[0])
    releases: dict[str, ReleaseDimension] = {}
    areas: dict[str, StudyAreaDimension] = {}
    bindings: dict[tuple[str, str], SourceRunBinding] = {}
    for run in runs:
        if analysis_sha256(run) != definition_hash:
            raise ValueError("Dimension runs have different analysis definitions")
        if not re.fullmatch(r"[0-9a-f]{64}", run.summary_sha256):
            raise ValueError("Dimension source-run hash is invalid")
        release, area = release_dimension(run), study_area_dimension(run)
        binding = SourceRunBinding(
            release.source_release, area.area_id, run.summary_sha256, area.study_area_sha256
        )
        scope = (release.source_release, area.area_id)
        if scope in bindings and bindings[scope] != binding:
            raise ValueError("Choose one completed source run per release and study area")
        releases[release.source_release] = release
        areas[area.study_area_sha256] = area
        bindings[scope] = binding
    return DimensionContract(
        definition_hash,
        tuple(releases[key] for key in sorted(releases)),
        tuple(areas[key] for key in sorted(areas)),
        tuple(bindings[key] for key in sorted(bindings)),
    )


def dimension_frames(
    spark: SparkSession, contract: DimensionContract
) -> tuple[DataFrame, DataFrame]:
    """Construct typed in-memory relations, including entirely NULL boundary columns."""
    return (
        spark.createDataFrame([asdict(row) for row in contract.releases], RELEASE_SCHEMA),
        spark.createDataFrame([asdict(row) for row in contract.study_areas], AREA_SCHEMA),
    )


def _unique_keys(frame: DataFrame, key: str) -> None:
    if frame.filter(F.col(key).isNull() | (F.col(key) == "")).limit(1).count():
        raise ValueError(f"Missing dimension key: {key}")
    if frame.select(key).distinct().count() != frame.count():
        raise ValueError(f"Duplicate dimension key: {key}")


def _matching_row(frame: DataFrame, key: str, expected: dict[str, Any]) -> DataFrame:
    selected = frame.filter(F.col(key) == expected[key])
    if selected.count() != 1:
        raise ValueError(f"Missing dimension reference: {key}")
    invalid = F.lit(False)
    for name, value in expected.items():
        invalid = invalid | ~F.col(name).eqNullSafe(F.lit(value))
    if selected.filter(invalid).limit(1).count():
        raise ValueError(f"Dimension attributes disagree with the run: {key}")
    return selected


def join_run_dimensions(
    facts: DataFrame,
    run: CompletedRun,
    releases: DataFrame,
    study_areas: DataFrame,
) -> DataFrame:
    """Validate one reference fact run, then enrich it with exactly one row per dimension."""
    total = _validate_facts(facts, run.summary)
    _unique_keys(releases, "source_release")
    _unique_keys(study_areas, "study_area_sha256")
    release = _matching_row(releases, "source_release", asdict(release_dimension(run)))
    area_row = study_area_dimension(run)
    area = _matching_row(study_areas, "study_area_sha256", asdict(area_row))
    # Shared selection/boundary attributes were checked above; retain the original fact columns.
    joined = (
        facts.withColumn("study_area_sha256", F.lit(area_row.study_area_sha256))
        .withColumn("source_run_sha256", F.lit(run.summary_sha256))
        .join(release, "source_release", "inner")
        .join(
            area.select(
                "study_area_sha256", "area_id", "west", "south", "east", "north", "metric_crs"
            ),
            ["study_area_sha256", "area_id"],
            "inner",
        )
    )
    if joined.count() != total:
        raise ValueError("Dimension joins changed the building count")
    return joined
