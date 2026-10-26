"""Dimension identity, run bindings and offline Spark join acceptance."""

import copy
from dataclasses import replace
from datetime import date

import pytest
from pyspark.sql import functions as F

from urban_geography.dimensions import (
    build_dimension_contract,
    dimension_frames,
    join_run_dimensions,
    release_dimension,
    study_area_dimension,
)
from urban_geography.ingest import digest
from urban_geography.lakehouse import FACT_TYPES


def modified(run, change):
    summary = copy.deepcopy(run.summary)
    change(summary)
    return replace(run, summary=summary)


def boundary_run(run):
    summary = copy.deepcopy(run.summary)
    summary["selection"].update(
        version="point_on_surface_bbox_boundary_v1",
        scope="pilot_bbox_intersection_boundary",
        boundary={
            "area_id": "municipality",
            "source_release": "CAOP2025",
            "geometry_sha256": "3" * 64,
            "manifest_sha256": "4" * 64,
            "attribution": "Synthetic boundary context",
        },
    )
    return replace(run, summary=summary)


def test_area_identity_is_independent_of_observation_and_operational_details(history_run_factory):
    first = boundary_run(history_run_factory())
    key = study_area_dimension(first).study_area_sha256
    later = modified(
        first,
        lambda s: s["config"].update(
            release="2026-10-21.0", threshold_m=400, max_download_bytes=1024
        ),
    )
    later = modified(
        later,
        lambda s: s.update(
            completed_at_utc="2026-10-27T15:00:00Z",
            raw_manifest_sha256="5" * 64,
            metric_version="another_metric",
        ),
    )
    later = modified(later, lambda s: s["selection"]["boundary"].update(manifest_sha256="6" * 64))
    assert study_area_dimension(later).study_area_sha256 == key
    assert len(key) == 64
    assert release_dimension(first).release_label_date == date(2026, 9, 23)
    assert release_dimension(first).release_revision == 1
    assert release_dimension(later).source_release == "2026-10-21.0"
    assert release_dimension(later).release_revision == 0


@pytest.mark.parametrize("change", ["bbox", "boundary_geometry", "boundary_release", "selection"])
def test_selection_changes_version_the_study_area(history_run_factory, change):
    first = boundary_run(history_run_factory())
    summary = copy.deepcopy(first.summary)
    if change == "bbox":
        summary["config"]["bbox"] = [-8.434, 40.195, -8.415, 40.215]
    elif change == "boundary_geometry":
        summary["selection"]["boundary"]["geometry_sha256"] = "7" * 64
    elif change == "boundary_release":
        summary["selection"]["boundary"]["source_release"] = "CAOP-next-synthetic"
    else:
        summary["selection"]["version"] = "another_selection"
    later = replace(first, summary=summary)
    assert study_area_dimension(first).area_id == study_area_dimension(later).area_id
    assert (
        study_area_dimension(first).study_area_sha256
        != study_area_dimension(later).study_area_sha256
    )


def test_contract_deduplicates_exact_replays_and_orders_rows_deterministically(history_run_factory):
    first, second = history_run_factory(), history_run_factory("2026-10-21.0")
    contract = build_dimension_contract([second, first, first])
    assert contract == build_dimension_contract([first, second])
    assert len(contract.releases) == len(contract.source_runs) == 2
    assert len(contract.study_areas) == 1
    assert contract.releases[0].source_release == "2026-09-23.1"
    assert contract.source_runs[0].source_run_sha256 == first.summary_sha256
    assert all(
        binding.study_area_sha256 == contract.study_areas[0].study_area_sha256
        for binding in contract.source_runs
    )


def test_contract_refuses_competing_runs_incomparable_analyses_and_invalid_hashes(
    history_run_factory,
):
    first = history_run_factory()
    with pytest.raises(ValueError, match="at least one"):
        build_dimension_contract([])
    with pytest.raises(ValueError, match="one completed source run"):
        build_dimension_contract([first, history_run_factory(revision=1)])
    other = history_run_factory("2026-10-21.0", bbox=(-8.434, 40.195, -8.415, 40.215))
    with pytest.raises(ValueError, match="different analysis"):
        build_dimension_contract([first, other])
    with pytest.raises(ValueError, match="source-run hash is invalid"):
        build_dimension_contract([replace(first, summary_sha256="invalid")])


@pytest.mark.parametrize("release", ["latest", "2026-02-30.0", "2026-09-23.2147483648"])
def test_release_labels_are_validated(history_run_factory, release):
    run = modified(history_run_factory(), lambda s: s["config"].update(release=release))
    with pytest.raises(ValueError, match="Release"):
        release_dimension(run)


@pytest.mark.integration
def test_dimension_joins_preserve_all_facts_and_release_totals(history_spark, history_run_factory):
    spark = history_spark
    runs = [history_run_factory(), history_run_factory("2026-10-21.0")]
    before = {str(p): digest(p) for run in runs for p in run.path.rglob("*") if p.is_file()}
    contract = build_dimension_contract(runs)
    releases, areas = dimension_frames(spark, contract)
    assert releases.schema["release_label_date"].dataType.simpleString() == "date"
    assert areas.schema["boundary_geometry_sha256"].dataType.simpleString() == "string"
    assert areas.first().boundary_geometry_sha256 is None
    outputs = []
    for run in runs:
        facts = spark.read.parquet(str(run.path / "building_proximity"))
        joined = join_run_dimensions(facts, run, releases, areas)
        assert joined.count() == facts.count() == 2
        assert not joined.select(*FACT_TYPES).exceptAll(facts.select(*FACT_TYPES)).count()
        assert not facts.select(*FACT_TYPES).exceptAll(joined.select(*FACT_TYPES)).count()
        assert joined.filter("within_threshold").count() == 1
        assert (
            joined.filter("NOT within_threshold AND distance_m IS NULL AND park_id IS NULL").count()
            == 1
        )
        assert joined.select("source_run_sha256").distinct().first()[0] == run.summary_sha256
        assert (
            joined.select("study_area_sha256").distinct().first()[0]
            == contract.study_areas[0].study_area_sha256
        )
        outputs.append(joined)
    combined = outputs[0].unionByName(outputs[1])
    assert combined.count() == 4
    assert combined.select("source_release", "area_id", "building_id").distinct().count() == 4
    assert combined.select("building_id").distinct().count() == 2
    assert {str(p): digest(p) for run in runs for p in run.path.rglob("*") if p.is_file()} == before


@pytest.mark.integration
@pytest.mark.parametrize(
    "defect",
    [
        "duplicate_release",
        "duplicate_area",
        "missing_release",
        "missing_area",
        "blank_key",
        "wrong_attributes",
    ],
)
def test_dimension_join_rejects_ambiguous_missing_or_inconsistent_references(
    history_spark, history_run_factory, defect
):
    spark, run = history_spark, history_run_factory()
    releases, areas = dimension_frames(spark, build_dimension_contract([run]))
    if defect == "duplicate_release":
        releases = releases.unionByName(releases)
        message = "Duplicate dimension key: source_release"
    elif defect == "duplicate_area":
        areas = areas.unionByName(areas)
        message = "Duplicate dimension key: study_area_sha256"
    elif defect == "missing_release":
        releases = releases.limit(0)
        message = "Missing dimension reference: source_release"
    elif defect == "missing_area":
        areas = areas.limit(0)
        message = "Missing dimension reference: study_area_sha256"
    elif defect == "blank_key":
        areas = areas.withColumn("study_area_sha256", F.lit(""))
        message = "Missing dimension key"
    else:
        areas = areas.withColumn("west", F.lit(-8.434))
        message = "Dimension attributes disagree"
    facts = spark.read.parquet(str(run.path / "building_proximity"))
    with pytest.raises(ValueError, match=message):
        join_run_dimensions(facts, run, releases, areas)
