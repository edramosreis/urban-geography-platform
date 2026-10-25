"""Analysis comparability and real offline Iceberg release-history acceptance."""

import copy
from pathlib import Path

import pytest

from urban_geography.config import RunConfig
from urban_geography.lakehouse import CompletedRun, analysis_definition, analysis_sha256


def example_summary():
    config = RunConfig("offline_history", "2026-09-23.1", (-8.435, 40.195, -8.415, 40.215))
    return {
        "config": config.as_dict(),
        "metric_version": "park_proximity_v1",
        "selection": {"version": "point_on_surface_bbox_v1", "boundary": None},
    }


def definition_digest(summary):
    return analysis_sha256(CompletedRun(Path("unused"), summary, "0" * 64))


def test_analysis_comparability_ignores_release_and_operational_details():
    original = example_summary()
    later = copy.deepcopy(original)
    later["config"]["release"] = "2026-10-21.0"
    later["config"]["max_download_bytes"] *= 2
    later["config"]["max_features"] *= 2
    later["completed_at_utc"] = "2026-10-25T12:00:00Z"
    later["raw_manifest_sha256"] = "1" * 64
    later["files"] = {"new.parquet": "2" * 64}
    assert definition_digest(original) == definition_digest(later)


@pytest.mark.parametrize("change", ["area", "bbox", "threshold", "metric", "selection", "boundary"])
def test_changed_analysis_meaning_has_a_different_definition(change):
    original = example_summary()
    changed = copy.deepcopy(original)
    if change == "area":
        changed["config"]["area_id"] = "another_area"
    elif change == "bbox":
        changed["config"]["bbox"] = (-8.434, 40.195, -8.415, 40.215)
    elif change == "threshold":
        changed["config"]["threshold_m"] = 400
    elif change == "metric":
        changed["metric_version"] = "park_proximity_v2"
    elif change == "selection":
        changed["selection"]["version"] = "another_selection_v1"
    else:
        changed["selection"]["boundary"] = {
            "area_id": "municipality",
            "source_release": "CAOP2025",
            "geometry_sha256": "3" * 64,
        }
    assert definition_digest(original) != definition_digest(changed)


def test_boundary_provenance_refresh_is_not_an_analysis_change():
    original = example_summary()
    original["selection"]["boundary"] = {
        "area_id": "municipality",
        "source_release": "CAOP2025",
        "geometry_sha256": "3" * 64,
        "manifest_sha256": "4" * 64,
        "acquired_at_utc": "2026-10-24T00:00:00Z",
    }
    refreshed = copy.deepcopy(original)
    refreshed["selection"]["boundary"]["manifest_sha256"] = "5" * 64
    refreshed["selection"]["boundary"]["acquired_at_utc"] = "2026-10-25T00:00:00Z"
    assert definition_digest(original) == definition_digest(refreshed)
    refreshed["selection"]["boundary"]["geometry_sha256"] = "6" * 64
    assert definition_digest(original) != definition_digest(refreshed)


def test_numeric_representation_does_not_change_analysis_identity():
    first = example_summary()
    second = copy.deepcopy(first)
    second["config"]["threshold_m"] = 500
    assert definition_digest(first) == definition_digest(second)
    assert (
        analysis_definition(CompletedRun(Path("unused"), first, "0" * 64))["threshold_m"] == 500.0
    )
