"""Named candidate ceilings and unchanged identities of all retained configurations."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from urban_geography.config import RunConfig, load_config
from urban_geography.ingest import config_key
from urban_geography.workload_limits import COIMBRA_EXTENDED_LIMITS, PILOT_LIMITS, workload_limits


@pytest.mark.parametrize(
    "name,key",
    [
        ("coimbra_pilot.json", "d981ab1558f93b9c"),
        ("coimbra_pilot_october.json", "851f8ade72098b64"),
        ("coimbra_performance_trial.json", "6b42872945ba1279"),
    ],
)
def test_legacy_raw_keys_and_configuration_serialization_stay_identical(name, key):
    path = Path("configs") / name
    config = load_config(path)
    assert config_key(config) == key
    assert json.loads(json.dumps(config.as_dict())) == json.loads(path.read_text(encoding="utf-8"))
    assert config.limits == PILOT_LIMITS and "workload_profile" not in config.as_dict()
    assert RunConfig(**config.as_dict()) == config
    # Old source-run readers also accept operational budgets that were historically valid.
    assert replace(config, max_features=40000).limits.max_pairs == 2_000_000


def test_extended_config_is_explicit_pinned_and_serializable():
    config = load_config(Path("configs/coimbra_extended_performance_trial.json"))
    assert config.limits == COIMBRA_EXTENDED_LIMITS
    assert config.max_features == 100000 and config.limits.max_pairs == 20_000_000
    assert config.as_dict()["workload_profile"] == "coimbra_extended_v1"
    assert RunConfig(**config.as_dict()) == config
    with pytest.raises(ValueError, match="bbox"):
        replace(config, workload_profile="pilot")
    PILOT_LIMITS.check_pairs(1000, 2000)
    with pytest.raises(ValueError, match="2 million"):
        PILOT_LIMITS.check_pairs(1001, 2000)
    COIMBRA_EXTENDED_LIMITS.check_pairs(100000, 200)
    with pytest.raises(ValueError, match="20 million"):
        COIMBRA_EXTENDED_LIMITS.check_pairs(100000, 201)


@pytest.mark.parametrize(
    "changes",
    [
        {"bbox": (-8.50, 40.125, -8.345, 40.285)},
        {"release": "2026-09-23.1"},
        {"area_id": "another_area"},
        {"threshold_m": 1000},
        {"max_download_bytes": 256 * 1024**2 + 1},
        {"max_output_bytes": 64 * 1024**2 + 1},
        {"max_features": 100001},
        {"max_source_files": 25},
        {"max_features": True},
        {"max_features": 1.5},
        {"max_features": 0},
        {"workload_profile": "unbounded"},
    ],
)
def test_candidate_profile_cannot_drift_or_silently_raise_ceilings(changes):
    config = load_config(Path("configs/coimbra_extended_performance_trial.json"))
    with pytest.raises(ValueError):
        replace(config, **changes)


def test_larger_bbox_cannot_be_enabled_by_only_increasing_a_pilot_budget():
    config = load_config(Path("configs/coimbra_performance_trial.json"))
    with pytest.raises(ValueError, match="bbox"):
        replace(config, bbox=(-8.505, 40.125, -8.345, 40.285), max_features=100000)
    with pytest.raises(ValueError, match="Unknown workload"):
        workload_limits("spark://unapproved")


@pytest.mark.integration
def test_tiny_synthetic_extended_cohort_processes_profiles_and_compares_without_acquisition(
    tmp_path,
):
    from processing_fixtures import make_processing_case

    from urban_geography.benchmark import prepare_baseline, run_baseline
    from urban_geography.cli import process
    from urban_geography.ingest import digest
    from urban_geography.lakehouse import load_completed_run
    from urban_geography.workload import run_profile

    original = make_processing_case(tmp_path / "original")
    config_path = Path("configs/coimbra_extended_performance_trial.json")
    config = load_config(config_path)
    raw = original.raw_root / config.release / config.area_id / config_key(config)
    raw.mkdir(parents=True)
    manifest = json.loads((original.raw / "manifest.json").read_text(encoding="utf-8"))
    manifest["config"] = config.as_dict()
    for kind in ("building", "land_use"):
        source, target = original.raw / (kind + ".parquet"), raw / (kind + ".parquet")
        target.write_bytes(source.read_bytes())
        manifest["extracts"][kind]["sha256"] = digest(target)
    (raw / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    reference = tmp_path / "reference"
    process(config, raw, reference)
    completed = load_completed_run(reference)
    assert completed.summary["mapped_buildings"] == 3
    assert completed.summary["workload_limits"]["max_pairs"] == 20_000_000
    plan = prepare_baseline(
        config_path, reference, output=tmp_path / "profile", raw_root=original.raw_root
    )
    profile = run_profile(plan)
    assert profile["profile"]["selected_buildings"] == 3
    plan = prepare_baseline(
        config_path, reference, output=tmp_path / "comparison", raw_root=original.raw_root
    )
    result = run_baseline(plan, compare_envelope=True, compare_distance_join=True)
    assert result["status"] == "complete"
    assert result["workload_limits"]["max_features"] == 100000
    assert len(result["trials"]) == 18
    assert all(trial["equivalence"]["rows"] == 3 for trial in result["trials"])
    assert len({trial["equivalence"]["output_sha256"] for trial in result["trials"]}) == 1
    assert result["inputs_unchanged"]


def test_extended_cluster_override_changes_only_explicit_cohort_environment():
    spec = json.loads(
        Path("configs/local_cluster_extended.compose.json").read_text(encoding="utf-8")
    )
    assert spec == {
        "services": {
            "driver": {
                "environment": {
                    "URBAN_CLUSTER_CONFIG": "configs/coimbra_extended_performance_trial.json",
                    "URBAN_CLUSTER_REFERENCE": (
                        "${URBAN_CLUSTER_REFERENCE:?Set the completed extended cohort reference}"
                    ),
                }
            }
        }
    }
