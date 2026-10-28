"""Executor evidence gates, inactive topology contract and local negative control."""

import json
from pathlib import Path

import pytest
from processing_fixtures import make_processing_case

import urban_geography.cluster as cluster
from urban_geography.benchmark import prepare_baseline
from urban_geography.cli import process


def metric(counts, failed=0, killed=0):
    return {
        "task_attempts_by_executor": counts,
        "failed_task_attempts": failed,
        "killed_task_attempts": killed,
    }


def test_measured_tasks_must_cover_both_executors_not_each_trial():
    values = {"a": metric({"0": 2}), "b": metric({"0": 1, "1": 3})}
    assert cluster.verify_executor_work(values, ["a", "b"], ["0", "1"]) == {"0": 3, "1": 3}


@pytest.mark.parametrize(
    "values,registered",
    [
        ({"a": metric(None)}, ["0", "1"]),
        ({"a": metric({"0": 1})}, ["0", "1"]),
        ({"a": metric({"0": 1, "2": 1})}, ["0", "1"]),
        ({"a": metric({"0": 1, "1": 1}, failed=1)}, ["0", "1"]),
        ({"a": metric({"0": 1, "1": 1}, killed=1)}, ["0", "1"]),
        ({"a": metric({"0": 1, "1": 1}, failed=None)}, ["0", "1"]),
        ({"a": metric({"driver": 2})}, ["driver", "0"]),
        ({"a": metric({"0": 2})}, ["0", "0"]),
    ],
)
def test_missing_idle_replaced_driver_or_failed_executor_evidence_rejected(values, registered):
    with pytest.raises(ValueError):
        cluster.verify_executor_work(values, ["a"], registered)


def test_inactive_compose_has_bounded_resources_internal_network_and_readonly_inputs():
    path = Path(__file__).resolve().parents[1] / "configs/local_cluster.compose.json"
    spec = json.loads(path.read_text(encoding="utf-8"))
    assert spec["networks"] == {"trial": {"internal": True}}
    services = spec["services"]
    assert set(services) == {"master", "driver", "worker-a", "worker-b"}
    assert sum(item["cpus"] for item in services.values()) == 3
    assert sum(int(item["mem_limit"][:-1]) for item in services.values()) == 5120
    for name, item in services.items():
        assert item["image"] == "urban-geography:local" and item["pull_policy"] == "never"
        assert "ports" not in item and item["networks"] == ["trial"]
        assert item["environment"]["SPARK_LOCAL_IP"] == name
        assert item["volumes"][0] == {
            "type": "bind",
            "source": "..",
            "target": "/workspace",
            "read_only": True,
        }
        assert len(item["volumes"]) == (2 if name == "driver" else 1)
        assert "privileged" not in item and "network_mode" not in item


@pytest.mark.integration
def test_local_tasks_cannot_pass_the_cluster_acceptance_gate(tmp_path, monkeypatch):
    case = make_processing_case(tmp_path)
    # This is intentionally local[2]; no standalone services or extra resources.
    process(case.config, case.raw, case.output)
    plan = prepare_baseline(
        case.config_path, case.output, output=tmp_path / "cluster-negative", raw_root=case.raw_root
    )
    original = cluster.create_session
    original_ids = cluster.executor_ids

    def local_session(events, **kwargs):
        spark = original(events)
        assert original_ids(spark) == []  # Exercise the pinned JVM registry API.
        return spark

    monkeypatch.setattr(cluster, "create_session", local_session)
    monkeypatch.setattr(cluster, "executor_ids", lambda spark: ["0", "1"])
    report = cluster.run_two_executor(plan)
    assert report["status"] == "failed" and report["measured_collect_seconds"] is None
    assert "executor task identity" in report["failure"]
    assert report["point_partitions"] == 2 and report["buildings"] == 2
    assert len(report["trials"]) == 6
    assert all(trial["equivalence"]["rows"] == 2 for trial in report["trials"])
    assert report["inputs_unchanged"]
