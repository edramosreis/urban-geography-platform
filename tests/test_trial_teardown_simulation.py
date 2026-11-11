"""Synthetic teardown state: exact scope, owned work and gated model removal."""

import importlib.util
import sys
from dataclasses import replace

import pytest
from trial_cleanup_command_fixtures import ROOT, command
from trial_cleanup_fixtures import PRINCIPAL

spec = importlib.util.spec_from_file_location(
    "trial_teardown_simulation", ROOT / "scripts/trial_teardown_simulation.py"
)
model = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = model
spec.loader.exec_module(model)


@pytest.fixture
def world(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(sys.modules["trial_cleanup_inventory"], "ARTIFACTS", tmp_path / "artifacts")
    return model.Simulation(frozen.path, frozen.pin, PRINCIPAL, "clean")


def test_original_synthetic_evidence_passes_real_phase_preparation(world, tmp_path):
    op = world.inputs(tmp_path / "inputs")
    for phase in ("inventory", "quiesce"):
        current = (
            op
            if phase == "quiesce"
            else replace(op, phase=phase, job=None, queries=(), paused=False)
        )
        result = command.prepare(current, tmp_path / "artifacts" / phase)
        assert result["recipe"]["phase"] == phase
    assert not (tmp_path / "artifacts").exists()
    assert len(world.queries) == 3
    assert world.counts() == {
        "catalog": 3,
        "versions": 1,
        "markers": 1,
        "objects": 1,
        "uploads": 1,
        "infrastructure": 16,
    }


@pytest.mark.parametrize(
    "proof",
    [
        {},
        {"phase": "delete-data", "data_cleanup_observed": True, "requires_review": False},
        {
            "phase": "inspect-infrastructure",
            "data_cleanup_observed": True,
            "requires_review": False,
        },
    ],
)
def test_model_removal_requires_both_verified_cleanup_and_empty_state(world, proof):
    with pytest.raises(ValueError):
        world.remove_infrastructure(proof)
    assert not world.removed and len(world.remaining) == 16


@pytest.mark.parametrize(
    "method,params",
    [
        ("delete_object", {"Bucket": "unrelated"}),
        ("delete_object", {}),
        ("delete_bucket", {}),
        ("get_bucket_location", {"Bucket": "unrelated"}),
    ],
)
def test_unscoped_or_unsupported_calls_are_visible_even_if_controller_swallows_error(
    world, method, params
):
    before = world.counts()
    with pytest.raises(ValueError):
        world.respond("s3", method, params, False)
    assert world.counts() == before
    assert world.unexpected == ["s3." + method]


def test_stop_request_can_only_change_the_bound_job(world):
    world.runs["jr_unbound"] = world.runs["jr_owned"] | {"Id": "jr_unbound"}
    with pytest.raises(ValueError):
        world.respond(
            "glue",
            "batch_stop_job_run",
            {"JobName": world.expected["job"], "JobRunIds": ["jr_unbound"]},
            False,
        )
    world.respond(
        "glue",
        "batch_stop_job_run",
        {"JobName": world.expected["job"], "JobRunIds": ["jr_owned"]},
        False,
    )
    assert world.runs["jr_owned"]["JobRunState"] == "STOPPED"
    assert world.runs["jr_unbound"]["JobRunState"] == "RUNNING"


def test_model_returns_detached_replies(world):
    response = world.respond("glue", "get_job", {"JobName": world.expected["job"]}, False)
    response["Job"]["Name"] = "changed"
    assert world.job["Name"] == world.expected["job"]


def test_exact_version_rejection_does_not_delete_data_and_explicit_retry_can_continue(world):
    world.mode = "partial_delete"
    row = world.versions[0]
    params = {
        "Bucket": world.lake,
        "Key": row["Key"],
        "VersionId": row["VersionId"],
        "ExpectedBucketOwner": world.owner,
    }
    with pytest.raises(model.SimulatedError):
        world.respond("s3", "delete_object", params, False)
    assert len(world.versions) == 1 and len(world.markers) == 1
    world.respond("s3", "delete_object", params, False)
    assert not world.versions and len(world.markers) == 1


def test_cleanup_role_cannot_be_assumed_after_simulated_removal(world):
    world.catalog.clear()
    for rows in (world.versions, world.markers, world.objects, world.uploads):
        rows.clear()
    world.remove_infrastructure(
        {"phase": "delete-data", "data_cleanup_observed": True, "requires_review": False}
    )
    with pytest.raises(ValueError):
        world.respond("sts", "assume_role", {}, True)
    assert world.removed and not world.remaining
