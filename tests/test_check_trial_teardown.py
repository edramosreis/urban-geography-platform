"""Joined real receiver/controller/readback acceptance, with in-memory AWS replies."""

import sys

import pytest
from trial_cleanup_fixtures import PRINCIPAL
from trial_teardown_fixtures import model, rehearsal


@pytest.mark.parametrize("mode", model.MODES)
def test_joined_receiver_and_saved_readback_on_evolving_state(frozen, tmp_path, monkeypatch, mode):
    monkeypatch.setattr(sys.modules["trial_cleanup_inventory"], "ARTIFACTS", tmp_path / "artifacts")
    world = model.Simulation(frozen.path, frozen.pin, PRINCIPAL, mode)
    result = rehearsal.run_case(world, tmp_path / "artifacts" / mode)
    assert result["expected_outcome_verified"] and result["owned_keys_cleared"]
    assert not result["terraform_invoked"] and not result["live_teardown_verified"]
    assert result["live_aws_requests"] == 0
    assert not world.unexpected
    if mode == "changed_target":
        assert result["phases"][-1]["receiver_status"] == "operation_failed"
        assert not (
            tmp_path / "artifacts" / mode / "phases/delete-data/transport-proof.json"
        ).exists()
    if mode == "partial_delete":
        deletion = [p for p in result["phases"] if p["phase"] == "delete-data"]
        assert (
            len(deletion) == 2
            and deletion[0]["requires_review"]
            and not deletion[1]["requires_review"]
        )
    assert [p["phase"] for p in result["phases"]][:2] == ["inventory", "quiesce"]
    assert (result["phases"][-1]["phase"] == "inspect-infrastructure") is (
        mode not in {"changed_target", "unbound_work"}
    )


def test_existing_output_is_never_reused(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(sys.modules["trial_cleanup_inventory"], "ARTIFACTS", tmp_path / "artifacts")
    output = tmp_path / "artifacts" / "existing"
    output.mkdir(parents=True)
    sentinel = output / "keep.txt"
    sentinel.write_text("keep")
    world = model.Simulation(frozen.path, frozen.pin, PRINCIPAL, "clean")
    with pytest.raises(ValueError):
        rehearsal.run_case(world, output)
    assert sentinel.read_text() == "keep" and not world.journal


@pytest.mark.parametrize("phase", ["quiesce", "delete-data"])
def test_unverified_saved_proof_blocks_the_next_stage(frozen, tmp_path, monkeypatch, phase):
    monkeypatch.setattr(sys.modules["trial_cleanup_inventory"], "ARTIFACTS", tmp_path / "artifacts")
    world = model.Simulation(frozen.path, frozen.pin, PRINCIPAL, "clean")
    original = rehearsal.load_completion

    def tamper(output, op, prepared, reply):
        if op.phase == phase:
            path = output / "transport-proof.json"
            path.write_bytes(path.read_bytes() + b" ")
        return original(output, op, prepared, reply)

    monkeypatch.setattr(rehearsal, "load_completion", tamper)
    with pytest.raises(sys.modules["aws_login_bridge"].BridgeError):
        rehearsal.run_case(world, tmp_path / "artifacts" / "tampered")
    assert not world.removed and len(world.remaining) == 16
    assert not any(r["phase"] == "inspect-infrastructure" for r in world.journal)
    if phase == "quiesce":
        assert len(world.catalog) == 3 and len(world.versions) == 1
