"""Caller/source gates bind the model executor and its controlled submissions."""

import copy
import hashlib
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt_runner_fixtures import CALLER, RUNNER, runner_case


@pytest.fixture
def case(tmp_path, monkeypatch):
    return runner_case(tmp_path, monkeypatch)


def run(case):
    return RUNNER.run_view_build(case.clients, case.project, case.output, **case.options)


def test_source_readback_precedes_both_queries_and_final_receipt_remains_limited(case):
    report = run(case)
    assert report["status"] == "source_checked_view_build"
    assert report["caller_identity_verified"] and report["source_readback_verified"]
    assert report["source_catalog_stable"] and report["controlled_view_queries_completed"]
    assert not report["view_catalog_verified"] and not report["result_verified"]
    assert not report["query_control"]["source_readback_verified"]
    case.reader.assert_called_once()
    assert case.clients["sts"].get_caller_identity.call_count == 5
    assert case.clients["glue"].get_table.call_count == 5
    assert len(case.requests) == 2
    assert (case.output / "submission-gate-01.json").exists()
    assert (case.output / "submission-gate-02.json").exists()
    assert (case.output / "final-gate.json").exists()
    for name, key in (
        ("source-verification.json", "source_verification_sha256"),
        ("final-gate.json", "final_gate_sha256"),
    ):
        assert hashlib.sha256((case.output / name).read_bytes()).hexdigest() == report[key]
    case.clients["athena"].stop_query_execution.assert_not_called()


@pytest.mark.parametrize("kind", ["caller", "reader", "catalog"])
def test_initial_source_failure_never_enters_the_executor(case, kind):
    if kind == "caller":
        case.clients["sts"].get_caller_identity.return_value = CALLER | {"UserId": "different"}
    elif kind == "reader":
        case.reader.return_value["binding"] = case.reader.return_value["binding"] | {
            "snapshot_id": 456
        }
    else:
        table = case.clients["glue"].get_table.return_value
        changed = copy.deepcopy(table)
        changed["Table"]["VersionId"] = "2"
        case.clients["glue"].get_table.side_effect = [table, changed]
    with pytest.raises(ValueError):
        run(case)
    case.executor.assert_not_called()
    case.clients["athena"].start_query_execution.assert_not_called()
    failure = json.loads((case.output / "failure.json").read_text())
    assert failure["stage"] == "source_preflight" and not failure["initial_source_verified"]
    assert (
        hashlib.sha256((case.output / "intent.json").read_bytes()).hexdigest()
        == failure["run_intent_sha256"]
    )


@pytest.mark.parametrize("kind", ["caller", "catalog"])
@pytest.mark.parametrize("moment,successful_queries", [("first", 0), ("second", 1), ("final", 2)])
def test_current_source_or_caller_drift_stops_at_the_corresponding_gate(
    case, kind, moment, successful_queries
):
    initial = 2 + successful_queries
    if kind == "caller":
        case.clients["sts"].get_caller_identity.side_effect = [dict(CALLER)] * initial + [
            CALLER | {"UserId": "changed-session"}
        ]
    else:
        table = case.clients["glue"].get_table.return_value
        changed = copy.deepcopy(table)
        changed["Table"]["VersionId"] = "2"
        case.clients["glue"].get_table.side_effect = [
            copy.deepcopy(table) for _ in range(initial)
        ] + [changed]
    with pytest.raises(ValueError):
        run(case)
    assert len(case.requests) == successful_queries
    assert not (case.output / "receipt.json").exists()
    failure = json.loads((case.output / "failure.json").read_text())
    assert failure["initial_source_verified"]
    assert failure["stage"] == ("final_source_gate" if moment == "final" else "view_execution")
    case.clients["athena"].stop_query_execution.assert_not_called()


@pytest.mark.parametrize("what", ["writer", "proof", "intent", "project"])
def test_changed_local_proof_or_input_blocks_a_following_query(case, what):
    original = case.clients["athena"].start_query_execution.side_effect

    def changed(**request):
        value = original(**request)
        paths = {
            "writer": case.options["writer_receipt"],
            "proof": case.output / "source-verification.json",
            "intent": case.output / "intent.json",
            "project": case.project / "macros/require_view.sql",
        }
        paths[what].write_text("changed")
        return value

    case.clients["athena"].start_query_execution.side_effect = changed
    with pytest.raises(ValueError):
        run(case)
    assert len(case.requests) == 1
    assert not (case.output / "queries/query-02").exists()
    assert not (case.output / "receipt.json").exists()


def test_executor_cannot_claim_success_without_both_controlled_queries(case):
    case.executor.side_effect = None
    case.executor.return_value = {
        "success": True,
        "model_statuses": {name: "success" for name in ("grid_proximity", "release_proximity")},
    }
    with pytest.raises(ValueError, match="did not complete"):
        run(case)
    case.clients["athena"].start_query_execution.assert_not_called()
    assert not (case.output / "receipt.json").exists()


@pytest.mark.parametrize("failure", [None, "reader", "factory"])
def test_owned_resources_share_and_clear_frozen_credentials_on_every_exit(
    case, monkeypatch, failure
):
    environment = {
        "AWS_ACCESS_KEY_ID": "offline-access-private",
        "AWS_SECRET_ACCESS_KEY": "offline-secret-private",
    }
    closed = []

    @contextmanager
    def resources(**kwargs):
        try:
            yield SimpleNamespace(clients=case.clients, reader_environment=environment)
        finally:
            environment.clear()
            closed.append(True)

    monkeypatch.setattr(RUNNER, "query_resources", resources)

    def factory(path, credentials, target):
        assert credentials is environment and target == case.options["target"]
        assert path == case.output / "reader"
        if failure == "factory":
            raise RuntimeError("synthetic factory failure")
        return case.reader

    if failure == "reader":
        case.reader.side_effect = RuntimeError("synthetic reader failure")
    options = {key: value for key, value in case.options.items() if key != "snapshot_reader"}
    if failure:
        with pytest.raises(RuntimeError):
            RUNNER.run_owned_views(case.project, case.output, **options, reader_factory=factory)
    else:
        assert RUNNER.run_owned_views(case.project, case.output, **options, reader_factory=factory)[
            "source_readback_verified"
        ]
    assert closed == [True] and environment == {}
    contents = (
        b"".join(p.read_bytes() for p in case.output.rglob("*") if p.is_file())
        if case.output.exists()
        else b""
    )
    assert b"offline-access-private" not in contents and b"offline-secret-private" not in contents


def test_invalid_local_receipt_fails_before_credentials_or_outputs(case, monkeypatch):
    resources = Mock(side_effect=AssertionError("Do not discover credentials"))
    monkeypatch.setattr(RUNNER, "query_resources", resources)
    options = {key: value for key, value in case.options.items() if key != "snapshot_reader"}
    options["writer_receipt_sha256"] = "b" * 64
    with pytest.raises(ValueError):
        RUNNER.run_owned_views(case.project, case.output, **options)
    resources.assert_not_called()
    assert not case.output.exists()


def test_stale_reader_runtime_fails_before_credentials(case, monkeypatch):
    resources = Mock(side_effect=AssertionError("Do not discover credentials"))
    monkeypatch.setattr(RUNNER, "query_resources", resources)
    monkeypatch.setattr(RUNNER, "verified_jars", Mock(side_effect=ValueError("Stale reader JAR")))
    options = {key: value for key, value in case.options.items() if key != "snapshot_reader"}
    with pytest.raises(ValueError, match="Stale reader JAR"):
        RUNNER.run_owned_views(case.project, case.output, **options)
    resources.assert_not_called()
    assert not case.output.exists()
