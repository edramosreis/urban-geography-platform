"""Fixed record inventory, strict replies and semantic checks reject substituted completion."""

import hashlib
import json

import pytest
from dbt_host_fixtures import RECEIPT as receipt
from dbt_host_fixtures import RUNNER, prepared_case


@pytest.fixture
def case(tmp_path, monkeypatch):
    return prepared_case(tmp_path, monkeypatch)


def load(case):
    return receipt.load_completion(
        case.evidence, case.recipe, case.pin, case.input, case.writer_pin, case.reply
    )


def test_both_queries_and_worker_completion_bind_to_the_pinned_source(case):
    assert load(case) == {
        "status": "scoped_view_build_completed",
        "models_succeeded": True,
        "result_verified": False,
    }
    assert receipt.verify_reply(RUNNER.canonical(case.reply), case.pin) == case.reply


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"version": 2},
        {"models_succeeded": 1},
        {"models_succeeded": False},
        {"proof_sha256": None},
        {"plan_sha256": "0" * 64},
        {"status": "verified_trial"},
        {"extra": "do-not-emit"},
    ],
)
def test_closed_reply_rejects_other_schemas_and_false_success(case, change):
    with pytest.raises(ValueError):
        receipt.verify_reply(RUNNER.canonical(case.reply | change), case.pin)


def test_failure_reply_can_never_claim_completion_or_a_proof(case):
    failed = receipt.operation_reply(case.pin)
    assert receipt.verify_reply(RUNNER.canonical(failed), case.pin) == failed
    with pytest.raises(ValueError):
        receipt.verify_reply(RUNNER.canonical(failed | {"proof_sha256": "a" * 64}), case.pin)


@pytest.mark.parametrize("name", sorted(receipt.FILES))
def test_changed_saved_record_cannot_match_the_transport_proof(case, name):
    (case.evidence / name).write_bytes(b"{}")
    with pytest.raises(ValueError):
        load(case)


@pytest.mark.parametrize(
    "name,key,value",
    [
        ("dbt/process.json", "owned_group_stopped", False),
        ("dbt/process.json", "credentials_sent", True),
        ("dbt/process.json", "returncode", False),
        ("dbt/process.json", "ipc_requests", 65),
        ("dbt/worker-report.json", "credentials_received", True),
        ("dbt/worker-report.json", "network_attempts", 1),
        ("builder-operation.json", "view_plan_sha256", "0" * 64),
        ("builder-operation.json", "writer_receipt_sha256", "0" * 64),
        ("queries/query-01/receipt.json", "terminal_confirmed", 1),
    ],
)
def test_semantic_substitution_fails_even_with_updated_file_hash(case, name, key, value):
    path = case.evidence / name
    record = json.loads(path.read_bytes())
    record[key] = value
    path.write_bytes(RUNNER.canonical(record))
    proof_path = case.evidence / receipt.PROOF
    proof = json.loads(proof_path.read_bytes())
    proof["files"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
    proof_path.write_bytes(RUNNER.canonical(proof))
    case.reply["proof_sha256"] = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        load(case)


def test_rehashed_receipt_cannot_escalate_completion_to_gold_acceptance(case):
    path = case.evidence / "receipt.json"
    value = json.loads(path.read_bytes())
    value["result_verified"] = True
    path.write_bytes(RUNNER.canonical(value))
    outer = case.evidence / "builder-operation.json"
    value = json.loads(outer.read_bytes())
    value["receipt_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    outer.write_bytes(RUNNER.canonical(value))
    proof_path = case.evidence / receipt.PROOF
    proof = json.loads(proof_path.read_bytes())
    for p in (path, outer):
        proof["files"][p.name] = hashlib.sha256(p.read_bytes()).hexdigest()
    proof_path.write_bytes(RUNNER.canonical(proof))
    case.reply["proof_sha256"] = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        load(case)


@pytest.mark.parametrize("defect", ["extra_name", "oversize", "duplicate", "directory_link"])
def test_proof_paths_and_record_reads_are_closed_and_bounded(case, defect):
    proof_path = case.evidence / receipt.PROOF
    if defect == "extra_name":
        value = json.loads(proof_path.read_bytes())
        value["files"]["../../private.json"] = "a" * 64
        proof_path.write_bytes(RUNNER.canonical(value))
        case.reply["proof_sha256"] = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    elif defect == "oversize":
        (case.evidence / "dbt/process.json").write_bytes(b" " * (64 * 1024 + 1))
    elif defect == "duplicate":
        proof_path.write_bytes(b'{"version":1,"version":1}')
    else:
        directory = case.evidence / "queries"
        saved = case.evidence / "saved-queries"
        directory.rename(saved)
        directory.symlink_to(saved, target_is_directory=True)
    with pytest.raises(ValueError):
        load(case)
