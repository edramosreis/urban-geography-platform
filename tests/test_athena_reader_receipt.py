"""Closed transport replies and cross-file evidence reject substitutions and false claims."""

import hashlib
import json

import pytest
from athena_host_fixtures import CONTROL, prepared_case
from athena_host_fixtures import RECEIPT as receipt


@pytest.fixture
def case(tmp_path, monkeypatch):
    return prepared_case(tmp_path, monkeypatch)


def load(case):
    return receipt.load_completion(
        case.evidence, case.case.plan, case.pin, case.input, case.writer_pin, case.reply
    )


def test_completed_proof_binds_saved_gold_result_to_the_pinned_plan_and_input(case):
    assert load(case) == {
        "status": "verified_trial",
        "terminal_confirmed": True,
        "result_verified": True,
    }
    assert receipt.verify_reply(CONTROL.canonical(case.reply), case.pin) == case.reply


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"version": 2},
        {"terminal_confirmed": 1},
        {"terminal_confirmed": False},
        {"proof_sha256": None},
        {"plan_sha256": "0" * 64},
        {"status": "unknown"},
        {"extra": "sensitive"},
    ],
)
def test_closed_reply_rejects_invalid_schema_and_completion_claims(case, change):
    with pytest.raises(ValueError):
        receipt.verify_reply(CONTROL.canonical(case.reply | change), case.pin)


def test_failed_reply_never_claims_terminal_state_or_proof(case):
    failed = receipt.operation_reply(case.pin)
    assert receipt.verify_reply(CONTROL.canonical(failed), case.pin) == failed
    with pytest.raises(ValueError):
        receipt.verify_reply(CONTROL.canonical(failed | {"proof_sha256": "a" * 64}), case.pin)


@pytest.mark.parametrize("name", sorted(receipt.BASE_FILES | {"result-verification.json"}))
def test_any_changed_saved_record_is_rejected(case, name):
    (case.evidence / name).write_bytes(b"{}")
    with pytest.raises(ValueError):
        load(case)


@pytest.mark.parametrize(
    "defect", ["query", "snapshot", "row", "caller", "numeric_flag", "writer", "object", "scans"]
)
def test_semantic_substitution_fails_even_with_recomputed_proof_pins(case, defect):
    name = {
        "query": "query.json",
        "snapshot": "receipt.json",
        "row": "result-verification.json",
        "caller": "source-verification.json",
        "numeric_flag": "source-verification.json",
        "writer": "source-verification.json",
        "object": "result-verification.json",
        "scans": "result-verification.json",
    }[defect]
    path = case.evidence / name
    value = json.loads(path.read_bytes())
    if defect == "query":
        value["query_id"] = "other-query"
    elif defect == "snapshot":
        value["binding"]["snapshot_id"] = 456
    elif defect == "row":
        value["row"]["mapped_buildings"] += 1
    elif defect == "caller":
        value["caller"]["Arn"] = value["caller"]["Arn"].replace(
            "-athena-reader/", "-glue-operator/"
        )
    elif defect == "numeric_flag":
        value["catalog_stable"] = 1
    elif defect == "writer":
        value["source_readback"]["writer_receipt_sha256"] = "b" * 64
    elif defect == "object":
        value["result_object"]["key"] = "queries/another.csv"
    else:
        value["data_scanned_bytes"] = True
    path.write_bytes(CONTROL.canonical(value))
    proof_path = case.evidence / receipt.PROOF
    proof = json.loads(proof_path.read_bytes())
    proof["files"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
    proof_path.write_bytes(CONTROL.canonical(proof))
    case.reply["proof_sha256"] = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        load(case)


def test_proof_has_no_path_or_writer_binding_escape(case):
    path = case.evidence / receipt.PROOF
    proof = json.loads(path.read_bytes())
    proof["files"]["../escape.json"] = "a" * 64
    path.write_bytes(CONTROL.canonical(proof))
    case.reply["proof_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        load(case)


def test_regular_file_requirement_and_record_size_are_enforced(case, tmp_path):
    target = tmp_path / "target.json"
    target.write_text("{}")
    path = case.evidence / "query.json"
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(ValueError):
        load(case)
    target.write_bytes(b"x" * (receipt.MAX_RECORD_BYTES + 1))
    with pytest.raises(ValueError):
        receipt.read_record(target)


def test_known_failed_query_has_a_bound_receipt_without_result_verification(tmp_path, monkeypatch):
    case = prepared_case(tmp_path, monkeypatch, failed=True)
    assert load(case) == {
        "status": "query_not_verified",
        "terminal_confirmed": True,
        "result_verified": False,
    }


@pytest.mark.parametrize("delta,accepted", [(1e-14, True), (1e-5, False)])
def test_host_share_check_uses_the_existing_csv_tolerance(case, delta, accepted):
    proof_path = case.evidence / receipt.PROOF
    proof = json.loads(proof_path.read_bytes())
    for name in ("receipt.json", "result-verification.json"):
        path = case.evidence / name
        record = json.loads(path.read_bytes())
        record["row"]["share_within"] += delta
        path.write_bytes(CONTROL.canonical(record))
        proof["files"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
    proof_path.write_bytes(CONTROL.canonical(proof))
    case.reply["proof_sha256"] = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    if accepted:
        assert load(case)["result_verified"]
    else:
        with pytest.raises(ValueError):
            load(case)
