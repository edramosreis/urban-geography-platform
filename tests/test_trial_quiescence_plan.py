"""First-trial stop targets come only from pinned matching controller records."""

import json

import pytest
from trial_quiescence_fixtures import make_evidence, plans, save


@pytest.fixture
def case(frozen, tmp_path):
    return make_evidence(frozen, tmp_path / "saved")


def build(case):
    return plans.quiescence_plan(case.frozen.path, case.frozen.pin, case.job_receipt, case.evidence)


def test_scope_binds_one_job_and_the_three_exact_query_recipes_without_saving_sql(case):
    result = build(case)
    assert result["job_run"] == {"id": "jr_owned", "receipt_sha256": case.job_receipt.sha256}
    assert [row["kind"] for row in result["queries"]] == [
        "fact_select",
        "grid_proximity",
        "release_proximity",
    ]
    assert result["disable_request"] == {"WorkGroup": case.group["Name"], "State": "DISABLED"}
    assert result["snapshot_id"] == 123 and not result["deletion_authorized"]
    assert result["limits"]["max_total_calls"] == 107
    assert "QueryString" not in json.dumps(result)
    assert len(plans.plan_sha256(result)) == 64
    case.glue.get_job.assert_not_called()


def test_no_receipts_binds_no_stop_targets_and_never_discovers_them(case):
    result = plans.quiescence_plan(case.frozen.path, case.frozen.pin, None, ())
    assert result["job_run"] is None and result["queries"] == []
    assert not result["execution_authorized"] and not result["deletion_authorized"]


@pytest.mark.parametrize(
    "field", ["job_name", "release_id", "release_sha256", "run_id", "terminal_confirmed"]
)
def test_bad_job_receipt_cannot_create_a_stop_scope(case, field):
    value = json.loads(case.job_receipt.path.read_bytes())
    value[field] = "wrong" if field != "run_id" else "bad\nidentity"
    case.job_receipt = save(case.job_receipt.path, value)
    with pytest.raises(ValueError):
        build(case)


@pytest.mark.parametrize(
    "defect",
    [
        "intent_pin",
        "receipt_pin",
        "receipt_id",
        "intent_binding",
        "receipt_sql",
        "token",
        "context",
        "source",
        "parent_pin",
        "parent_sql",
        "view_sql",
        "duplicate_kind",
        "duplicate_id",
        "too_many",
        "snapshot",
    ],
)
def test_bad_query_records_or_ambiguous_targets_never_create_a_stop_scope(case, defect):
    rows = list(case.evidence)
    index = 1 if defect in {"parent_pin", "parent_sql", "view_sql"} else 0
    evidence = rows[index]
    intent = json.loads(evidence.intent.path.read_bytes())
    receipt = json.loads(evidence.receipt.path.read_bytes())
    if defect == "intent_pin":
        rows[0] = plans.QueryEvidence(
            plans.Record(evidence.intent.path, "0" * 64), evidence.receipt
        )
    elif defect == "receipt_pin":
        rows[0] = plans.QueryEvidence(
            evidence.intent, plans.Record(evidence.receipt.path, "0" * 64)
        )
    elif defect == "too_many":
        rows.append(rows[0])
    elif defect == "duplicate_kind":
        rows = [rows[0], rows[0]]
    elif defect == "duplicate_id":
        second = json.loads(rows[1].receipt.path.read_bytes())
        second["query_id"] = "query-0"
        rows[1] = plans.QueryEvidence(
            rows[1].intent, save(rows[1].receipt.path, second), rows[1].parent
        )
    elif defect == "parent_pin":
        rows[index] = plans.QueryEvidence(
            evidence.intent, evidence.receipt, plans.Record(evidence.parent.path, "0" * 64)
        )
    elif defect == "parent_sql":
        parent = json.loads(evidence.parent.path.read_bytes())
        parent["queries"][0]["sql"] = "DROP TABLE unrelated"
        rows[index] = plans.QueryEvidence(
            evidence.intent, evidence.receipt, save(evidence.parent.path, parent)
        )
    else:
        if defect == "receipt_id":
            receipt["query_id"] = "bad identity"
        elif defect == "intent_binding":
            receipt["intent_sha256"] = "0" * 64
        elif defect == "receipt_sql":
            receipt["query_sha256"] = "0" * 64
        elif defect == "token":
            intent["request"]["ClientRequestToken"] = "invalid"
        elif defect == "context":
            intent["request"]["QueryExecutionContext"]["Database"] = "other"
        elif defect == "source":
            intent["plan"]["binding"]["source_run_sha256"] = "0" * 64
        elif defect == "snapshot":
            intent["plan"]["binding"]["snapshot_id"] = 124
        else:
            intent["request"]["QueryString"] = "DROP TABLE unrelated"
        rows[index] = plans.QueryEvidence(
            save(evidence.intent.path, intent),
            save(evidence.receipt.path, receipt),
            evidence.parent,
        )
    case.evidence = tuple(rows)
    with pytest.raises(ValueError):
        build(case)


@pytest.mark.parametrize("what", ["manifest", "job", "query", "parent"])
def test_recheck_rejects_changed_evidence_bytes(case, what):
    path = {
        "manifest": case.frozen.path / "release.json",
        "job": case.job_receipt.path,
        "query": case.evidence[0].intent.path,
        "parent": case.evidence[1].parent.path,
    }[what]
    path.write_bytes(b"changed")
    with pytest.raises(ValueError):
        plans.recheck_evidence(case.frozen.path, case.frozen.pin, case.job_receipt, case.evidence)


@pytest.mark.parametrize("defect", ["source", "snapshot"])
def test_other_valid_query_recipe_cannot_be_bound_to_this_trial(case, defect):
    import hashlib
    import sys

    evidence = case.evidence[0]
    source = sys.modules["athena_read_plan"].read_plan(
        **case.frozen.manifest["identity"]["target"],
        snapshot_id=124 if defect == "snapshot" else 123,
        source_run_sha256="0" * 64
        if defect == "source"
        else case.frozen.manifest["identity"]["summary_sha256"],
    )
    request = source["request_template"] | {"ClientRequestToken": "a" * 64}
    intent = save(evidence.intent.path, {"contract_version": 1, "plan": source, "request": request})
    receipt = save(
        evidence.receipt.path,
        {
            "query_id": "query-0",
            "intent_sha256": intent.sha256,
            "query_sha256": hashlib.sha256(request["QueryString"].encode()).hexdigest(),
            "terminal_confirmed": False,
        },
    )
    case.evidence = (plans.QueryEvidence(intent, receipt), *case.evidence[1:])
    with pytest.raises(ValueError, match="source differs|writer snapshot"):
        build(case)
