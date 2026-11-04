"""Exact remote cleanup preparation with retained local data and no mutation."""

import copy
import json
from dataclasses import replace

import pytest
from trial_data_cleanup_fixtures import SENTINEL, make_cleanup_case, plans
from trial_quiescence_fixtures import save


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    return make_cleanup_case(frozen, tmp_path, monkeypatch)


def prepare(case, **kwargs):
    return plans.prepare_cleanup(
        case.glue,
        case.athena,
        case.s3,
        case.frozen.path,
        case.frozen.pin,
        case.evidence,
        case.output,
        coordinator_paused=kwargs.get("paused", True),
    )


def no_changes(case):
    case.glue.delete_table.assert_not_called()
    case.s3.delete_object.assert_not_called()
    case.s3.abort_multipart_upload.assert_not_called()


def test_plan_names_exact_catalog_generations_versions_objects_and_uploads(case):
    plan = prepare(case)
    assert [a["kind"] for a in plan["actions"]] == [
        "table",
        "table",
        "table",
        "multipart_upload",
        "version",
        "delete_marker",
        "object",
    ]
    assert [a["name"] for a in plan["actions"][:3]] == list(plans.TABLE_ORDER)
    assert plan["limits"]["max_actions"] == 64 and plan["limits"]["max_calls"] == 515
    assert not plan["execution_authorized"] and plan["coordinator_paused_assertion"]
    assert SENTINEL not in json.dumps(plan) and "ViewOriginalText" not in json.dumps(plan)
    assert (case.output / "preparation.json").exists()
    no_changes(case)


@pytest.mark.parametrize("paused", [False, 1, "yes"])
def test_pause_must_be_explicit_and_boolean_before_any_cloud_read(case, paused):
    with pytest.raises(ValueError, match="coordinator"):
        prepare(case, paused=paused)
    assert not case.glue.method_calls and not case.s3.method_calls and not case.athena.method_calls


@pytest.mark.parametrize("which", ["receipt", "completion", "inventory"])
def test_changed_evidence_never_reaches_live_inventory(case, which):
    record = {
        "receipt": case.evidence.quiescence_receipt,
        "completion": case.evidence.quiescence_completion,
        "inventory": case.evidence.inventory_plan,
    }[which]
    record.path.write_bytes(b"changed")
    with pytest.raises(ValueError):
        prepare(case)
    assert not case.glue.method_calls and not case.s3.method_calls


def test_unconfirmed_quiescence_is_not_a_cleanup_permission(case):
    record = case.evidence.quiescence_receipt
    content = json.loads(record.path.read_bytes())
    content["quiescence_observed"] = False
    case.evidence = replace(case.evidence, quiescence_receipt=save(record.path, content))
    with pytest.raises(ValueError, match="quiescence evidence"):
        prepare(case)
    no_changes(case)


@pytest.mark.parametrize(
    "defect",
    [
        "job_active",
        "query_active",
        "unbound_query",
        "unknown_key",
        "fact_location",
        "generation",
        "wildcard_etag",
        "too_many",
    ],
)
def test_live_drift_or_unbounded_selection_never_produces_an_executable_plan(case, defect):
    if defect == "job_active":
        case.run["JobRunState"] = "RUNNING"
    elif defect == "query_active":
        case.query_rows["query-0"]["Status"]["State"] = "RUNNING"
    elif defect == "unbound_query":
        case.query_rows["unbound"] = {**case.query_rows["query-0"], "QueryExecutionId": "unbound"}
    elif defect == "unknown_key":
        case.versions[0]["Key"] = "private/keep.parquet"
    elif defect == "fact_location":
        case.catalog["building_proximity"]["StorageDescriptor"]["Location"] = "s3://other/data"
    elif defect == "generation":
        case.catalog["grid_proximity"].pop("CreateTime")
    elif defect == "wildcard_etag":
        case.objects[0]["ETag"] = "*"
    else:
        case.versions[:] = [{**case.versions[0], "VersionId": str(i)} for i in range(65)]
    with pytest.raises(ValueError):
        prepare(case)
    assert not (case.output / "plan.json").exists()
    no_changes(case)


def test_normal_execution_needs_same_selection_and_explicit_resume_accepts_only_remaining_subset(
    case,
):
    plan = prepare(case)
    subset = copy.deepcopy(plan["actions"][1:])
    with pytest.raises(ValueError):
        plans.remaining(plan, subset, False)
    assert plans.remaining(plan, subset, True) == subset
    changed = copy.deepcopy(subset)
    changed[-1]["etag"] = '"replacement"'
    with pytest.raises(ValueError):
        plans.remaining(plan, changed, True)
    added = copy.deepcopy(subset)
    added.append({**plan["actions"][-3], "version_id": "new-version"})
    with pytest.raises(ValueError):
        plans.remaining(plan, added, True)
    no_changes(case)


def test_existing_output_is_rejected_before_cloud_reads(case):
    case.output.mkdir(parents=True)
    with pytest.raises(ValueError):
        prepare(case)
    assert not case.glue.method_calls and not case.s3.method_calls
