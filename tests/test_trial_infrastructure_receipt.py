"""Saved infrastructure evidence cannot turn retained or unconfirmed targets into absence."""

import copy
import importlib.util
import sys

import pytest
from trial_cleanup_fixtures import packet
from trial_infrastructure_fixtures import ROOT, ServiceError, make_case

for name in (
    "trial_infrastructure_absence",
    "trial_infrastructure_operation",
    "trial_infrastructure_receipt",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
operation = sys.modules["trial_infrastructure_operation"]
receipt = sys.modules["trial_infrastructure_receipt"]


@pytest.fixture(params=["absent", "present", "unconfirmed"])
def case(frozen, tmp_path, monkeypatch, request):
    case = make_case(frozen, tmp_path, monkeypatch)
    case.mode = request.param
    if case.mode == "present":
        target = next(t for t in case.plan["targets"] if t["kind"] == "bucket")

        def present(**kw):
            if kw["Bucket"] == target["name"]:
                return {"Name": target["name"], "Contents": []}
            raise ServiceError("NoSuchBucket", 404)

        case.clients["s3"].list_objects_v2.side_effect = present
    elif case.mode == "unconfirmed":
        case.clients["iam"].get_policy.side_effect = ServiceError("AccessDenied", 403)
    monkeypatch.setattr(operation, "create_client", lambda snapshot, service: case.clients[service])
    operation.run_absence(frozen.path, frozen.pin, case.principal, case.pin, packet(), case.output)
    case.records = {}
    case.hashes = {}
    for name in receipt.record_names(case.plan):
        value, pin = operation.read_record(case.output / name)
        case.records[name] = value
        case.hashes[name] = pin
    return case


def validate(case):
    return receipt.validate_completion(case.plan, case.records, case.hashes)


def test_honest_completed_observation_can_still_require_review(case):
    value = validate(case)
    assert value["named_infrastructure_absence_observed"] == (case.mode == "absent")
    assert value["requires_review"] == (case.mode != "absent")
    assert value["data_cleanup_observed"] is False and value["prepared_plan_sha256"] is None


@pytest.mark.parametrize(
    "defect",
    [
        "target_scope",
        "target_record",
        "source_identity",
        "billing_claim",
        "counter",
        "time",
        "report_pin",
        "extra_path",
    ],
)
def test_changed_scope_records_or_claims_are_refused(case, defect):
    report = case.records["inspection/inventory.json"]
    if defect == "target_scope":
        report["targets"][0]["name"] = "other-project"
    elif defect == "target_record":
        case.records["inspection/target-00.json"]["name"] = "tampered-target"
    elif defect == "source_identity":
        report["source_identity_verified"] = 1
    elif defect == "billing_claim":
        report["billing_zero_verified"] = True
    elif defect == "counter":
        report["api_calls"] = 19
    elif defect == "time":
        report["completed_at_utc"] = "2026-11-10T00:00:00"
    elif defect == "report_pin":
        case.hashes["inspection/inventory.json"] = "0" * 64
    else:
        case.records["../../unexpected.json"] = {}
    with pytest.raises((ValueError, KeyError)):
        validate(case)


def test_top_level_absence_cannot_override_present_or_unconfirmed_targets(case):
    if case.mode == "absent":
        row = case.records["inspection/inventory.json"]["targets"][0]
        row.update(state="present", reason="matching_identity")
        case.records["inspection/target-00.json"] = copy.deepcopy(row)
    report = case.records["inspection/inventory.json"]
    report.update(
        named_infrastructure_absence_observed=True,
        requires_review=False,
        status="named_infrastructure_absence_observed",
        all_targets_confirmed=True,
    )
    case.records["session-complete.json"].update(
        named_infrastructure_absence_observed=True, requires_review=False
    )
    with pytest.raises(ValueError):
        validate(case)
