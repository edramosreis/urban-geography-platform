"""Known absence, retained resources and incomplete/denied observations stay distinct."""

import importlib.util
import sys

import pytest
from trial_infrastructure_fixtures import ROOT, SENTINEL, ServiceError, make_case

if "trial_infrastructure_absence" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "trial_infrastructure_absence", ROOT / "scripts/trial_infrastructure_absence.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
observe = sys.modules["trial_infrastructure_absence"]


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    return make_case(frozen, tmp_path, monkeypatch)


def run(case):
    return observe.observe_absence(
        case.clients,
        case.frozen.path,
        case.frozen.pin,
        case.principal,
        case.pin,
        case.output,
        now=lambda: case.clock[0],
    )


def test_all_named_resources_absent_is_a_read_only_point_in_time_observation(case):
    result = run(case)
    assert result["named_infrastructure_absence_observed"] and not result["requires_review"]
    assert len(result["targets"]) == 16 and result["api_calls"] == 16
    assert result["all_targets_confirmed"] and not result["deletion_performed"]
    assert not result["billing_zero_verified"] and not result["terraform_state_verified"]
    assert (
        not result["unmanaged_resources_checked"]
        and not result["orphaned_service_metadata_checked"]
    )
    for client in case.clients.values():
        client.close.assert_not_called()
    assert all(SENTINEL not in p.read_text() for p in case.output.rglob("*.json"))
    for call in case.clients["s3"].list_objects_v2.call_args_list:
        assert call.kwargs["MaxKeys"] == 1 and call.kwargs["ExpectedBucketOwner"] == "123456789012"
    case.clients["logs"].describe_log_groups.assert_called_once_with(
        logGroupIdentifiers=[t["name"] for t in case.plan["targets"] if t["kind"] == "log_group"],
        includeLinkedAccounts=False,
    )


@pytest.mark.parametrize(
    "kind", ["bucket", "database", "job", "role", "policy", "workgroup", "log_group"]
)
def test_present_named_resource_including_an_empty_bucket_is_not_absent(case, kind):
    target = next(t for t in case.plan["targets"] if t["kind"] == kind)
    if kind == "bucket":
        case.clients["s3"].list_objects_v2.side_effect = None
        case.clients["s3"].list_objects_v2.return_value = {
            "Name": target["name"],
            "IsTruncated": False,
            "Contents": [],
        }
    elif kind in {"database", "job"}:
        method = getattr(case.clients["glue"], "get_" + kind)
        method.side_effect = None
        method.return_value = {kind.title(): {"Name": target["name"]}}
    elif kind in {"role", "policy"}:
        method = getattr(case.clients["iam"], "get_" + kind)
        method.side_effect = None
        method.return_value = {
            kind.title(): {kind.title() + "Name": target["name"], "Arn": target["arn"]}
        }
    elif kind == "workgroup":
        case.clients["athena"].list_work_groups.return_value = {
            "WorkGroups": [{"Name": target["name"]}]
        }
    else:
        case.clients["logs"].describe_log_groups.return_value = {
            "logGroups": [{"logGroupName": target["name"]}]
        }
    result = run(case)
    assert not result["named_infrastructure_absence_observed"] and result["requires_review"]
    assert (
        next(r for r in result["targets"] if (r["kind"], r["name"]) == (kind, target["name"]))[
            "state"
        ]
        == "present"
    )


@pytest.mark.parametrize(
    "service,method,code,status",
    [
        ("s3", "list_objects_v2", "AccessDenied", 403),
        ("s3", "list_objects_v2", "404", 404),
        ("s3", "list_objects_v2", "NoSuchBucket", 403),
        ("glue", "get_database", "AccessDeniedException", 400),
        ("glue", "get_job", "EntityNotFoundException", 500),
        ("iam", "get_role", "NoSuchEntity", 403),
        ("iam", "get_policy", "ServiceFailure", 500),
        ("athena", "list_work_groups", "InvalidRequestException", 400),
        ("logs", "describe_log_groups", "ResourceNotFoundException", 400),
    ],
)
def test_access_denial_generic_errors_and_wrong_status_codes_never_prove_absence(
    case, service, method, code, status
):
    getattr(case.clients[service], method).side_effect = ServiceError(code, status)
    result = run(case)
    assert (
        not result["named_infrastructure_absence_observed"] and not result["all_targets_confirmed"]
    )
    assert any(r["state"] == "unconfirmed" for r in result["targets"])
    assert all(SENTINEL not in p.read_text() for p in case.output.rglob("*.json"))


def test_federated_source_missing_is_not_missing_local_database(case):
    case.clients["glue"].get_database.side_effect = ServiceError(
        "EntityNotFoundException", 400, FromFederationSource=True
    )
    assert not run(case)["named_infrastructure_absence_observed"]


@pytest.mark.parametrize(
    "which,defect",
    [
        (service, defect)
        for service in ("athena", "logs")
        for defect in ("missing_list", "too_many", "cycle", "ceiling", "duplicate", "foreign_log")
        if not (service == "athena" and defect == "foreign_log")
    ],
)
def test_incomplete_or_malformed_listing_cannot_confirm_missing_resources(case, which, defect):
    method = (
        case.clients[which].list_work_groups
        if which == "athena"
        else case.clients[which].describe_log_groups
    )
    key, name, token = (
        ("WorkGroups", "Name", "NextToken")
        if which == "athena"
        else ("logGroups", "logGroupName", "nextToken")
    )
    if defect == "missing_list":
        method.return_value = {}
    elif defect == "too_many":
        method.return_value = {key: [{name: str(i)} for i in range(51)]}
    elif defect in {"cycle", "ceiling"}:
        method.side_effect = [
            {key: [], token: "page-2"},
            {key: [], token: "page-2" if defect == "cycle" else "page-3"},
        ]
    elif defect == "duplicate":
        method.return_value = {key: [{name: "other"}, {name: "other"}]}
    else:
        method.return_value = {key: [{name: "/some-other-project"}]}
    result = run(case)
    assert not result["named_infrastructure_absence_observed"] and method.call_count <= 2


@pytest.mark.parametrize("service", ["athena", "logs"])
def test_complete_second_page_can_confirm_absence(case, service):
    method = (
        case.clients[service].list_work_groups
        if service == "athena"
        else case.clients[service].describe_log_groups
    )
    key, token = ("WorkGroups", "NextToken") if service == "athena" else ("logGroups", "nextToken")
    method.side_effect = [{key: [], token: "page-2"}, {key: []}]
    assert run(case)["named_infrastructure_absence_observed"] and method.call_count == 2


@pytest.mark.parametrize("defect", ["pin", "output", "caller", "client_set"])
def test_invalid_scope_output_or_caller_stops_before_resource_reads(case, defect):
    if defect == "pin":
        case.pin = "0" * 64
    elif defect == "output":
        case.output.mkdir(parents=True)
    elif defect == "caller":
        case.clients["sts"].get_caller_identity.return_value["Account"] = "000000000000"
    else:
        case.clients.pop("iam")
    with pytest.raises(ValueError):
        run(case)
    assert all(not c.method_calls for k, c in case.clients.items() if k != "sts")
    assert not (case.output / "inventory.json").exists()


@pytest.mark.parametrize("which", ["time", "calls"])
def test_deadline_and_call_ceiling_stop_without_an_absence_receipt(case, monkeypatch, which):
    if which == "calls":
        monkeypatch.setattr(observe, "MAX_CALLS", 3)
    else:
        original = case.clients["sts"].get_caller_identity.return_value

        def expire(**kw):
            case.clock[0] = 400
            return original

        case.clients["sts"].get_caller_identity.side_effect = expire
    with pytest.raises(TimeoutError):
        run(case)
    assert (case.output / "failure.json").exists() and not (case.output / "inventory.json").exists()
