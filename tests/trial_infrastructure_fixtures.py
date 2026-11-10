"""Synthetic direct and listed resources for post-teardown inspection; never AWS."""

import importlib.util
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from trial_cleanup_fixtures import PRINCIPAL, ROOT, inventory

if "trial_infrastructure_plan" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "trial_infrastructure_plan", ROOT / "scripts/trial_infrastructure_plan.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
plans = sys.modules["trial_infrastructure_plan"]
SENTINEL = "sentinel-private-service-payload"


class ServiceError(RuntimeError):
    def __init__(self, code, status, **extra):
        super().__init__(SENTINEL)
        self.response = {
            "Error": {"Code": code, "Message": SENTINEL},
            "ResponseMetadata": {"HTTPStatusCode": status},
            **extra,
        }


def make_case(frozen, folder, monkeypatch):
    monkeypatch.setattr(inventory, "ARTIFACTS", folder / "artifacts")
    plan = plans.absence_plan(frozen.path, frozen.pin, PRINCIPAL)
    owner = plan["target"]["account_id"]
    clients = {
        service: Mock(spec=[*methods, "close"])
        for service, methods in {
            "sts": ["get_caller_identity"],
            "s3": ["list_objects_v2"],
            "glue": ["get_database", "get_job"],
            "athena": ["list_work_groups"],
            "iam": ["get_role", "get_policy"],
            "logs": ["describe_log_groups"],
        }.items()
    }
    clients["sts"].get_caller_identity.return_value = {
        "Account": owner,
        "Arn": f"arn:aws:sts::{owner}:assumed-role/login/browser",
        "UserId": "AROA" + "A" * 17 + ":browser",
    }
    clients["s3"].list_objects_v2.side_effect = ServiceError("NoSuchBucket", 404)
    clients["glue"].get_database.side_effect = ServiceError("EntityNotFoundException", 400)
    clients["glue"].get_job.side_effect = ServiceError("EntityNotFoundException", 400)
    clients["iam"].get_role.side_effect = ServiceError("NoSuchEntity", 404)
    clients["iam"].get_policy.side_effect = ServiceError("NoSuchEntity", 404)
    clients["athena"].list_work_groups.return_value = {
        "WorkGroups": [{"Name": "primary", "Description": SENTINEL}]
    }
    clients["logs"].describe_log_groups.return_value = {"logGroups": []}
    return SimpleNamespace(
        frozen=frozen,
        plan=plan,
        pin=plans.plan_sha256(plan),
        principal=PRINCIPAL,
        clients=clients,
        output=folder / "artifacts/absence",
        clock=[0.0],
    )
