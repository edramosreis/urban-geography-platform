"""Explicit phase inputs built from the existing synthetic quiet-service fixtures."""

import hashlib
import importlib.util
import sys
from dataclasses import replace

from trial_cleanup_fixtures import PRINCIPAL
from trial_data_cleanup_fixtures import ROOT, make_cleanup_case, plans

for name in ("trial_quiescence", "trial_cleanup_command"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
command = sys.modules["trial_cleanup_command"]


def make_command_case(frozen, folder, monkeypatch, phase="prepare-data"):
    case = make_cleanup_case(frozen, folder, monkeypatch)
    evidence = case.evidence
    case.operation = command.Operation(
        frozen.path,
        frozen.pin,
        evidence.inventory_plan,
        PRINCIPAL,
        phase,
        evidence.job,
        evidence.queries,
        evidence.quiescence_receipt,
        evidence.quiescence_completion,
        paused=True,
    )
    if phase == "inventory":
        case.operation = replace(
            case.operation,
            job=None,
            queries=(),
            quiet_receipt=None,
            quiet_completion=None,
            paused=False,
        )
    elif phase == "quiesce":
        case.operation = replace(case.operation, quiet_receipt=None, quiet_completion=None)
    elif phase == "delete-data":
        plans.prepare_cleanup(
            case.glue,
            case.athena,
            case.s3,
            frozen.path,
            frozen.pin,
            evidence,
            case.output,
            coordinator_paused=True,
        )
        path = case.output / "plan.json"
        case.operation = replace(
            case.operation,
            approved=plans.Record(path, hashlib.sha256(path.read_bytes()).hexdigest()),
        )
    case.output = folder / "artifacts/command-operation"
    for client in (case.glue, case.athena, case.s3):
        client.reset_mock()
    return case
