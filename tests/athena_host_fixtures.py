"""Saved offline workflow evidence and a fake project for host/receiver tests."""

import hashlib
import importlib.util
import sys
from types import SimpleNamespace

from athena_control_fixtures import CONTROL
from athena_runner_fixtures import ROOT, RUNNER, trial_case

for name in (
    "aws_login_bridge",
    "aws_role_session",
    "athena_reader_session",
    "athena_reader_operation",
    "athena_reader_receipt",
    "run_athena_reader",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
RECEIPT = sys.modules["athena_reader_receipt"]
RECEIVER = sys.modules["run_athena_reader"]
OPERATION = sys.modules["athena_reader_operation"]
BRIDGE = sys.modules["aws_login_bridge"]
PRINCIPAL = "arn:aws:iam::123456789012:role/managed/project/login"
CALLER = {
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/urban-geo-test-athena-reader/athena-reader-"
    + "a" * 32,
    "UserId": "AROA" + "B" * 17 + ":athena-reader-" + "a" * 32,
}


def prepared_case(tmp_path, monkeypatch, failed=False):
    fixture = tmp_path / "artifacts/fixtures"
    fixture.mkdir(parents=True)
    case = trial_case(fixture, monkeypatch)
    monkeypatch.setattr(CONTROL, "ARTIFACTS", tmp_path / "artifacts")
    monkeypatch.setattr(OPERATION, "ARTIFACTS", tmp_path / "artifacts")
    monkeypatch.setattr(BRIDGE, "ROOT", tmp_path)
    case.clients["sts"].get_caller_identity.return_value = dict(CALLER)
    case.options.update(caller_arn=CALLER["Arn"], caller_user_id=CALLER["UserId"])
    if failed:
        case.clients["athena"].get_query_execution.return_value["QueryExecution"]["Status"][
            "State"
        ] = "FAILED"
    result = RUNNER.run_trial(case.clients, case.attempt, **case.options)
    plans = fixture / "plans"
    plans.mkdir()
    plan = plans / "plan.json"
    plan.write_bytes(CONTROL.canonical(case.plan))
    pin = hashlib.sha256(plan.read_bytes()).hexdigest()
    writer = case.options["writer_receipt"]
    writer_pin = case.options["writer_receipt_sha256"]
    proof = RECEIPT.save_completion(
        case.attempt.output, case.plan, pin, case.options["input_path"], writer_pin, result
    )
    reply = RECEIPT.operation_reply(pin, result, proof)
    sdk = tmp_path / "artifacts/sdk"
    sdk.mkdir()
    bundle = tmp_path / ".private/tools/glue-runtime" / "iceberg-aws-bundle-1.10.2.jar"
    bundle.parent.mkdir(parents=True)
    bundle.write_bytes(b"synthetic bundle")
    return SimpleNamespace(
        case=case,
        result=result,
        plan=plan,
        pin=pin,
        input=case.options["input_path"],
        writer=writer,
        writer_pin=writer_pin,
        reply=reply,
        evidence=case.attempt.output,
        sdk=sdk,
        bundle=bundle,
        output=tmp_path / "artifacts/launch",
    )
