"""Saved synthetic two-view evidence for launcher tests; no SDK, dbt or Spark startup."""

import hashlib
import importlib.util
import json
import sys
from types import SimpleNamespace

from dbt_runner_fixtures import ROOT, RUNNER, runner_case

for name in (
    "aws_login_bridge",
    "aws_role_session",
    "athena_reader_session",
    "dbt_builder_session",
    "dbt_rpc",
    "dbt_metadata",
    "dbt_adapter_binding",
    "dbt_model_worker",
    "dbt_model_executor",
    "dbt_view_capture",
    "dbt_builder_operation",
    "dbt_builder_receipt",
    "run_dbt_builder",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
BRIDGE = sys.modules["aws_login_bridge"]
OPERATION = sys.modules["dbt_builder_operation"]
RECEIPT = sys.modules["dbt_builder_receipt"]
RECEIVER = sys.modules["run_dbt_builder"]
PRINCIPAL = "arn:aws:iam::123456789012:role/managed/project/login"
CALLER = {
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/urban-geo-test-dbt-view-builder/dbt-builder-"
    + "a" * 32,
    "UserId": "AROA" + "B" * 17 + ":dbt-builder-" + "a" * 32,
}


def prepared_case(tmp_path, monkeypatch, capture=False):
    fixture = tmp_path / "artifacts/fixtures"
    fixture.mkdir(parents=True)
    case = runner_case(fixture, monkeypatch)
    monkeypatch.setattr(OPERATION, "ARTIFACTS", tmp_path / "artifacts")
    monkeypatch.setattr(BRIDGE, "ROOT", tmp_path)
    case.clients["sts"].get_caller_identity.return_value = dict(CALLER)
    case.options.update(caller_arn=CALLER["Arn"], caller_user_id=CALLER["UserId"])
    result = RUNNER.run_view_build(case.clients, case.project, case.output, **case.options)
    recipe = RUNNER.view_build_plan(case.project, **case.options["target"])
    plan = fixture / "plans/views.json"
    plan.parent.mkdir()
    plan.write_bytes(RUNNER.canonical(recipe))
    pin = hashlib.sha256(plan.read_bytes()).hexdigest()
    request = {
        "version": 1,
        "project": str(case.project),
        "output": str(case.output / "dbt"),
        "view_plan": recipe,
        "controller_intent_sha256": pin,
    }
    request_pin = RUNNER.save_record(case.output / "dbt/worker-request.json", request)
    outcome = {
        "success": True,
        "model_statuses": {"grid_proximity": "success", "release_proximity": "success"},
    }
    RUNNER.save_record(
        case.output / "dbt/process.json",
        {
            "pid": 12345,
            "returncode": 0,
            "owned_group_stopped": True,
            "failure_type": None,
            "ipc_requests": 6,
            "metadata_requests": 0,
            "credentials_sent": False,
        },
    )
    RUNNER.save_record(
        case.output / "dbt/worker-report.json",
        {
            "request_sha256": request_pin,
            "binding_restored": True,
            "credentials_received": False,
            "network_attempts": 0,
            "ipc_requests": 6,
            "outcome": outcome,
        },
    )
    writer_pin = case.options["writer_receipt_sha256"]
    RUNNER.save_record(
        case.output / "builder-operation.json",
        {
            "version": 1,
            "status": "scoped_view_build_completed",
            "view_plan_sha256": pin,
            "project_receipt_sha256": recipe["project_receipt_sha256"],
            "writer_receipt_sha256": writer_pin,
            "binding": recipe["source_plan"]["binding"],
            "caller": CALLER,
            "run_intent_sha256": result["run_intent_sha256"],
            "receipt_sha256": hashlib.sha256(
                (case.output / "receipt.json").read_bytes()
            ).hexdigest(),
        },
    )
    if capture:
        from unittest.mock import Mock

        from dbt_catalog_fixtures import fake_view

        metadata = sys.modules["dbt_view_capture"]
        monkeypatch.setattr(metadata, "ARTIFACTS", tmp_path / "artifacts")
        client = Mock()
        client.get_table.side_effect = lambda **kw: {"Table": fake_view(recipe, kw["Name"])[0]}
        capture_pin = metadata.capture_views(
            client, recipe, pin, CALLER, case.output / "view-metadata"
        )
        source = RECEIPT.read_record(case.output / "source-verification.json")[0]["source"]
        gate_pin = RUNNER.save_record(
            case.output / "view-metadata/source-gate.json",
            {
                "run_intent_sha256": result["run_intent_sha256"],
                "capture_sha256": capture_pin,
                "caller": CALLER,
                "catalog": source["catalog"],
            },
        )
        outer = case.output / "builder-operation.json"
        binding = json.loads(outer.read_bytes()) | {
            "capture_view_metadata": True,
            "view_metadata_sha256": capture_pin,
            "view_metadata_source_gate_sha256": gate_pin,
        }
        outer.write_bytes(RUNNER.canonical(binding))
    proof = RECEIPT.save_completion(
        case.output,
        recipe,
        pin,
        case.options["input_path"],
        writer_pin,
        result,
        capture_view_metadata=capture,
    )
    sdk = tmp_path / "artifacts/sdk"
    sdk.mkdir()
    bundle = tmp_path / ".private/tools/glue-runtime/iceberg-aws-bundle-1.10.2.jar"
    bundle.parent.mkdir(parents=True)
    bundle.write_bytes(b"synthetic bundle")
    return SimpleNamespace(
        case=case,
        project=case.project,
        plan=plan,
        pin=pin,
        recipe=recipe,
        result=result,
        input=case.options["input_path"],
        writer=case.options["writer_receipt"],
        writer_pin=writer_pin,
        evidence=case.output,
        reply=RECEIPT.operation_reply(pin, proof),
        sdk=sdk,
        bundle=bundle,
        output=tmp_path / "artifacts/launch",
    )
