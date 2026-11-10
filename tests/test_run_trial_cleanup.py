"""Bounded stdin receiver and saved proof checks using injected, offline AWS clients."""

import importlib.util
import io
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from trial_cleanup_command_fixtures import ROOT, command, make_command_case
from trial_cleanup_fixtures import make_case, packet

for name in ("trial_data_cleanup", "trial_cleanup_receipt", "run_trial_cleanup"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
runner = sys.modules["run_trial_cleanup"]
receipts = sys.modules["trial_cleanup_receipt"]


@pytest.fixture(params=command.PHASES)
def case(frozen, tmp_path, monkeypatch, request):
    case = make_command_case(frozen, tmp_path, monkeypatch, request.param)
    case.prepared = command.prepare(case.operation, case.output)
    (tmp_path / "auth").mkdir()
    case.auth = make_case(frozen, tmp_path / "auth", monkeypatch)
    for name in ("trial_cleanup_operation", "trial_quiescence", "trial_data_cleanup"):
        module = sys.modules[name]
        if hasattr(module, "create_s3_client"):
            monkeypatch.setattr(module, "create_s3_client", lambda snapshot: case.s3)
        monkeypatch.setattr(
            module,
            "create_service_client",
            lambda snapshot, service: case.glue if service == "glue" else case.athena,
        )
    case.stdin = SimpleNamespace(isatty=lambda: False, buffer=io.BytesIO(packet()))
    monkeypatch.setattr(sys, "stdin", case.stdin)
    return case


def invoke(case, monkeypatch, extra=()):
    argv = ["run_trial_cleanup.py", *command.arguments(case.operation, case.output), *extra]
    monkeypatch.setattr(sys, "argv", argv)
    runner.main()


def execute(case, monkeypatch, capsys):
    invoke(
        case,
        monkeypatch,
        ["--execute", "--request-sha256", case.prepared["request_sha256"], "--credentials-stdin"],
    )
    content = capsys.readouterr().out.encode()
    return receipts.verify_reply(content, case.prepared["request_sha256"], case.operation.phase)


def test_plan_only_never_reads_stdin_opens_clients_or_creates_output(case, monkeypatch, capsys):
    case.stdin.buffer = Mock(side_effect=AssertionError("must not read"))
    invoke(case, monkeypatch)
    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "plan_only" and not result["credential_exported"]
    assert not case.output.exists() and not case.auth.factory.called


def test_each_phase_runs_through_scoped_clients_and_produces_verifiable_fixed_proof(
    case, monkeypatch, capsys
):
    value = execute(case, monkeypatch, capsys)
    assert value["status"] == "phase_completed", value
    proof = receipts.load_completion(case.output, case.operation, case.prepared, value)
    assert proof["data_cleanup_observed"] == (case.operation.phase == "delete-data")
    assert proof["operation_observed"] == (case.operation.phase != "prepare-data")
    assert not proof["requires_review"]
    if case.operation.phase == "prepare-data":
        assert len(proof["prepared_plan_sha256"]) == 64
    assert all(not any(c.session_options().values()) for c in case.auth.credentials)
    assert all(c.close.call_count == 1 for c in case.auth.clients)
    assert all("dummyLoginToken" not in p.read_text() for p in case.output.rglob("*.json"))


@pytest.mark.parametrize("defect", ["no_pin", "wrong_pin", "no_pipe", "tty", "too_large", "empty"])
def test_execution_refuses_bad_request_or_private_pipe_before_sdk(
    case, monkeypatch, capsys, defect
):
    extra = [
        "--execute",
        "--request-sha256",
        case.prepared["request_sha256"],
        "--credentials-stdin",
    ]
    if defect == "no_pin":
        extra = ["--execute", "--credentials-stdin"]
    elif defect == "wrong_pin":
        extra[2] = "0" * 64
    elif defect == "no_pipe":
        extra.pop()
    elif defect == "tty":
        case.stdin.isatty = lambda: True
    elif defect == "too_large":
        case.stdin.buffer = io.BytesIO(b"x" * (runner.MAX_BYTES + 1))
    else:
        case.stdin.buffer = io.BytesIO(b"")
    invoke(case, monkeypatch, extra)
    assert json.loads(capsys.readouterr().out)["status"] == "operation_failed"
    assert not case.auth.factory.called and not case.output.exists()


def test_changed_saved_record_cannot_be_accepted_after_transport(case, monkeypatch, capsys):
    value = execute(case, monkeypatch, capsys)
    (case.output / "session-complete.json").write_text("{}")
    with pytest.raises((ValueError, KeyError)):
        receipts.load_completion(case.output, case.operation, case.prepared, value)


def test_untrusted_proof_paths_are_never_followed(case, monkeypatch, capsys):
    value = execute(case, monkeypatch, capsys)
    proof, _ = receipts._read(case.output, receipts.PROOF)
    proof["files"]["../../secret.json"] = "0" * 64
    content = runner.sys.modules["athena_control"].canonical(proof)
    (case.output / receipts.PROOF).write_bytes(content)
    value["proof_sha256"] = __import__("hashlib").sha256(content).hexdigest()
    with pytest.raises(ValueError):
        receipts.load_completion(case.output, case.operation, case.prepared, value)


@pytest.mark.parametrize("case", ["delete-data"], indirect=True)
@pytest.mark.parametrize("defect", ["denied_delete", "new_version", "parts_remain"])
def test_incomplete_cleanup_is_transportable_but_cannot_be_forged_as_absent(
    case, monkeypatch, capsys, defect
):
    from trial_data_cleanup_fixtures import ServiceError

    if defect == "denied_delete":
        case.s3.delete_object.side_effect = ServiceError("AccessDenied", 403)
    elif defect == "parts_remain":
        case.s3.list_parts.side_effect = lambda **kw: {
            "Bucket": kw["Bucket"],
            "Key": kw["Key"],
            "UploadId": kw["UploadId"],
            "IsTruncated": False,
            "Parts": [{"PartNumber": 1, "Size": 5}],
        }
    else:
        original = case.s3.delete_object.side_effect

        def new_version(**kw):
            value = original(**kw)
            if "VersionId" not in kw:
                from trial_data_cleanup_fixtures import NOW

                case.versions.append(
                    {
                        "Key": "warehouse/building_proximity/new.parquet",
                        "VersionId": "new",
                        "IsLatest": True,
                        "Size": 1,
                        "ETag": '"new"',
                        "LastModified": NOW,
                    }
                )
            return value

        case.s3.delete_object.side_effect = new_version
    value = execute(case, monkeypatch, capsys)
    proof = receipts.load_completion(case.output, case.operation, case.prepared, value)
    assert proof["requires_review"] and not proof["data_cleanup_observed"]
    path = case.output / "attempt/receipt.json"
    saved = json.loads(path.read_bytes())
    saved.update(
        data_cleanup_observed=True, requires_review=False, status="trial_data_absence_observed"
    )
    content = sys.modules["athena_control"].canonical(saved)
    path.write_bytes(content)
    outer = case.output / "session-complete.json"
    saved = json.loads(outer.read_bytes())
    saved.update(
        data_cleanup_observed=True,
        requires_review=False,
        record_sha256=__import__("hashlib").sha256(content).hexdigest(),
    )
    outer.write_bytes(sys.modules["athena_control"].canonical(saved))
    with pytest.raises(ValueError):
        receipts.save_completion(case.output, case.operation, case.prepared)


@pytest.mark.parametrize("mutation", ["extra", "boolean_version", "wrong_phase", "bad_hash"])
def test_transport_reply_schema_is_strict(mutation):
    value = receipts.reply("a" * 64, "inventory", "b" * 64)
    if mutation == "extra":
        value["secret"] = "sentinel"
    elif mutation == "boolean_version":
        value["version"] = True
    elif mutation == "wrong_phase":
        value["phase"] = "delete-data"
    else:
        value["proof_sha256"] = "invalid"
    with pytest.raises(ValueError):
        receipts.verify_reply(json.dumps(value).encode(), "a" * 64, "inventory")
