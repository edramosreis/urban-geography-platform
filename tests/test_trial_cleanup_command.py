"""Phase selection, explicit evidence, portable hashes and local-only refusal checks."""

import argparse
import shutil
from dataclasses import replace

import pytest
from trial_cleanup_command_fixtures import command, make_command_case


@pytest.fixture(params=command.PHASES)
def case(frozen, tmp_path, monkeypatch, request):
    return make_command_case(frozen, tmp_path, monkeypatch, request.param)


def test_preparation_is_local_only_and_each_phase_has_an_explicit_pin(case):
    result = command.prepare(case.operation, case.output)
    assert len(result["request_sha256"]) == 64
    assert result["recipe"]["phase"] == case.operation.phase
    assert not case.output.exists()
    assert not case.glue.method_calls and not case.athena.method_calls and not case.s3.method_calls
    assert command.checked(case.operation, case.output, result["request_sha256"]) == result


def test_argument_roundtrip_preserves_every_explicit_record(case):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    command.add_arguments(parser)
    args = parser.parse_args(command.arguments(case.operation, case.output))
    assert command.from_arguments(args) == case.operation
    assert args.output == case.output and not args.execute


def test_request_pin_is_portable_across_explicit_readonly_record_mounts(case, tmp_path):
    original = command.prepare(case.operation, case.output)
    relocated = tmp_path / "relocated"
    relocated.mkdir()
    mapping = {}
    for index, record in enumerate(case.operation.records()):
        path = relocated / f"record-{index:02}.json"
        shutil.copyfile(record.path, path)
        mapping[record] = command.Record(path, record.sha256)
    op = replace(
        case.operation,
        inventory=mapping[case.operation.inventory],
        job=None if case.operation.job is None else mapping[case.operation.job],
        queries=tuple(
            command.QueryEvidence(
                mapping[q.intent],
                mapping[q.receipt],
                None if q.parent is None else mapping[q.parent],
            )
            for q in case.operation.queries
        ),
        quiet_receipt=None
        if case.operation.quiet_receipt is None
        else mapping[case.operation.quiet_receipt],
        quiet_completion=None
        if case.operation.quiet_completion is None
        else mapping[case.operation.quiet_completion],
        approved=None if case.operation.approved is None else mapping[case.operation.approved],
    )
    assert command.prepare(op, case.output)["request_sha256"] == original["request_sha256"]


@pytest.mark.parametrize("pin", [None, "0" * 64])
def test_execution_pin_is_required_before_any_output_or_sdk_call(case, pin):
    with pytest.raises(ValueError, match="reviewed phase"):
        command.checked(case.operation, case.output, pin)
    assert not case.output.exists() and not case.s3.method_calls


def test_modified_explicit_record_is_rejected_before_output(case):
    case.operation.inventory.path.write_bytes(b"changed")
    with pytest.raises(ValueError):
        command.prepare(case.operation, case.output)
    assert not case.output.exists() and not case.glue.method_calls


def test_output_cannot_overlap_any_explicit_input_parent(case):
    with pytest.raises(ValueError):
        command.prepare(case.operation, case.operation.inventory.path.parent / "child")


@pytest.mark.parametrize("defect", ["unknown", "resume", "approved", "quiet", "pause", "boolean"])
def test_conflicting_phase_options_fail_closed(frozen, tmp_path, monkeypatch, defect):
    case = make_command_case(frozen, tmp_path, monkeypatch)
    op = case.operation
    if defect == "unknown":
        op = replace(op, phase="destroy")
    elif defect == "resume":
        op = replace(op, resume=True)
    elif defect == "approved":
        op = replace(op, approved=op.inventory)
    elif defect == "quiet":
        op = replace(op, quiet_receipt=None)
    elif defect == "pause":
        op = replace(op, paused=False)
    else:
        op = replace(op, paused=1)
    with pytest.raises(ValueError):
        command.prepare(op, case.output)
    assert not case.output.exists() and not case.s3.method_calls
