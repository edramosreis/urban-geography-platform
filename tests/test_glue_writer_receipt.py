"""Bounded log acquisition tied to successful controller and frozen input, without AWS."""

import copy
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from urban_geography.glue_input import load_prepared_input
from urban_geography.glue_runtime import runtime_plan
from urban_geography.ingest import digest

ROOT = Path(__file__).resolve().parents[1]
for name in (
    "aws_login_bridge",
    "glue_control",
    "prepare_dbt_project",
    "writer_readback",
    "glue_writer_receipt",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
capture = sys.modules["glue_writer_receipt"]
control = sys.modules["glue_control"]
readback = sys.modules["writer_readback"]


@pytest.fixture
def case(frozen, tmp_path, monkeypatch):
    monkeypatch.setattr(control, "ROOT", tmp_path)
    plan = control.load_plan(frozen.path, frozen.pin)
    controller = tmp_path / "artifacts/attempt/receipt.json"
    controller.parent.mkdir(parents=True)
    result = {
        "job_name": plan["job"]["Name"],
        "release_id": plan["release_id"],
        "release_sha256": frozen.pin,
        "run_id": "jr_synthetic_123",
        "outcome": "job_succeeded",
        "terminal_confirmed": True,
    }
    controller.write_text(json.dumps(result))
    prepared = load_prepared_input(
        frozen.path / "input", frozen.manifest["identity"]["summary_sha256"]
    )
    recipe = runtime_plan(**frozen.manifest["identity"]["target"])
    claim = {
        "status": "created",
        "table": recipe["catalog"] + "." + recipe["database"] + ".building_proximity",
        "format_version": 2,
        "snapshot_id": 123,
        "rows": prepared.run.summary["mapped_buildings"],
        "source_run_sha256": prepared.run.summary_sha256,
        "readback_matches": True,
        "input_files": len(prepared.files),
        "input_payload_bytes": prepared.payload_bytes,
        "input_manifest_sha256": digest(prepared.run.path / "input-manifest.json"),
        "verified_spark_settings": recipe["spark_settings"],
    }
    versions = {name: "version-1" for name in set(prepared.files) | {"input-manifest.json"}}
    transfer = {
        "status": "complete",
        "source_run_sha256": prepared.run.summary_sha256,
        "input_manifest_sha256": claim["input_manifest_sha256"],
        "bucket": plan["bucket"],
        "expected_bucket_owner": plan["owner"],
        "prefix": "inputs/" + prepared.run.summary_sha256 + "/",
        "object_versions": versions,
        "get_requests": len(versions),
        "downloaded_body_bytes": prepared.payload_bytes
        + (prepared.run.path / "input-manifest.json").stat().st_size,
        "payload_bytes": prepared.payload_bytes,
        "mapped_buildings": prepared.run.summary["mapped_buildings"],
    }
    wrapper = {
        "status": "complete",
        "transfer": transfer,
        "publication": claim,
        "sdk_client_closed_before_writer": True,
        "scratch_cleaned": True,
    }
    event = {
        "logStreamName": result["run_id"] + "-driver",
        "eventId": "event-1",
        "timestamp": 1000,
        "ingestionTime": 1100,
        "message": json.dumps(wrapper) + "\n",
    }
    client = SimpleNamespace(filter_log_events=Mock(return_value={"events": [event]}), close=Mock())
    return SimpleNamespace(
        frozen=frozen,
        controller=controller,
        result=result,
        prepared=prepared,
        claim=claim,
        wrapper=wrapper,
        event=event,
        client=client,
        output=tmp_path / "artifacts/capture",
        recipe=recipe,
    )


def run(case, **kwargs):
    return capture.capture_writer_receipt(
        case.client,
        case.frozen.path,
        case.frozen.pin,
        case.controller,
        digest(case.controller),
        case.output,
        **kwargs,
    )


@pytest.mark.parametrize("status", ["created", "reused"])
def test_extracts_one_bound_claim_compatible_with_existing_reader(case, status):
    case.claim["status"] = status
    case.event["message"] = json.dumps(case.wrapper) + "\n"
    result = run(case)
    assert result["status"] == "writer_claim_captured" and result["snapshot_id"] == 123
    assert result["row_readback_performed"] is False
    assert result["binding"]["controller_receipt_sha256"] == digest(case.controller)
    assert result["log_event"]["event_id"] == "event-1"
    assert result["writer_receipt_sha256"] == digest(case.output / "writer.json")
    assert (
        readback.load_claim(
            case.output / "writer.json",
            result["writer_receipt_sha256"],
            case.prepared,
            case.claim["table"],
            case.recipe["spark_settings"],
        )
        == case.claim
    )
    case.client.filter_log_events.assert_called_once_with(
        logGroupIdentifier="arn:aws:logs:eu-north-1:123456789012:log-group:/urban-geography/urban-geo-test/glue/output",
        logStreamNamePrefix=case.result["run_id"],
        limit=100,
        unmask=False,
    )
    case.client.close.assert_not_called()  # Client ownership stays with the caller.


@pytest.mark.parametrize("defect", ["job", "pin", "state", "integer_boolean", "run_id"])
def test_invalid_controller_binding_cannot_read_logs(case, defect):
    if defect == "job":
        case.result["job_name"] = "another-job"
    elif defect == "pin":
        case.result["release_sha256"] = "0" * 64
    elif defect == "state":
        case.result["outcome"] = "job_failed"
    elif defect == "integer_boolean":
        case.result["terminal_confirmed"] = 1
    else:
        case.result["run_id"] = "jr_another/stream"
    case.controller.write_text(json.dumps(case.result))
    with pytest.raises(capture.CaptureError):
        run(case)
    case.client.filter_log_events.assert_not_called()
    assert not case.output.exists()


def test_controller_receipt_requires_the_explicit_checksum(case):
    with pytest.raises(capture.CaptureError, match="checksum"):
        capture.capture_writer_receipt(
            case.client, case.frozen.path, case.frozen.pin, case.controller, "0" * 64, case.output
        )
    case.client.filter_log_events.assert_not_called()


def test_empty_page_with_token_is_followed_before_publishing(case):
    case.client.filter_log_events.side_effect = [
        {"events": [], "nextToken": "next-page"},
        {"events": [case.event]},
    ]
    result = run(case)
    assert result["statistics"]["pages"] == 2
    assert case.client.filter_log_events.call_args.kwargs["nextToken"] == "next-page"


def test_repeated_event_is_deduplicated_but_later_pages_are_still_checked(case):
    case.client.filter_log_events.side_effect = [
        {"events": [case.event], "nextToken": "next-page"},
        {"events": [copy.deepcopy(case.event)]},
    ]
    assert run(case)["statistics"]["events"] == 2


@pytest.mark.parametrize(
    "defect",
    [
        "another_run",
        "prefix_collision",
        "event_conflict",
        "duplicate_claim",
        "repeated_token",
        "page_limit",
        "empty",
        "message_limit",
        "total_bytes",
        "events_limit",
        "boolean_timestamp",
    ],
)
def test_unbounded_ambiguous_or_unbound_logs_publish_no_claim(case, defect):
    second = copy.deepcopy(case.event)
    if defect == "another_run":
        case.event["logStreamName"] = "jr_other-driver"
    elif defect == "prefix_collision":
        case.event["logStreamName"] = case.result["run_id"] + "other-driver"
    elif defect == "event_conflict":
        second["message"] = "different"
        case.client.filter_log_events.return_value = {"events": [case.event, second]}
    elif defect == "duplicate_claim":
        second["eventId"] = "event-2"
        case.client.filter_log_events.return_value = {"events": [case.event, second]}
    elif defect == "repeated_token":
        case.client.filter_log_events.return_value = {"events": [], "nextToken": "same"}
    elif defect == "page_limit":
        case.client.filter_log_events.side_effect = [
            {"events": [], "nextToken": f"p-{i}"} for i in range(8)
        ]
    elif defect == "empty":
        case.client.filter_log_events.return_value = {"events": []}
    elif defect == "message_limit":
        case.event["message"] = "x" * (128 * 1024 + 1)
    elif defect == "total_bytes":
        events = [dict(case.event, eventId=f"e-{i}", message="x" * (128 * 1024)) for i in range(9)]
        case.client.filter_log_events.return_value = {"events": events}
    elif defect == "events_limit":
        case.client.filter_log_events.return_value = {"events": [case.event] * 101}
    else:
        case.event["timestamp"] = True
    with pytest.raises(capture.CaptureError):
        run(case)
    assert not (case.output / "writer.json").exists()
    assert json.loads((case.output / "capture.json").read_text())["status"] == "capture_failed"
    assert case.client.filter_log_events.call_count <= 8


@pytest.mark.parametrize(
    "defect",
    [
        "wrong_rows",
        "wrong_summary",
        "wrong_manifest",
        "wrong_table",
        "bad_snapshot",
        "wrong_settings",
        "transfer_account",
        "transfer_summary",
        "transfer_version_keys",
        "unclean_scratch",
        "malformed",
        "duplicate_key",
    ],
)
def test_invalid_writer_claim_cannot_become_a_receipt(case, defect):
    if defect == "wrong_rows":
        case.claim["rows"] += 1
    elif defect == "wrong_summary":
        case.claim["source_run_sha256"] = "0" * 64
    elif defect == "wrong_manifest":
        case.claim["input_manifest_sha256"] = "0" * 64
    elif defect == "wrong_table":
        case.claim["table"] = "another.database.table"
    elif defect == "bad_snapshot":
        case.claim["snapshot_id"] = True
    elif defect == "wrong_settings":
        case.claim["verified_spark_settings"] = {}
    elif defect == "transfer_account":
        case.wrapper["transfer"]["expected_bucket_owner"] = "999999999999"
    elif defect == "transfer_summary":
        case.wrapper["transfer"]["source_run_sha256"] = "0" * 64
    elif defect == "transfer_version_keys":
        case.wrapper["transfer"]["object_versions"].pop("summary.json")
    elif defect == "unclean_scratch":
        case.wrapper["scratch_cleaned"] = False
    case.event["message"] = json.dumps(case.wrapper)
    if defect == "malformed":
        case.event["message"] = '{"publication":'
    elif defect == "duplicate_key":
        case.event["message"] = '{"publication":{},"publication":{}}'
    with pytest.raises(capture.CaptureError):
        run(case)
    assert not (case.output / "writer.json").exists()


def test_budget_stops_before_the_next_request(case):
    case.client.filter_log_events.return_value = {"events": [], "nextToken": "later"}
    clock = iter([0, 0, 1, 60])
    with pytest.raises(capture.CaptureError, match="time_limit"):
        run(case, now=lambda: next(clock))
    assert case.client.filter_log_events.call_count == 1


def test_controller_mutation_during_capture_prevents_publication(case):
    def response(**kwargs):
        case.controller.write_text("changed")
        return {"events": [case.event]}

    case.client.filter_log_events.side_effect = response
    with pytest.raises(capture.CaptureError, match="changed_during_capture"):
        capture.capture_writer_receipt(
            case.client,
            case.frozen.path,
            case.frozen.pin,
            case.controller,
            digest(case.controller),
            case.output,
        )
    assert not (case.output / "writer.json").exists()


def test_sdk_failure_records_no_raw_message_and_never_retries(case):
    secret = "must-not-appear-in-evidence"
    case.client.filter_log_events.side_effect = RuntimeError(secret)
    with pytest.raises(capture.CaptureError) as error:
        run(case)
    assert secret not in str(error.value) + (case.output / "capture.json").read_text()
    case.client.filter_log_events.assert_called_once()


@pytest.mark.parametrize("kind", ["existing", "source", "controller"])
def test_unsafe_or_reused_capture_destination_never_reads_logs(case, kind):
    if kind == "existing":
        case.output.mkdir()
    elif kind == "source":
        case.output = case.frozen.path / "capture"
    else:
        case.output = case.controller.parent / "capture"
    with pytest.raises(ValueError):
        run(case)
    case.client.filter_log_events.assert_not_called()


def test_time_budget_also_applies_before_final_publication(case):
    clock = iter([0, 0, 1, 60])
    with pytest.raises(capture.CaptureError, match="time_limit"):
        run(case, now=lambda: next(clock))
    assert not (case.output / "writer.json").exists()
    case.client.filter_log_events.assert_called_once()


def test_frozen_payload_mutation_is_caught_before_publication(case):
    def response(**kwargs):
        next((case.frozen.path / "input").rglob("*.parquet")).write_bytes(b"changed")
        return {"events": [case.event]}

    case.client.filter_log_events.side_effect = response
    with pytest.raises(capture.CaptureError):
        run(case)
    assert not (case.output / "writer.json").exists()


def test_normal_stdout_lines_can_share_the_event_with_the_json_result(case):
    case.event["message"] = "initializing writer\n" + json.dumps(case.wrapper) + "\n"
    assert run(case)["status"] == "writer_claim_captured"


def test_saved_capture_is_revalidated_with_both_explicit_pins(case):
    result = run(case)
    verified = capture.load_capture(
        case.frozen.path,
        case.frozen.pin,
        case.controller,
        digest(case.controller),
        case.output,
        digest(case.output / "capture.json"),
        result["writer_receipt_sha256"],
    )
    assert verified == json.loads((case.output / "capture.json").read_text())


@pytest.mark.parametrize(
    "defect",
    [
        "proof_pin",
        "writer_pin",
        "controller_pin",
        "wrong_run",
        "wrong_snapshot",
        "wrong_stream",
        "bad_stats",
        "bad_claim",
    ],
)
def test_saved_capture_cannot_be_substituted_or_rebound(case, defect):
    run(case)
    proof_path = case.output / "capture.json"
    writer_path = case.output / "writer.json"
    proof = json.loads(proof_path.read_text())
    if defect == "wrong_run":
        proof["binding"]["run_id"] = "jr_other"
    elif defect == "wrong_snapshot":
        proof["snapshot_id"] = 124
    elif defect == "wrong_stream":
        proof["log_event"]["stream"] = "jr_other-driver"
    elif defect == "bad_stats":
        proof["statistics"]["pages"] = True
    elif defect == "bad_claim":
        claim = json.loads(writer_path.read_text())
        claim["rows"] += 1
        writer_path.write_text(json.dumps(claim))
        proof["writer_receipt_sha256"] = digest(writer_path)
    proof_path.write_text(json.dumps(proof))
    with pytest.raises(ValueError):
        capture.load_capture(
            case.frozen.path,
            case.frozen.pin,
            case.controller,
            "0" * 64 if defect == "controller_pin" else digest(case.controller),
            case.output,
            "0" * 64 if defect == "proof_pin" else digest(proof_path),
            "0" * 64 if defect == "writer_pin" else digest(writer_path),
        )
