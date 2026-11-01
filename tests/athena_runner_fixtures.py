"""Load isolated-reader/runner helpers without starting Spark or discovering credentials."""

import importlib.util
import sys
from unittest.mock import Mock

from athena_plan_fixtures import ATHENA, OPTIONS, ROOT
from athena_preflight_fixtures import CALLER, CLIENTS, PREFLIGHT  # noqa: F401 - shared contracts
from athena_results_fixtures import RESULTS  # noqa: F401 - registers result helper


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


WORKER = load("athena_reader_worker")
READER = load("athena_reader")


def reader_case(tmp_path, monkeypatch):
    monkeypatch.setattr(READER, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(WORKER, "ARTIFACTS", tmp_path)
    plan = ATHENA.read_plan(**OPTIONS)
    recipe = WORKER.runtime_plan(**plan["target"])
    output = tmp_path / "reader"
    credentials = {
        "AWS_ACCESS_KEY_ID": "offline-testing",
        "AWS_SECRET_ACCESS_KEY": "offline-secret",
        "AWS_SESSION_TOKEN": "offline-token",
    }
    adapter = READER.ReaderProcess(output, credentials, plan["target"])
    process = Mock(pid=12345)
    process.wait.return_value = 0
    process.poll.return_value = 0
    captured = {}

    def launch(command, **kwargs):
        captured.update(command=command, environment=dict(kwargs["env"]), options=kwargs)
        content = (output / "request.json").read_bytes()
        report = {
            "request_sha256": READER.hashlib.sha256(content).hexdigest(),
            "runtime": {},
            "readback": {"proof": "reader"},
        }
        (output / "report.json").write_text(READER.json.dumps(report))
        return process

    popen = Mock(side_effect=launch)
    monkeypatch.setattr(READER.subprocess, "Popen", popen)
    cleanup = Mock(return_value=True)
    monkeypatch.setattr(READER, "cleanup_group", cleanup)
    options = dict(
        catalog_name=recipe["catalog"],
        namespace=recipe["database"],
        expected_settings=recipe["spark_settings"],
        expected_location=recipe["warehouse"] + "building_proximity",
    )
    args = (tmp_path / "input", "a" * 64, tmp_path / "receipt/writer.json", "b" * 64)
    return adapter, args, options, process, popen, cleanup, captured


RUNNER = load("athena_runner")
CLI = load("run_athena_pilot")


def trial_case(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from athena_control_fixtures import CONTROL, Clock, enabled_group, execution
    from athena_preflight_fixtures import synthetic_readback, synthetic_table
    from athena_results_fixtures import Body, csv_bytes, summary_reference

    source = tmp_path / "input"
    source.mkdir()
    summary, pin, expected = summary_reference(source)
    plan = ATHENA.read_plan(**(OPTIONS | {"source_run_sha256": pin}))
    recipe = WORKER.runtime_plan(**plan["target"])
    receipt = tmp_path / "writer.json"
    receipt.write_bytes(
        CONTROL.canonical(
            {
                "status": "created",
                "format_version": 2,
                "snapshot_id": 123,
                "source_run_sha256": pin,
                "rows": 6,
                "readback_matches": True,
                "table": "glue_catalog." + recipe["database"] + ".building_proximity",
                "verified_spark_settings": recipe["spark_settings"],
            }
        )
    )
    receipt_pin = CONTROL.hashlib.sha256(receipt.read_bytes()).hexdigest()
    monkeypatch.setattr(CONTROL, "ARTIFACTS", tmp_path)
    attempt = CONTROL.Attempt(tmp_path / "attempt", plan)
    clients = {name: Mock() for name in ("sts", "glue", "athena", "s3")}
    clients["sts"].get_caller_identity.return_value = dict(CALLER)
    clients["glue"].get_table.return_value = synthetic_table(plan)
    clients["athena"].get_work_group.return_value = enabled_group(attempt)
    clients["athena"].start_query_execution.return_value = {"QueryExecutionId": "query-001"}
    clients["athena"].get_query_execution.return_value = execution(attempt)
    data = csv_bytes(expected)
    body = Body(data)
    head = {
        "ContentLength": len(data),
        "ETag": '"offline-etag"',
        "ServerSideEncryption": "AES256",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }
    clients["s3"].head_object.return_value = head
    clients["s3"].get_object.return_value = head | {
        "Body": body,
        "ContentRange": f"bytes 0-{len(data) - 1}/{len(data)}",
        "ResponseMetadata": {"HTTPStatusCode": 206},
    }
    reader = Mock(
        return_value=synthetic_readback(plan) | {"rows": 6, "writer_receipt_sha256": receipt_pin}
    )
    clock = Clock()
    options = dict(
        input_path=source,
        writer_receipt=receipt,
        writer_receipt_sha256=receipt_pin,
        caller_arn=CALLER["Arn"],
        caller_user_id=CALLER["UserId"],
        snapshot_reader=reader,
        now=clock.now,
        pause=clock.pause,
    )
    return SimpleNamespace(
        plan=plan,
        attempt=attempt,
        clients=clients,
        reader=reader,
        body=body,
        expected=expected,
        options=options,
    )
