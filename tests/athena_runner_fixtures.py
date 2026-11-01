"""Load isolated-reader/runner helpers without starting Spark or discovering credentials."""

import importlib.util
import sys
from unittest.mock import Mock

from athena_plan_fixtures import ATHENA, OPTIONS, ROOT
from athena_preflight_fixtures import CALLER, CLIENTS, PREFLIGHT  # noqa: F401 - shared contracts


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
