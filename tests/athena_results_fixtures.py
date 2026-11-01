"""Small summary/CSV references with no Java, credentials or network."""

import csv
import importlib.util
import io
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from athena_control_fixtures import CONTROL, Clock, enabled_group, execution
from athena_plan_fixtures import ROOT

spec = importlib.util.spec_from_file_location("athena_results", ROOT / "scripts/athena_results.py")
assert spec is not None and spec.loader is not None
RESULTS = importlib.util.module_from_spec(spec)
sys.modules["athena_results"] = RESULTS
spec.loader.exec_module(RESULTS)


def summary_reference(tmp_path):
    summary = {
        "status": "complete",
        "config": {"release": "2026-09-23.1", "area_id": "pilot"},
        "mapped_buildings": 6,
        "buildings_within_500m": 2,
        "share_within": 2 / 6,
    }
    path = tmp_path / "summary.json"
    path.write_text(json.dumps(summary), encoding="utf-8")
    pin = RESULTS.hashlib.sha256(path.read_bytes()).hexdigest()
    expected = RESULTS.expected_gold(path, pin)
    return path, pin, expected


def csv_bytes(expected, *, row=None, header=None, extra=None):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writerow(header if header is not None else RESULTS.COLUMNS)
    writer.writerow(row if row is not None else [expected[name] for name in RESULTS.COLUMNS])
    if extra is not None:
        writer.writerow(extra)
    return stream.getvalue().encode("utf-8")


class Body(io.BytesIO):
    def __init__(self, content):
        super().__init__(content)
        self.requests = []

    def read(self, size=-1):
        self.requests.append(size)
        return super().read(size)


def make_download_case(tmp_path, monkeypatch):
    from athena_plan_fixtures import ATHENA, OPTIONS

    path, pin, expected = summary_reference(tmp_path)
    plan = ATHENA.read_plan(**(OPTIONS | {"source_run_sha256": pin}))
    monkeypatch.setattr(CONTROL, "ARTIFACTS", tmp_path)
    attempt = CONTROL.Attempt(tmp_path / "attempt", plan)
    athena = Mock()
    athena.get_work_group.return_value = enabled_group(attempt)
    athena.start_query_execution.return_value = {"QueryExecutionId": "query-001"}
    athena.get_query_execution.return_value = execution(attempt)
    clock = Clock()
    receipt = CONTROL.execute_attempt(athena, attempt, now=clock.now, pause=clock.pause)
    athena.reset_mock()
    data = csv_bytes(expected)
    body = Body(data)
    head = {
        "ContentLength": len(data),
        "ETag": '"offline-etag"',
        "ServerSideEncryption": "AES256",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }
    s3 = Mock()
    s3.head_object.return_value = head
    s3.get_object.return_value = head | {
        "Body": body,
        "ContentRange": f"bytes 0-{len(data) - 1}/{len(data)}",
        "ResponseMetadata": {"HTTPStatusCode": 206},
    }
    return SimpleNamespace(
        path=path,
        pin=pin,
        expected=expected,
        plan=plan,
        attempt=attempt,
        athena=athena,
        receipt=receipt,
        data=data,
        body=body,
        s3=s3,
    )
