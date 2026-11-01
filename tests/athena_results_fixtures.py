"""Small summary/CSV references with no Java, credentials or network."""

import csv
import importlib.util
import io
import json
import sys

from athena_control_fixtures import CONTROL  # noqa: F401 - registers script imports
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
