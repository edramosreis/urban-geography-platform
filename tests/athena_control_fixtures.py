"""Load the offline Athena controller and construct explicitly synthetic attempts."""

import importlib.util
import sys

from athena_plan_fixtures import ATHENA, OPTIONS, ROOT, workgroup

spec = importlib.util.spec_from_file_location("athena_control", ROOT / "scripts/athena_control.py")
assert spec is not None and spec.loader is not None
CONTROL = importlib.util.module_from_spec(spec)
sys.modules["athena_control"] = CONTROL
spec.loader.exec_module(CONTROL)


def make_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(CONTROL, "ARTIFACTS", tmp_path)
    return CONTROL.Attempt(tmp_path / "attempt", ATHENA.read_plan(**OPTIONS))


def enabled_group(attempt):
    return {"WorkGroup": workgroup(attempt.intent()["plan"])}


class Clock:
    def __init__(self):
        self.value = 0.0

    def now(self):
        return self.value

    def pause(self, seconds):
        self.value += seconds


def execution(attempt, state="SUCCEEDED", **overrides):
    request = attempt.intent()["request"]
    query = {
        "QueryExecutionId": "query-001",
        "Query": request["QueryString"],
        "WorkGroup": request["WorkGroup"],
        "QueryExecutionContext": request["QueryExecutionContext"],
        "StatementType": "DML",
        "SubstatementType": "SELECT",
        "Status": {"State": state},
        "EngineVersion": {
            "SelectedEngineVersion": "Athena engine version 3",
            "EffectiveEngineVersion": "Athena engine version 3",
        },
        "ResultConfiguration": request["ResultConfiguration"]
        | {"OutputLocation": request["ResultConfiguration"]["OutputLocation"] + "query-001.csv"},
        "ResultReuseConfiguration": request["ResultReuseConfiguration"],
        "Statistics": {
            "DataScannedInBytes": 4096,
            "ResultReuseInformation": {"ReusedPreviousResult": False},
        },
    }
    return {"QueryExecution": query | overrides}
