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
