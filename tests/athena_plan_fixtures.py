"""Load the administration planner without optional SDK/dbt dependencies."""

import copy
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for name in ("prepare_dbt_project", "athena_read_plan"):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
ATHENA = sys.modules["athena_read_plan"]
OPTIONS = dict(
    deployment_name="urban-geo-test",
    account_id="123456789012",
    snapshot_id=123,
    source_run_sha256="a" * 64,
)


def workgroup(plan):
    result = copy.deepcopy(plan["workgroup"])
    result["State"] = "ENABLED"
    result["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = "Athena engine version 3"
    return result
