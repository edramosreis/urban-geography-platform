"""Load executor components without dbt imports or credential discovery."""

import importlib.util
import sys

from athena_plan_fixtures import ROOT
from dbt_broker_fixtures import broker_case as broker_case

for name in (
    "athena_reader_worker",
    "athena_reader",
    "dbt_adapter_binding",
    "dbt_model_worker",
    "dbt_model_executor",
):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
EXECUTOR = sys.modules["dbt_model_executor"]
WORKER = sys.modules["dbt_model_worker"]
