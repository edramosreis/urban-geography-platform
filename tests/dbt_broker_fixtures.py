"""Load the broker/IPC without the optional dbt runtime."""

import importlib.util
import sys
from unittest.mock import Mock

from athena_plan_fixtures import ROOT
from dbt_control_fixtures import case

for name in ("dbt_rpc", "dbt_metadata"):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
RPC = sys.modules["dbt_rpc"]
META = sys.modules["dbt_metadata"]


def broker_case(tmp_path, monkeypatch):
    fixture = case(tmp_path, monkeypatch)
    clients = {"athena": fixture.client, "sts": Mock(), "glue": Mock()}
    broker = META.DbtBroker(clients, fixture.control)
    return fixture, clients, broker


def message(broker, service, operation, **params):
    return {
        "version": 1,
        "sequence": broker.calls + 1,
        "service": service,
        "operation": operation,
        "parameters": params,
    }
