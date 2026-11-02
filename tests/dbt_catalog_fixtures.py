"""Bounded synthetic ordinary Athena view metadata; no service calls."""

import base64
import importlib.util
import json
import sys

from dbt_control_fixtures import ROOT
from dbt_view_plan import compiled_models

SPEC = importlib.util.spec_from_file_location(
    "dbt_view_catalog", ROOT / "scripts/dbt_view_catalog.py"
)
assert SPEC is not None and SPEC.loader is not None
CATALOG = importlib.util.module_from_spec(SPEC)
sys.modules["dbt_view_catalog"] = CATALOG
SPEC.loader.exec_module(CATALOG)


def encoded(definition):
    return "/* Presto View: " + base64.b64encode(json.dumps(definition).encode()).decode() + " */"


def fake_view(plan, name):
    source = plan["source_plan"]
    text = ["source_release", "area_id", "source_run_sha256"]
    if name == "grid_proximity":
        text.append("grid_id")
    pairs = [(n, "varchar") for n in text] + [
        ("mapped_buildings", "bigint"),
        ("buildings_within", "bigint"),
        ("share_within", "double"),
    ]
    definition = {
        "originalSql": compiled_models(source["binding"])[name],
        "catalog": "awsdatacatalog",
        "schema": source["binding"]["source_schema"],
        "columns": [{"name": n, "type": t} for n, t in pairs],
        "owner": "offline",
        "runAsInvoker": True,
    }
    table = {
        "CatalogId": source["target"]["account_id"],
        "DatabaseName": source["binding"]["source_schema"],
        "Name": name,
        "VersionId": "1",
        "TableType": "VIRTUAL_VIEW",
        "Parameters": {"presto_view": "true"},
        "StorageDescriptor": {
            "Columns": [{"Name": n, "Type": "string" if t == "varchar" else t} for n, t in pairs]
        },
        "ViewOriginalText": encoded(definition),
        "ViewExpandedText": "/* Presto View */",
    }
    return table, definition
