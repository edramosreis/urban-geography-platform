"""Fixed SQL and local plan for the two dbt Gold views; no AWS or dbt imports."""

import re
from pathlib import Path
from typing import Any

from athena_read_plan import MAX_SCAN_BYTES, read_plan
from prepare_dbt_project import project_files, sha256, verify_project

VIEWS = ("grid_proximity", "release_proximity")


def normalized_sql(sql: str) -> str:
    # Canonical project SQL has no comment delimiters inside literals.
    return " ".join(re.sub(r"/\*.*?\*/|--[^\n]*", "", sql, flags=re.S).split())


def compiled_models(binding: dict[str, Any]) -> dict[str, str]:
    files, _ = project_files(binding)
    schema = binding["source_schema"]
    stage = (
        files["models/stg_building_proximity.sql"]
        .decode()
        .replace('{{ var("source_run_sha256") }}', binding["source_run_sha256"])
        .replace("{{ var('snapshot_id') }}", str(binding["snapshot_id"]))
        .replace(
            "{{ source('lakehouse', 'building_proximity') }}",
            f'"awsdatacatalog"."{schema}"."building_proximity"',
        )
    )
    grid = (
        files["models/grid_proximity.sql"]
        .decode()
        .replace("{{ ref('stg_building_proximity') }}", "__dbt__cte__stg_building_proximity")
        .replace(
            "WITH counts AS (",
            "WITH __dbt__cte__stg_building_proximity as (\n" + stage + "\n), counts AS (",
            1,
        )
    )
    release = (
        files["models/release_proximity.sql"]
        .decode()
        .replace("{{ ref('grid_proximity') }}", f'"AwsDataCatalog"."{schema}"."grid_proximity"')
    )
    return {"stg_building_proximity": stage, "grid_proximity": grid, "release_proximity": release}


def check_compiled_models(manifest: dict[str, Any], binding: dict[str, Any]) -> None:
    for name, expected in compiled_models(binding).items():
        compiled = manifest["nodes"]["model.urban_geo_athena." + name].get("compiled_code")
        if compiled is None and name == "release_proximity":
            continue  # A failed parent causes dbt to skip compiling this model.
        if not isinstance(compiled, str) or normalized_sql(compiled) != normalized_sql(expected):
            raise ValueError("dbt compilation differs from pinned model SQL: " + name)


def view_build_plan(
    project: Path, *, deployment_name: str, account_id: str, region: str = "eu-west-1"
) -> dict[str, Any]:
    receipt = verify_project(project)
    plan = read_plan(
        deployment_name=deployment_name,
        account_id=account_id,
        region=region,
        snapshot_id=receipt["binding"]["snapshot_id"],
        source_run_sha256=receipt["binding"]["source_run_sha256"],
    )
    if receipt["binding"] != plan["binding"]:
        raise ValueError("dbt source schema differs from the dedicated target database")
    models = compiled_models(receipt["binding"])
    schema = plan["binding"]["source_schema"]
    queries = []
    for name in VIEWS:
        sql = f'create or replace view "AwsDataCatalog"."{schema}"."{name}" as\n' + models[name]
        queries.append({"name": name, "sql": sql, "sql_sha256": sha256(sql.encode())})
    return {
        "contract_version": 1,
        "status": "local_plan_only",
        "source_plan": plan,
        "project_receipt_sha256": sha256((project / "preparation.json").read_bytes()),
        "queries": queries,
        "limits": {
            "max_queries": 2,
            "max_query_bytes": 16384,
            "wall_seconds": 600,
            "max_total_scan_bytes": MAX_SCAN_BYTES,
        },
        "source_readback_verified": False,
        "execution_authorized": False,
    }


def check_manifest(
    manifest: dict[str, Any], project: Path, binding: dict[str, Any]
) -> dict[str, int]:
    prefix = "model.urban_geo_athena."
    parents = {
        "stg_building_proximity": ["source.urban_geo_athena.lakehouse.building_proximity"],
        "grid_proximity": [prefix + "stg_building_proximity"],
        "release_proximity": [prefix + "grid_proximity"],
    }
    nodes = manifest["nodes"]
    models = {key: node for key, node in nodes.items() if node["resource_type"] == "model"}
    if set(models) != {prefix + name for name in parents}:
        raise ValueError("Unexpected dbt model graph")
    for name, expected in parents.items():
        node = models[prefix + name]
        config = node["config"]
        materialization = "ephemeral" if name.startswith("stg_") else "view"
        if (
            node["depends_on"]["nodes"] != expected
            or config["materialized"] != materialization
            or config.get("pre-hook")
            or config.get("post-hook")
            or node["raw_code"]
            != (project / "models" / (name + ".sql")).read_text(encoding="utf-8").strip()
        ):
            raise ValueError("dbt model configuration differs: " + name)
    source_id = parents["stg_building_proximity"][0]
    if set(manifest["sources"]) != {source_id}:
        raise ValueError("Unexpected dbt sources")
    source = manifest["sources"][source_id]
    if (source["database"], source["schema"], source["identifier"]) != (
        "awsdatacatalog",
        binding["source_schema"],
        "building_proximity",
    ):
        raise ValueError("dbt source binding differs")
    tests = [node for node in nodes.values() if node["resource_type"] == "test"]
    if len(tests) != 7 or len(nodes) != 10:
        raise ValueError("Expected six source-column tests and one Gold invariant test")
    if manifest["metadata"]["adapter_type"] != "athena":
        raise ValueError("Expected the real Athena adapter")
    return {"models": len(models), "sources": 1, "data_tests_parsed": len(tests)}
