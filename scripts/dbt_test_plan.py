"""Fixed seven-test SQL contract for the pinned dbt project; no dbt or SDK imports."""

from pathlib import Path
from typing import Any

from athena_read_plan import MAX_SCAN_BYTES
from dbt_view_plan import compiled_models, normalized_sql, view_build_plan
from prepare_dbt_project import project_files, sha256

NOT_NULL_COLUMNS = ("building_id", "within_threshold", "grid_id", "source_release", "area_id")
TEST_NAMES = (
    "gold_invariants",
    "unique_stg_building_proximity_building_id",
    *("not_null_stg_building_proximity_" + name for name in NOT_NULL_COLUMNS),
)


def compiled_tests(binding: dict[str, Any]) -> dict[str, str]:
    files, _ = project_files(binding)
    stage = compiled_models(binding)["stg_building_proximity"]
    cte = "with __dbt__cte__stg_building_proximity as (\n" + stage + "\n)\n"
    relation = "__dbt__cte__stg_building_proximity"
    result = {
        "not_null_stg_building_proximity_" + column: cte
        + f"select {column} from {relation} where {column} is null"
        for column in NOT_NULL_COLUMNS
    }
    result["unique_stg_building_proximity_building_id"] = cte + (
        f"select building_id as unique_field, count(*) as n_records from {relation} "
        "where building_id is not null group by building_id having count(*) > 1"
    )
    gold = files["tests/gold_invariants.sql"].decode()
    for name in ("grid_proximity", "release_proximity"):
        gold = gold.replace(
            "{{ ref('" + name + "') }}", f'"AwsDataCatalog"."{binding["source_schema"]}"."{name}"'
        )
    result["gold_invariants"] = gold
    return result


def summary_sql(sql: str) -> str:
    return (
        "select count(*) as failures, count(*) != 0 as should_warn, "
        "count(*) != 0 as should_error from (\n" + sql + "\n) dbt_internal_test"
    )


def test_plan(project: Path, **target: str) -> dict[str, Any]:
    base = view_build_plan(project, **target)
    models = compiled_tests(base["source_plan"]["binding"])
    queries = []
    for name in sorted(models):
        sql = summary_sql(models[name])
        queries.append({"name": name, "sql": sql, "sql_sha256": sha256(sql.encode())})
    return base | {
        "kind": "data_tests",
        "queries": queries,
        "limits": base["limits"] | {"max_queries": 7, "max_total_scan_bytes": 7 * MAX_SCAN_BYTES},
    }


def check_compiled_tests(manifest: dict[str, Any], binding: dict[str, Any]) -> None:
    expected = compiled_tests(binding)
    nodes = {
        node["name"]: node for node in manifest["nodes"].values() if node["resource_type"] == "test"
    }
    if set(nodes) != set(expected):
        raise ValueError("Unexpected data-test graph")
    for name, node in nodes.items():
        config = node["config"]
        if (
            normalized_sql(node.get("compiled_code", "")) != normalized_sql(expected[name])
            or config.get("store_failures")
            or config.get("store_failures_as")
            or config["fail_calc"] != "count(*)"
            or config["warn_if"] != "!= 0"
            or config["error_if"] != "!= 0"
            or config["severity"].upper() != "ERROR"
        ):
            raise ValueError("Data-test SQL/configuration differs: " + name)
