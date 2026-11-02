"""The two-view recipe stays bound to the prepared source and dedicated target."""

import hashlib
import importlib.util
import sys

import pytest
from athena_plan_fixtures import OPTIONS, ROOT

SPEC = importlib.util.spec_from_file_location("dbt_view_plan", ROOT / "scripts/dbt_view_plan.py")
assert SPEC is not None and SPEC.loader is not None
PLAN = importlib.util.module_from_spec(SPEC)
sys.modules["dbt_view_plan"] = PLAN
SPEC.loader.exec_module(PLAN)
PREPARE = sys.modules["prepare_dbt_project"]


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.setattr(PREPARE, "ARTIFACTS", tmp_path)
    path = tmp_path / "project"
    PREPARE.prepare_project(
        path, source_schema="urban_geo_test_m4", snapshot_id=123, source_run_sha256="a" * 64
    )
    return path


def test_two_view_plan_pins_source_recipe_and_caps_without_optional_tooling(project):
    plan = PLAN.view_build_plan(
        project, deployment_name=OPTIONS["deployment_name"], account_id=OPTIONS["account_id"]
    )
    assert [q["name"] for q in plan["queries"]] == ["grid_proximity", "release_proximity"]
    assert "FOR VERSION AS OF 123" in plan["queries"][0]["sql"]
    assert "'" + "a" * 64 + "' AS source_run_sha256" in plan["queries"][0]["sql"]
    assert '"urban_geo_test_m4"."grid_proximity"' in plan["queries"][1]["sql"]
    assert (
        plan["project_receipt_sha256"]
        == hashlib.sha256((project / "preparation.json").read_bytes()).hexdigest()
    )
    assert plan["limits"] == {
        "max_queries": 2,
        "max_query_bytes": 16384,
        "wall_seconds": 600,
        "max_total_scan_bytes": 104857600,
    }
    assert not plan["execution_authorized"] and not plan["source_readback_verified"]


def test_prepared_schema_must_match_target(project):
    with pytest.raises(ValueError, match="schema differs"):
        PLAN.view_build_plan(
            project, deployment_name="other-demo", account_id=OPTIONS["account_id"]
        )


def test_changed_guard_cannot_enter_the_view_plan(project):
    (project / "macros/require_view.sql").write_text("")
    with pytest.raises(ValueError, match="bytes differ"):
        PLAN.view_build_plan(
            project, deployment_name=OPTIONS["deployment_name"], account_id=OPTIONS["account_id"]
        )
