"""Offline preparation and shared-SQL semantics; these are not Athena execution tests."""

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "prepare_dbt_project",
    Path(__file__).resolve().parents[1] / "scripts" / "prepare_dbt_project.py",
)
assert SPEC is not None and SPEC.loader is not None
DBT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DBT)
BINDING = {"source_schema": "offline_m4", "snapshot_id": 123, "source_run_sha256": "a" * 64}


@pytest.fixture
def output(tmp_path, monkeypatch):
    monkeypatch.setattr(DBT, "ARTIFACTS", tmp_path / "artifacts")
    return tmp_path / "artifacts" / "prepared"


def test_preparation_records_shared_sql_and_requires_fresh_output(output):
    receipt = DBT.prepare_project(output, **BINDING)
    assert DBT.verify_project(output) == receipt
    assert receipt["binding"] == BINDING
    assert receipt["provenance_verified"] is False and receipt["athena_executed"] is False
    assert len(receipt["files"]) == 7
    assert set(receipt["canonical_sql_sha256"]) == {"grid_proximity", "release_proximity"}
    before = {p: p.read_bytes() for p in output.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="fresh"):
        DBT.prepare_project(output, **BINDING)
    assert all(p.read_bytes() == data for p, data in before.items())


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_schema", "unsafe; DROP TABLE facts"),
        ("source_schema", "two.schemas"),
        ("source_schema", ""),
        ("source_schema", "a" * 256),
        ("snapshot_id", True),
        ("snapshot_id", 0),
        ("snapshot_id", -1),
        ("snapshot_id", 2**63),
        ("snapshot_id", "123"),
        ("source_run_sha256", ""),
        ("source_run_sha256", "A" * 64),
        ("source_run_sha256", "a" * 63 + "'"),
    ],
)
def test_invalid_binding_fails_before_output(output, field, value):
    with pytest.raises(ValueError):
        DBT.prepare_project(output, **(BINDING | {field: value}))
    assert not output.parent.exists()


def test_output_cannot_escape_artifacts(output):
    with pytest.raises(ValueError, match="artifacts"):
        DBT.prepare_project(output.parent.parent / "outside", **BINDING)
    assert not output.parent.exists()


@pytest.mark.parametrize("tamper", ["model", "extra", "receipt", "canonical"])
def test_stale_or_modified_project_is_rejected(output, tmp_path, monkeypatch, tamper):
    DBT.prepare_project(output, **BINDING)
    if tamper == "model":
        (output / "models/grid_proximity.sql").write_text("SELECT 1", encoding="utf-8")
    elif tamper == "extra":
        (output / "profiles.yml").write_text("unreviewed profile", encoding="utf-8")
    elif tamper == "receipt":
        receipt = json.loads((output / "preparation.json").read_text())
        receipt["provenance_verified"] = True
        (output / "preparation.json").write_text(json.dumps(receipt))
    else:
        copy = tmp_path / "sql"
        copy.mkdir()
        for path in DBT.SQL.glob("*.sql"):
            (copy / path.name).write_bytes(path.read_bytes())
        with (copy / "grid_proximity.sql").open("a") as stream:
            stream.write("\n-- changed canonical SQL\n")
        monkeypatch.setattr(DBT, "SQL", copy)
    with pytest.raises(ValueError):
        DBT.verify_project(output)


def test_generated_models_preserve_weighted_counts_and_detect_bad_summaries(output):
    DBT.prepare_project(output, **BINDING)
    with sqlite3.connect(":memory:") as db:
        db.execute(
            "CREATE TABLE facts "
            "(source_release, area_id, source_run_sha256, grid_id, within_threshold)"
        )
        db.executemany(
            "INSERT INTO facts VALUES (?, ?, ?, ?, ?)",
            [
                ("r1", "pilot", "a" * 64, grid, flag)
                for grid, flag in [("g1", 1), ("g1", 0), ("g1", 0), ("g2", 0), ("g3", 1), ("g3", 0)]
            ]
            + [("r2", "pilot", "b" * 64, "g1", 1)],
        )
        grid = (
            (output / "models/grid_proximity.sql")
            .read_text()
            .replace("{{ ref('stg_building_proximity') }}", "facts")
        )
        release = (
            (output / "models/release_proximity.sql")
            .read_text()
            .replace("{{ ref('grid_proximity') }}", "grids")
        )
        db.execute("CREATE VIEW grids AS " + grid)
        db.execute("CREATE VIEW releases AS " + release)
        assert db.execute(
            "SELECT mapped_buildings, buildings_within, share_within "
            "FROM releases WHERE source_release='r1'"
        ).fetchone() == (6, 2, 2 / 6)
        assert db.execute(
            "SELECT mapped_buildings FROM releases WHERE source_release='r2'"
        ).fetchone() == (1,)
        invariant = (
            (output / "tests/gold_invariants.sql")
            .read_text()
            .replace("{{ ref('grid_proximity') }}", "grids")
            .replace("{{ ref('release_proximity') }}", "releases")
        )
        assert db.execute(invariant).fetchall() == []
        db.execute("DROP VIEW releases")
        db.execute("CREATE TABLE releases (mapped_buildings, buildings_within, share_within)")
        db.executemany(
            "INSERT INTO releases VALUES (?, ?, ?)",
            [(0, 0, None), (2, 3, 1.5), (6, 2, 0.5), (1, None, 0)],
        )
        assert len(db.execute(invariant).fetchall()) == 4
