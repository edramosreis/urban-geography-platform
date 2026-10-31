"""Prepare a fresh, local Athena dbt project from the shared Gold SQL; never query AWS."""

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"
SCAFFOLD = ROOT / "analytics" / "athena"
SQL = ROOT / "src" / "urban_geography" / "sql"
SCAFFOLD_FILES = (
    "dbt_project.yml",
    "models/sources.yml",
    "models/schema.yml",
    "models/stg_building_proximity.sql",
    "tests/gold_invariants.sql",
)


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def validate_binding(source_schema: str, snapshot_id: int, source_run_sha256: str) -> None:
    if not isinstance(source_schema, str) or not re.fullmatch(
        r"[a-z][a-z0-9_]{0,254}", source_schema
    ):
        raise ValueError("source_schema must be a lowercase Glue identifier")
    if type(snapshot_id) is not int or not 0 < snapshot_id <= 2**63 - 1:
        raise ValueError("snapshot_id must be a positive signed 64-bit integer")
    if not isinstance(source_run_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", source_run_sha256
    ):
        raise ValueError("source_run_sha256 must be an explicit lowercase SHA256")


def project_files(binding: dict[str, Any]) -> tuple[dict[str, bytes], dict[str, str]]:
    if set(binding) != {"source_schema", "snapshot_id", "source_run_sha256"}:
        raise ValueError("Unexpected dbt source binding fields")
    validate_binding(**binding)
    content = {name: (SCAFFOLD / name).read_bytes() for name in SCAFFOLD_FILES}
    variables = "vars:\n" + "".join(
        f"  {key}: {json.dumps(value)}\n" for key, value in sorted(binding.items())
    )
    content["dbt_project.yml"] += ("\n" + variables).encode("utf-8")
    hashes = {}
    for name, token, parent in (
        ("grid_proximity", "facts", "stg_building_proximity"),
        ("release_proximity", "grids", "grid_proximity"),
    ):
        # Read canonical SQL directly: this preparation needs neither Spark nor its Python imports.
        sql = (SQL / (name + ".sql")).read_text(encoding="utf-8")
        hashes[name] = sha256(sql.encode("utf-8"))
        content["models/" + name + ".sql"] = sql.format(
            **{token: "{{ ref('" + parent + "') }}"}
        ).encode("utf-8")
    return content, hashes


def expected_manifest(
    binding: dict[str, Any], content: dict[str, bytes], hashes: dict[str, str]
) -> dict[str, Any]:
    return {
        "format_version": 1,
        "binding": binding,
        "canonical_sql_sha256": hashes,
        "files": {
            name: {"sha256": sha256(data), "bytes": len(data)}
            for name, data in sorted(content.items())
        },
        "provenance_verified": False,
        "athena_executed": False,
    }


def prepare_project(
    output: Path, *, source_schema: str, snapshot_id: int, source_run_sha256: str
) -> dict[str, Any]:
    binding = dict(
        source_schema=source_schema, snapshot_id=snapshot_id, source_run_sha256=source_run_sha256
    )
    content, hashes = project_files(binding)
    resolved = output.resolve()
    if not resolved.is_relative_to(ARTIFACTS.resolve()) or resolved == ARTIFACTS.resolve():
        raise ValueError("Use a fresh project directory under this checkout's artifacts")
    if output.exists() or output.is_symlink():
        raise ValueError("Use fresh dbt project output")
    output.mkdir(parents=True)
    for name, data in content.items():
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    receipt = expected_manifest(binding, content, hashes)
    (output / "preparation.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


def verify_project(project: Path) -> dict[str, Any]:
    receipt_path = project / "preparation.json"
    if receipt_path.stat().st_size > 32_768 or receipt_path.is_symlink():
        raise ValueError("Invalid dbt preparation receipt")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    content, hashes = project_files(receipt["binding"])
    if receipt != expected_manifest(receipt["binding"], content, hashes):
        raise ValueError("dbt preparation differs from current source")
    paths = {p.relative_to(project).as_posix() for p in project.rglob("*") if p.is_file()}
    if paths != set(content) | {"preparation.json"} or any(
        p.is_symlink() for p in project.rglob("*")
    ):
        raise ValueError("dbt project contains unexpected files or symlinks")
    for name, data in content.items():
        if (project / name).stat().st_size != len(data) or (project / name).read_bytes() != data:
            raise ValueError("dbt project bytes differ: " + name)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--snapshot-id", type=int, required=True)
    parser.add_argument("--source-run-sha256", required=True)
    args = parser.parse_args()
    receipt = prepare_project(
        args.output,
        source_schema=args.source_schema,
        snapshot_id=args.snapshot_id,
        source_run_sha256=args.source_run_sha256,
    )
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
