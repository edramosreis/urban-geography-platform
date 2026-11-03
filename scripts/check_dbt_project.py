"""Parse the prepared project with real dbt Athena while denying network connections."""

import argparse
import json
import os
from importlib.metadata import version
from pathlib import Path
from typing import Any
from unittest.mock import patch

from dbt_view_plan import check_manifest
from prepare_dbt_project import ARTIFACTS, verify_project


def check_project(project: Path, output: Path) -> dict[str, Any]:
    receipt = verify_project(project)
    resolved = output.resolve()
    if (
        not resolved.is_relative_to(ARTIFACTS.resolve())
        or resolved == ARTIFACTS.resolve()
        or resolved.is_relative_to(project.resolve())
        or project.resolve().is_relative_to(resolved)
        or output.exists()
        or output.is_symlink()
    ):
        raise ValueError(
            "Use fresh check output under artifacts, separate from the prepared project"
        )
    if version("dbt-core") != "1.12.6" or version("dbt-athena") != "1.11.1":
        raise ValueError("Use the isolated, locked dbt environment")
    output.mkdir(parents=True)
    profiles = output / "profiles"
    profiles.mkdir()
    # Syntactically valid dummy settings for parsing, never a live connection profile.
    profile = {
        "urban_geo_athena": {
            "target": "offline_parse",
            "outputs": {
                "offline_parse": {
                    "type": "athena",
                    "database": "awsdatacatalog",
                    "schema": "offline_only",
                    "region_name": "eu-north-1",
                    "threads": 1,
                    "num_retries": 0,
                    "s3_staging_dir": "s3://dbt-offline-placeholder/queries/",
                    "work_group": "offline-placeholder",
                }
            },
        }
    }
    (profiles / "profiles.yml").write_text(json.dumps(profile, indent=2) + "\n", encoding="utf-8")
    env = {
        "AWS_ACCESS_KEY_ID": "offline-testing",
        "AWS_SECRET_ACCESS_KEY": "offline-testing",
        "AWS_SESSION_TOKEN": "offline-testing",
        "AWS_EC2_METADATA_DISABLED": "true",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DO_NOT_TRACK": "1",
    }
    attempts = []

    def denied(*args: Any, **kwargs: Any) -> Any:
        attempts.append("network request blocked")
        raise OSError("Network disabled during dbt parse")

    with patch.dict(os.environ, env):
        from dbt.cli.main import dbtRunner

        with (
            patch("socket.socket.connect", side_effect=denied),
            patch("socket.socket.connect_ex", side_effect=denied),
            patch("socket.create_connection", side_effect=denied),
            patch("socket.getaddrinfo", side_effect=denied),
        ):
            result = dbtRunner().invoke(
                [
                    "--no-use-colors",
                    "--no-partial-parse",
                    "--log-path",
                    str(output / "logs"),
                    "parse",
                    "--project-dir",
                    str(project),
                    "--profiles-dir",
                    str(profiles),
                    "--target-path",
                    str(output / "target"),
                ]
            )
    if attempts or not result.success:
        raise RuntimeError(
            f"Offline dbt parse failed; blocked attempts={len(attempts)}; {result.exception}"
        )
    manifest = json.loads((output / "target" / "manifest.json").read_text(encoding="utf-8"))
    counts = check_manifest(manifest, project, receipt["binding"])
    verify_project(project)
    report = {
        "dbt_core": version("dbt-core"),
        "dbt_athena": version("dbt-athena"),
        **counts,
        "binding": receipt["binding"],
        "canonical_sql_sha256": receipt["canonical_sql_sha256"],
        "blocked_network_attempts": len(attempts),
        "athena_queries_executed": 0,
        "data_tests_executed": 0,
        "provenance_verified": False,
        "scope": "dbt project parse and dependency graph only; Athena SQL execution unvalidated",
    }
    (output / "check.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check_project(args.project.resolve(), args.output.resolve()), indent=2))


if __name__ == "__main__":
    main()
