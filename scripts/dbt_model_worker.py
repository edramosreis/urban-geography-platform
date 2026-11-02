"""Run the pinned two-view dbt graph without credentials or direct network access."""

import argparse
import hashlib
import json
import os
import socket
import sys
import time
from importlib.metadata import version
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from athena_read_plan import ARTIFACTS, same, tooling_sdk_pins
from dbt_adapter_binding import bound_adapter
from dbt_rpc import RemoteSession
from dbt_view_plan import check_compiled_models, check_manifest, view_build_plan


def tooling_ready() -> None:
    if sys.version_info[:2] != (3, 11) or os.name != "posix":
        raise ValueError("Use the pinned Python 3.11 Linux tooling runtime")
    for name, pin in ({"dbt-core": "1.12.6", "dbt-athena": "1.11.1"} | tooling_sdk_pins()).items():
        if version(name) != pin:
            raise ValueError("dbt tooling pins differ")


def load_request(path: Path, pin: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Use a regular dbt worker request")
    with path.open("rb") as f:
        content = f.read(65537)
    if len(content) > 65536 or hashlib.sha256(content).hexdigest() != pin:
        raise ValueError("dbt worker request size/checksum differs")
    value = json.loads(content)
    if (
        set(value) != {"version", "project", "output", "view_plan", "controller_intent_sha256"}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("Unexpected dbt worker request fields")
    project, output = Path(value["project"]), Path(value["output"])
    if (
        not output.resolve().is_relative_to(ARTIFACTS.resolve())
        or output.resolve() == ARTIFACTS.resolve()
        or output.is_symlink()
        or output.resolve().is_relative_to(project.resolve())
        or project.resolve().is_relative_to(output.resolve())
    ):
        raise ValueError("dbt worker output/project overlap or escape")
    plan = view_build_plan(project, **value["view_plan"]["source_plan"]["target"])
    if (
        not same(plan, value["view_plan"])
        or hashlib.sha256(canonical(plan)).hexdigest() != value["controller_intent_sha256"]
    ):
        raise ValueError("dbt worker plan differs from the verified controller recipe")
    return value


def bounded_json(path: Path, maximum: int) -> dict[str, Any]:
    if path.is_symlink():
        raise ValueError("dbt artifact is a symlink")
    with path.open("rb") as f:
        content = f.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError("dbt artifact exceeds its byte ceiling")
    value = json.loads(content)
    if not isinstance(value, dict):
        raise ValueError("dbt artifact must be an object")
    return value


def run(request: dict[str, Any], pin: str, channel: socket.socket) -> dict[str, Any]:
    tooling_ready()
    if any(
        os.environ.get(name)
        for name in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
        )
    ):
        raise ValueError("The dbt worker must not receive credentials or profiles")
    network_attempts: list[str] = []

    def audit(event: str, args: Any) -> None:
        if event in {
            "socket.connect",
            "socket.getaddrinfo",
            "socket.gethostbyname",
            "socket.gethostbyaddr",
        }:
            network_attempts.append(event)
            raise OSError("Direct network access is disabled in the dbt worker")

    sys.addaudithook(audit)
    from dbt.cli.main import dbtRunner

    output, project = Path(request["output"]), Path(request["project"])
    profiles = output / "profiles"
    profiles.mkdir()
    profile = request["view_plan"]["source_plan"]["profile"]
    write_once(profiles / "profiles.yml", canonical(profile))
    settings = profile["urban_geo_athena"]["outputs"]["prepared_athena"]
    remote = RemoteSession(channel, time.monotonic() + 720)
    with bound_adapter(remote, settings):
        result = dbtRunner().invoke(
            [
                "--no-use-colors",
                "--no-partial-parse",
                "--log-path",
                str(output / "logs"),
                "run",
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(profiles),
                "--target-path",
                str(output / "target"),
                "--select",
                "grid_proximity release_proximity",
            ]
        )
    if network_attempts:
        raise ValueError("dbt attempted direct network access")
    manifest = bounded_json(output / "target/manifest.json", 4 * 1024 * 1024)
    results = bounded_json(output / "target/run_results.json", 128 * 1024)
    check_manifest(manifest, project, request["view_plan"]["source_plan"]["binding"])
    check_compiled_models(manifest, request["view_plan"]["source_plan"]["binding"])
    statuses = {r["unique_id"].split(".")[-1]: r["status"] for r in results["results"]}
    plan = view_build_plan(project, **request["view_plan"]["source_plan"]["target"])
    if not same(plan, request["view_plan"]):
        raise ValueError("Prepared project changed during dbt")
    return {
        "request_sha256": pin,
        "outcome": {"success": result.success is True, "model_statuses": statuses},
        "binding_restored": True,
        "network_attempts": len(network_attempts),
        "credentials_received": False,
        "ipc_requests": remote.sequence,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--rpc-fd", type=int, required=True)
    args = parser.parse_args()
    request = load_request(args.request, args.request_sha256)
    with socket.socket(fileno=args.rpc_fd) as channel:
        channel.set_inheritable(False)
        report = run(request, args.request_sha256, channel)
    write_once(Path(request["output"]) / "worker-report.json", canonical(report))


if __name__ == "__main__":
    main()
