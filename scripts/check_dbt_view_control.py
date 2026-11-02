"""Run real dbt through the view controller using only synthetic, SDK-validated responses."""

import argparse
import copy
import json
import os
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

from athena_control import ACTIVE, canonical
from athena_read_plan import create_athena_client, tooling_sdk_pins
from check_athena_control import Clock
from check_dbt_project import check_manifest
from check_dbt_views import ViewScenario
from dbt_view_control import ViewController
from dbt_view_plan import check_compiled_models
from prepare_dbt_project import ARTIFACTS, verify_project

SCENARIOS = ("success", "watchdog", "submission_unknown", "metadata_drift", "query_failure")


def check(project: Path, output: Path, scenario: str) -> dict[str, Any]:
    receipt = verify_project(project)
    resolved = output.resolve()
    if (
        scenario not in SCENARIOS
        or output.exists()
        or output.is_symlink()
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
        or resolved.is_relative_to(project.resolve())
        or project.resolve().is_relative_to(resolved)
    ):
        raise ValueError(
            "Use a known scenario and fresh artifacts output separate from the project"
        )
    from importlib.metadata import version

    pins = {"dbt-core": "1.12.6", "dbt-athena": "1.11.1"} | tooling_sdk_pins()
    if any(version(name) != pin for name, pin in pins.items()):
        raise ValueError("Use the locked analytics environment")
    output.mkdir(parents=True)
    trace: list[dict[str, Any]] = []
    requests: dict[str, dict[str, Any]] = {}
    violations: list[str] = []
    network: list[str] = []
    clients: list[Any] = []
    stopped: set[str] = set()
    clock = Clock()
    env = {
        "PATH": os.environ.get("PATH", ""),
        "AWS_ACCESS_KEY_ID": "offline-testing",
        "AWS_SECRET_ACCESS_KEY": "offline-testing",
        "AWS_SESSION_TOKEN": "offline-testing",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DO_NOT_TRACK": "1",
    }

    def denied(*args: Any, **kwargs: Any) -> Any:
        network.append("blocked")
        raise OSError("Network disabled during view controller check")

    with patch.dict(os.environ, env, clear=True):
        import boto3
        from boto3.session import Session
        from botocore.client import BaseClient
        from botocore.stub import Stubber
        from dbt.cli.main import dbtRunner

        raw = create_athena_client(session=boto3.Session(region_name="eu-west-1"))
        clients.append(raw)
        controller = ViewController(
            raw,
            project,
            output / "control",
            deployment_name="urban-geo-test",
            account_id="123456789012",
            now=clock.now,
            pause=clock.pause,
        )
        source = controller.plan()["source_plan"]
        fake = ViewScenario(source, "create")
        original_factory = Session.client
        original_call = BaseClient._make_api_call
        profiles = output / "profiles"
        profiles.mkdir()
        (profiles / "profiles.yml").write_text(
            json.dumps(source["profile"]) + "\n", encoding="utf-8"
        )

        def client_factory(session: Any, service: str, *args: Any, **kwargs: Any) -> Any:
            if service == "athena":
                return controller  # Exercise the adapter against the production boundary.
            if service not in {"glue", "sts"}:
                violations.append("Unexpected SDK client: " + service)
                raise ValueError(violations[-1])
            client = original_factory(session, service, *args, **kwargs)
            clients.append(client)
            return client

        def reply(operation: str, params: dict[str, Any]) -> dict[str, Any]:
            if operation == "GetWorkGroup" and params == {"WorkGroup": source["workgroup"]["Name"]}:
                group = copy.deepcopy(source["workgroup"]) | {"State": "ENABLED"}
                group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                    "Athena engine version 3"
                )
                return {"WorkGroup": group}
            if operation == "StartQueryExecution":
                number = len(requests) + 1
                saved = json.loads(
                    (controller.output / f"query-{number:02}/intent.json").read_text()
                )
                if (
                    params != saved["request"]
                    or not (controller.output / f"query-{number:02}/submission.json").is_file()
                ):
                    raise ValueError("Exact request/token was not durable before submission")
                identity = "controlled-view-" + str(number)
                requests[identity] = copy.deepcopy(params)
                return {"QueryExecutionId": identity}
            query_id = params.get("QueryExecutionId")
            if not isinstance(query_id, str) or query_id not in requests:
                raise ValueError("Unexpected query identity")
            identity = query_id
            if (
                operation == "StopQueryExecution"
                and params == {"QueryExecutionId": identity}
                and identity not in stopped
            ):
                stopped.add(identity)
                return {}
            if operation == "GetQueryExecution" and params == {"QueryExecutionId": identity}:
                request = requests[identity]
                state = (
                    "CANCELLED"
                    if identity in stopped
                    else (
                        "RUNNING"
                        if scenario in {"watchdog", "metadata_drift"}
                        else ("FAILED" if scenario == "query_failure" else "SUCCEEDED")
                    )
                )
                query = {
                    "QueryExecutionId": identity,
                    "Query": request["QueryString"],
                    "WorkGroup": request["WorkGroup"],
                    "QueryExecutionContext": request["QueryExecutionContext"],
                    "StatementType": "DDL",
                    "Status": {"State": state},
                    "EngineVersion": {
                        "SelectedEngineVersion": "Athena engine version 3",
                        "EffectiveEngineVersion": "Athena engine version 3",
                    },
                    "ResultConfiguration": request["ResultConfiguration"]
                    | {
                        "OutputLocation": request["ResultConfiguration"]["OutputLocation"]
                        + identity
                        + ".txt"
                    },
                    "ResultReuseConfiguration": request["ResultReuseConfiguration"],
                    "Statistics": {
                        "DataScannedInBytes": 0,
                        "ResultReuseInformation": {"ReusedPreviousResult": False},
                    },
                }
                if scenario == "metadata_drift" and state in ACTIVE:
                    query["WorkGroup"] = "wrong-workgroup"
                return {"QueryExecution": query}
            if operation == "GetQueryResults" and params == {
                "QueryExecutionId": identity,
                "MaxResults": 1,
            }:
                return {
                    "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
                    "UpdateCount": 0,
                }
            raise ValueError("Unexpected controller SDK operation: " + operation)

        def api_call(client: Any, operation: str, params: dict[str, Any]) -> Any:
            service = client.meta.service_model.service_name
            trace.append(
                {"service": service, "operation": operation, "parameters": copy.deepcopy(params)}
            )
            try:
                response = (
                    reply(operation, params)
                    if client is raw
                    else fake.reply(service, operation, params)
                )
            except Exception as error:
                violations.append(str(error))
                raise
            python_name = next(
                name for name, api in client.meta.method_to_api_mapping.items() if api == operation
            )
            with Stubber(client) as stub:
                if (
                    client is raw
                    and operation == "StartQueryExecution"
                    and scenario == "submission_unknown"
                ):
                    stub.add_client_error(
                        python_name,
                        service_error_code="InternalServerException",
                        service_message="Synthetic uncertain submission",
                        http_status_code=500,
                        expected_params=params,
                    )
                else:
                    stub.add_response(python_name, response, expected_params=params)
                value = original_call(client, operation, params)
                stub.assert_no_pending_responses()
                return value

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(Session, "client", client_factory))
                stack.enter_context(patch.object(BaseClient, "_make_api_call", api_call))
                for name in (
                    "socket.socket.connect",
                    "socket.socket.connect_ex",
                    "socket.create_connection",
                    "socket.getaddrinfo",
                ):
                    stack.enter_context(patch(name, side_effect=denied))
                with controller.session():
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
        finally:
            for client in reversed(clients):
                client.close()
            (output / "trace.json").write_bytes(canonical(trace))
    if network or violations or fake.violations or result.success != (scenario == "success"):
        raise ValueError(
            "Unexpected controlled dbt outcome: "
            + repr((violations, fake.violations, result.exception))
        )
    manifest = json.loads((output / "target/manifest.json").read_text())
    results = json.loads((output / "target/run_results.json").read_text())
    counts = check_manifest(manifest, project, receipt["binding"])
    check_compiled_models(manifest, receipt["binding"])
    report = controller.report()
    expected = {"success": "view_queries_completed"}.get(scenario, "view_queries_not_verified")
    if report["status"] != expected or len(requests) != (2 if scenario == "success" else 1):
        raise ValueError("Wrong query count or control outcome")
    expected_outcome = {
        "watchdog": "watchdog_limit",
        "submission_unknown": "submission_unknown",
        "metadata_drift": "execution_contract_unverified",
        "query_failure": "query_failed",
    }
    if scenario != "success" and report["queries"][0].get("outcome") != expected_outcome[scenario]:
        raise ValueError("Wrong controlled query failure")
    if len(stopped) != (1 if scenario in {"watchdog", "metadata_drift"} else 0):
        raise ValueError("Unexpected stop count")
    statuses = [r["status"] for r in results["results"]]
    if statuses != (["success", "success"] if scenario == "success" else ["error", "skipped"]):
        raise ValueError("Unexpected dbt dependency outcomes")
    verify_project(project)
    final = {
        "scenario": scenario,
        "tooling": pins,
        **counts,
        "controller": report,
        "simulated_query_submissions": len(requests),
        "stop_requests": len(stopped),
        "simulated_elapsed_seconds": clock.value,
        "sdk_clients_closed": len(clients),
        "bounded_query_controller_used": True,
        "aws_requests": 0,
        "spark_started": False,
        "data_tests_executed": 0,
        "live_ddl_verified": False,
        "scope": (
            "Real dbt with injected query controller and synthetic SDK responses; "
            "no live AWS execution"
        ),
    }
    (output / "check.json").write_bytes(canonical(final))
    return final


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    args = parser.parse_args()
    print(json.dumps(check(args.project.resolve(), args.output.resolve(), args.scenario), indent=2))


if __name__ == "__main__":
    main()
