"""Check the joined view runner with real dbt/SDK clients and a simulated source reader."""

import argparse
import copy
import hashlib
import json
import os
from contextlib import ExitStack
from importlib.metadata import version
from pathlib import Path
from typing import Any
from unittest.mock import patch

from athena_control import canonical
from athena_read_plan import read_plan, tooling_sdk_pins
from athena_results import expected_gold
from check_athena_control import Clock
from check_athena_preflight import CALLER, fake_readback, fake_table
from check_dbt_project import check_manifest
from check_dbt_views import ViewScenario
from dbt_view_plan import check_compiled_models
from dbt_view_runner import run_owned_views
from prepare_dbt_project import ARTIFACTS, prepare_project, verify_project

SCENARIOS = ("success", "wrong_caller", "wrong_readback", "catalog_between", "catalog_final")
EXPECTED_CALLER = CALLER | {"Arn": CALLER["Arn"].replace("-reader/", "-dbt/")}


def check(summary: Path, pin: str, output: Path, scenario: str) -> dict[str, Any]:
    gold = expected_gold(summary, pin)
    resolved = output.resolve()
    if (
        scenario not in SCENARIOS
        or output.exists()
        or output.is_symlink()
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
        or resolved.is_relative_to(summary.parent.resolve())
        or summary.parent.resolve().is_relative_to(resolved)
    ):
        raise ValueError("Use a known scenario and fresh artifacts output separate from input")
    pins = {"dbt-core": "1.12.6", "dbt-athena": "1.11.1"} | tooling_sdk_pins()
    if any(version(name) != value for name, value in pins.items()):
        raise ValueError("Use the locked analytics environment")
    source = read_plan(
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=123,
        source_run_sha256=pin,
    )
    output.mkdir(parents=True)
    project = output / "project"
    prepare_project(project, **source["binding"])
    fixtures = output / "fixtures"
    fixtures.mkdir()
    readback = fake_readback(source)
    writer = fixtures / "writer.json"
    writer.write_bytes(
        canonical(
            {
                "status": "created",
                "format_version": 2,
                "snapshot_id": 123,
                "source_run_sha256": pin,
                "rows": gold["mapped_buildings"],
                "readback_matches": True,
                "table": readback["table"],
                "verified_spark_settings": readback["verified_spark_settings"],
            }
        )
    )
    writer_pin = hashlib.sha256(writer.read_bytes()).hexdigest()
    readback.update(rows=gold["mapped_buildings"], writer_receipt_sha256=writer_pin)
    trace: list[dict[str, Any]] = []
    requests: dict[str, dict[str, Any]] = {}
    clients: dict[str, Any] = {}
    closers: list[Any] = []
    violations: list[str] = []
    network: list[str] = []
    reader_environments: list[dict[str, str]] = []
    reader_calls = dbt_calls = table_calls = 0
    active_control: Any = None
    dbt_active = False
    dbt_success = False
    dbt_statuses: dict[str, str] = {}
    clock = Clock()
    fake = ViewScenario(source, "create")
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
        raise OSError("Network disabled during joined view runner check")

    with patch.dict(os.environ, env, clear=True):
        import boto3
        from boto3.session import Session
        from botocore.client import BaseClient
        from botocore.stub import Stubber
        from dbt.cli.main import dbtRunner

        original_factory = Session.client
        original_call = BaseClient._make_api_call
        with ExitStack() as watches:

            def client_factory(session: Any, service: str, *args: Any, **kwargs: Any) -> Any:
                if dbt_active:
                    if service == "athena":
                        return active_control
                    if service in {"sts", "glue"}:
                        return clients[service]
                    raise ValueError("Unexpected dbt client: " + service)
                if service not in {"athena", "sts", "glue"} or service in clients:
                    raise ValueError("Unexpected owned client construction")
                client = original_factory(session, service, *args, **kwargs)
                clients[service] = client
                closers.append(
                    watches.enter_context(patch.object(client, "close", wraps=client.close))
                )
                return client

            def reader_factory(
                path: Path, credentials: dict[str, str], target: dict[str, str]
            ) -> Any:
                if target != source["target"] or path != output / "run/reader":
                    raise ValueError("Reader target/output differs")
                signer = clients["athena"]._request_signer._credentials
                if credentials.get("AWS_ACCESS_KEY_ID") != signer.access_key or any(
                    c._request_signer._credentials is not signer for c in clients.values()
                ):
                    raise ValueError("SDK/reader credentials are not frozen together")
                reader_environments.append(credentials)

                def reader(*args: Any, **kwargs: Any) -> dict[str, Any]:
                    nonlocal reader_calls
                    reader_calls += 1
                    value = copy.deepcopy(readback)
                    if scenario == "wrong_readback":
                        value["binding"] = value["binding"] | {"snapshot_id": 456}
                    return value

                return reader

            def execute(control: Any, path: Path) -> dict[str, Any]:
                nonlocal active_control, dbt_active, dbt_calls, dbt_success, dbt_statuses
                active_control, dbt_active = control, True
                dbt_calls += 1
                profiles = path / "profiles"
                profiles.mkdir(parents=True)
                (profiles / "profiles.yml").write_bytes(canonical(source["profile"]))
                try:
                    result = dbtRunner().invoke(
                        [
                            "--no-use-colors",
                            "--no-partial-parse",
                            "--log-path",
                            str(path / "logs"),
                            "run",
                            "--project-dir",
                            str(project),
                            "--profiles-dir",
                            str(profiles),
                            "--target-path",
                            str(path / "target"),
                            "--select",
                            "grid_proximity release_proximity",
                        ]
                    )
                finally:
                    dbt_active = False
                manifest = json.loads((path / "target/manifest.json").read_text())
                results = json.loads((path / "target/run_results.json").read_text())
                check_manifest(manifest, project, source["binding"])
                check_compiled_models(manifest, source["binding"])
                dbt_success = result.success is True
                dbt_statuses = {
                    r["unique_id"].split(".")[-1]: r["status"] for r in results["results"]
                }
                return {"success": dbt_success, "model_statuses": dbt_statuses}

            def reply(service: str, operation: str, params: dict[str, Any]) -> dict[str, Any]:
                nonlocal table_calls
                if service == "sts" and operation == "GetCallerIdentity" and not params:
                    return EXPECTED_CALLER | (
                        {"UserId": "unexpected-caller"} if scenario == "wrong_caller" else {}
                    )
                if (
                    service == "glue"
                    and operation == "GetTable"
                    and params
                    == {
                        "CatalogId": "123456789012",
                        "DatabaseName": source["binding"]["source_schema"],
                        "Name": "building_proximity",
                    }
                ):
                    table_calls += 1
                    table = fake_table(source)
                    if (scenario == "catalog_between" and table_calls >= 4) or (
                        scenario == "catalog_final" and table_calls >= 5
                    ):
                        table["Table"]["VersionId"] = "2"
                    return table
                if service != "athena":
                    return fake.reply(service, operation, params)
                if operation == "GetWorkGroup" and params == {
                    "WorkGroup": source["workgroup"]["Name"]
                }:
                    group = copy.deepcopy(source["workgroup"]) | {"State": "ENABLED"}
                    group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"] = (
                        "Athena engine version 3"
                    )
                    return {"WorkGroup": group}
                if operation == "StartQueryExecution":
                    number = len(requests) + 1
                    saved = json.loads(
                        (active_control.output / f"query-{number:02}/intent.json").read_text()
                    )
                    if (
                        params != saved["request"]
                        or not (output / f"run/submission-gate-{number:02}.json").is_file()
                    ):
                        raise ValueError("Source gate and exact request must precede submission")
                    identity = "joined-view-" + str(number)
                    requests[identity] = copy.deepcopy(params)
                    return {"QueryExecutionId": identity}
                query_id = params.get("QueryExecutionId")
                if not isinstance(query_id, str) or query_id not in requests:
                    raise ValueError("Unexpected query identity")
                request = requests[query_id]
                if operation == "GetQueryExecution" and params == {"QueryExecutionId": query_id}:
                    return {
                        "QueryExecution": {
                            "QueryExecutionId": query_id,
                            "Query": request["QueryString"],
                            "WorkGroup": request["WorkGroup"],
                            "QueryExecutionContext": request["QueryExecutionContext"],
                            "StatementType": "DDL",
                            "Status": {"State": "SUCCEEDED"},
                            "EngineVersion": {
                                "SelectedEngineVersion": "Athena engine version 3",
                                "EffectiveEngineVersion": "Athena engine version 3",
                            },
                            "ResultConfiguration": request["ResultConfiguration"]
                            | {
                                "OutputLocation": request["ResultConfiguration"]["OutputLocation"]
                                + query_id
                                + ".txt"
                            },
                            "ResultReuseConfiguration": request["ResultReuseConfiguration"],
                            "Statistics": {
                                "DataScannedInBytes": 0,
                                "ResultReuseInformation": {"ReusedPreviousResult": False},
                            },
                        }
                    }
                if operation == "GetQueryResults" and params == {
                    "QueryExecutionId": query_id,
                    "MaxResults": 1,
                }:
                    return {
                        "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
                        "UpdateCount": 0,
                    }
                raise ValueError("Unexpected Athena API operation: " + operation)

            def api_call(client: Any, operation: str, params: dict[str, Any]) -> Any:
                service = client.meta.service_model.service_name
                trace.append(
                    {
                        "service": service,
                        "operation": operation,
                        "parameters": copy.deepcopy(params),
                    }
                )
                try:
                    response = reply(service, operation, params)
                except Exception as error:
                    violations.append(str(error))
                    raise
                method = next(
                    name
                    for name, api in client.meta.method_to_api_mapping.items()
                    if api == operation
                )
                with Stubber(client) as stub:
                    stub.add_response(method, response, expected_params=params)
                    value = original_call(client, operation, params)
                    stub.assert_no_pending_responses()
                    return value

            watches.enter_context(patch.object(Session, "client", client_factory))
            watches.enter_context(patch.object(BaseClient, "_make_api_call", api_call))
            for name in (
                "socket.socket.connect",
                "socket.socket.connect_ex",
                "socket.create_connection",
                "socket.getaddrinfo",
            ):
                watches.enter_context(patch(name, side_effect=denied))
            session = boto3.Session(region_name="eu-west-1")
            error_type = None
            report: dict[str, Any] | None = None
            try:
                report = run_owned_views(
                    project,
                    output / "run",
                    target=source["target"],
                    input_path=summary.parent,
                    writer_receipt=writer,
                    writer_receipt_sha256=writer_pin,
                    caller_arn=EXPECTED_CALLER["Arn"],
                    caller_user_id=EXPECTED_CALLER["UserId"],
                    execute_models=execute,
                    session=session,
                    reader_factory=reader_factory,
                    now=clock.now,
                    pause=clock.pause,
                )
            except ValueError as error:
                error_type = type(error).__name__
            finally:
                (output / "trace.json").write_bytes(canonical(trace))
            if network or violations or fake.violations:
                raise ValueError("Unexpected network/API behavior: " + repr(violations))
            if (
                any(c.call_count != 1 for c in closers)
                or set(clients) != {"athena", "sts", "glue"}
                or any(reader_environments)
            ):
                raise ValueError("Owned clients or credential environment were not cleaned up")
    expected_queries = {
        "success": 2,
        "wrong_caller": 0,
        "wrong_readback": 0,
        "catalog_between": 1,
        "catalog_final": 2,
    }[scenario]
    if len(requests) != expected_queries or (report is not None) != (scenario == "success"):
        raise ValueError("Wrong query count or acceptance outcome")
    if (error_type is not None) != (scenario != "success"):
        raise ValueError("Unexpected runner exception")
    if dbt_calls != (0 if scenario in {"wrong_caller", "wrong_readback"} else 1):
        raise ValueError("dbt started before the source gate")
    if report is not None:
        assert (
            report["caller_identity_verified"]
            and report["source_readback_verified"]
            and not report["result_verified"]
        )
    elif (output / "run/receipt.json").exists():
        raise ValueError("Failed runner wrote an acceptance receipt")
    failure = json.loads((output / "run/failure.json").read_text()) if report is None else None
    if reader_calls != (0 if scenario == "wrong_caller" else 1):
        raise ValueError("Unexpected source reader ordering/count")
    if scenario in {"wrong_caller", "wrong_readback"} and (
        failure is None
        or failure["stage"] != "source_preflight"
        or failure["initial_source_verified"]
    ):
        raise ValueError("Initial source gate failed at the wrong stage")
    if scenario == "catalog_between" and (
        failure is None
        or failure["stage"] != "model_outcome"
        or dbt_statuses != {"grid_proximity": "success", "release_proximity": "error"}
    ):
        raise ValueError("Catalog drift must block exactly the second model")
    if scenario == "catalog_final" and (
        failure is None or failure["stage"] != "final_source_gate" or not dbt_success
    ):
        raise ValueError("Final drift must reject an otherwise successful model run")
    root_pin = hashlib.sha256((output / "run/intent.json").read_bytes()).hexdigest()
    outcome = report if report is not None else failure
    if outcome is None or outcome["run_intent_sha256"] != root_pin:
        raise ValueError("Outcome is not bound to the original run intent")
    if report is not None:
        for name, key in (
            ("source-verification.json", "source_verification_sha256"),
            ("final-gate.json", "final_gate_sha256"),
        ):
            if hashlib.sha256((output / "run" / name).read_bytes()).hexdigest() != report[key]:
                raise ValueError("Source/final proof is not bound to the receipt")
    verify_project(project)
    final = {
        "scenario": scenario,
        "tooling": pins,
        "simulated_query_submissions": len(requests),
        "actual_dbt_invocations": dbt_calls,
        "dbt_success": dbt_success,
        "dbt_model_statuses": dbt_statuses,
        "source_reader_calls": reader_calls,
        "source_readback_simulated": True,
        "owned_sdk_clients_closed": len(closers),
        "credential_environment_cleared": True,
        "receipt": report,
        "failure": failure,
        "aws_requests": 0,
        "spark_started": False,
        "live_source_readback_verified": False,
        "live_view_build_verified": False,
        "data_tests_executed": 0,
        "scope": "Real dbt/SDK and joined runner; simulated source and AWS responses",
    }
    (output / "trace.json").write_bytes(canonical(trace))
    (output / "check.json").write_bytes(canonical(final))
    return final


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--source-run-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            check(args.summary, args.source_run_sha256, args.output, args.scenario), indent=2
        )
    )


if __name__ == "__main__":
    main()
