"""Check the joined view runner with real dbt/SDK clients and a simulated source reader."""

import argparse
import copy
import hashlib
import json
import os
from contextlib import ExitStack
from datetime import UTC, datetime
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
from dbt_test_plan import test_plan
from dbt_view_plan import check_compiled_models, normalized_sql
from dbt_view_runner import run_owned_views
from prepare_dbt_project import ARTIFACTS, prepare_project, verify_project

SCENARIOS = (
    "success",
    "wrong_caller",
    "wrong_readback",
    "catalog_between",
    "catalog_final",
    "replace",
    "watchdog",
    "test_quality",
    "test_watchdog",
)
EXPECTED_CALLER = CALLER | {"Arn": CALLER["Arn"].replace("-reader/", "-dbt/")}


def check(
    summary: Path,
    pin: str,
    output: Path,
    scenario: str,
    *,
    process_executor: bool = False,
    data_tests: bool = False,
) -> dict[str, Any]:
    if data_tests and (
        not process_executor or scenario not in {"success", "test_quality", "test_watchdog"}
    ):
        raise ValueError("Test-phase scenarios require the supervised executor")
    if scenario.startswith("test_") and not data_tests:
        raise ValueError("Select the data-test phase for test scenarios")
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
    test_queries = test_plan(project, **source["target"])["queries"] if data_tests else []
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
    stopped: set[str] = set()
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
    fake = ViewScenario(source, "replace" if scenario == "replace" else "create")
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
                    if process_executor and service == "glue":
                        base = {
                            "CatalogId": "123456789012",
                            "DatabaseName": source["binding"]["source_schema"],
                        }
                        if operation == "GetDatabases" and params == {
                            "CatalogId": "123456789012",
                            "MaxResults": 10,
                        }:
                            return fake.reply(service, operation, {})
                        if operation == "GetTables" and params == base | {"MaxResults": 10}:
                            return fake.reply(service, operation, base)
                        if operation == "GetTableVersions" and params == base | {
                            "TableName": params.get("TableName"),
                            "MaxResults": 20,
                        }:
                            return fake.reply(
                                service,
                                operation,
                                {
                                    "DatabaseName": base["DatabaseName"],
                                    "TableName": params["TableName"],
                                },
                            )
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
                    testing = normalized_sql(params["QueryString"]).startswith(
                        "select count(*) as failures"
                    )
                    ordinal = 1 + sum(
                        normalized_sql(r["QueryString"]).startswith("select count(*) as failures")
                        == testing
                        for r in requests.values()
                    )
                    phase = "tests" if testing else "queries"
                    saved = json.loads(
                        (output / f"run/{phase}/query-{ordinal:02}/intent.json").read_text()
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
                if (
                    operation == "StopQueryExecution"
                    and params == {"QueryExecutionId": query_id}
                    and query_id not in stopped
                ):
                    stopped.add(query_id)
                    return {}
                state = (
                    "CANCELLED"
                    if query_id in stopped
                    else (
                        "RUNNING"
                        if scenario == "watchdog"
                        or (
                            scenario == "test_watchdog"
                            and normalized_sql(request["QueryString"]).startswith(
                                "select count(*) as failures"
                            )
                        )
                        else "SUCCEEDED"
                    )
                )
                testing = normalized_sql(request["QueryString"]).startswith(
                    "select count(*) as failures"
                )
                if operation == "GetQueryExecution" and params == {"QueryExecutionId": query_id}:
                    return {
                        "QueryExecution": {
                            "QueryExecutionId": query_id,
                            "Query": request["QueryString"],
                            "WorkGroup": request["WorkGroup"],
                            "QueryExecutionContext": request["QueryExecutionContext"],
                            "StatementType": "DML" if testing else "DDL",
                            "SubstatementType": "SELECT" if testing else "CREATE_VIEW",
                            "Status": {"State": state}
                            | (
                                {}
                                if state == "RUNNING"
                                else {"CompletionDateTime": datetime.now(UTC)}
                            ),
                            "EngineVersion": {
                                "SelectedEngineVersion": "Athena engine version 3",
                                "EffectiveEngineVersion": "Athena engine version 3",
                            },
                            "ResultConfiguration": request["ResultConfiguration"]
                            | {
                                "OutputLocation": request["ResultConfiguration"]["OutputLocation"]
                                + query_id
                                + (".csv" if testing else ".txt")
                            },
                            "ResultReuseConfiguration": request["ResultReuseConfiguration"],
                            "Statistics": {
                                "DataScannedInBytes": 4096 if testing else 0,
                                "ResultReuseInformation": {"ReusedPreviousResult": False},
                            },
                        }
                    }
                if (
                    operation == "GetQueryResults"
                    and testing
                    and params == {"QueryExecutionId": query_id, "MaxResults": 3}
                ):
                    name = next(
                        q["name"]
                        for q in test_queries
                        if normalized_sql(q["sql"]) == normalized_sql(request["QueryString"])
                    )
                    failures = 1 if scenario == "test_quality" and name == "gold_invariants" else 0
                    names = ["failures", "should_warn", "should_error"]
                    values = [
                        str(failures),
                        "true" if failures else "false",
                        "true" if failures else "false",
                    ]
                    return {
                        "ResultSet": {
                            "ResultSetMetadata": {
                                "ColumnInfo": [
                                    {"Name": n, "Type": t}
                                    for n, t in zip(
                                        names, ["bigint", "boolean", "boolean"], strict=True
                                    )
                                ]
                            },
                            "Rows": [
                                {"Data": [{"VarCharValue": v} for v in row]}
                                for row in (names, values)
                            ],
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
                    execute_models=None if process_executor else execute,
                    data_tests=data_tests,
                    session=session,
                    reader_factory=reader_factory,
                    now=clock.now,
                    pause=clock.pause,
                )
            except (ValueError, RuntimeError) as error:
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
    process = None
    worker_report = None
    if process_executor:
        process_path = output / "run/dbt/process.json"
        dbt_calls = int(process_path.exists())
        if process_path.exists():
            process = json.loads(process_path.read_text())
            if not process["owned_group_stopped"] or process["credentials_sent"]:
                raise ValueError("dbt process cleanup or credential isolation failed")
        report_path = output / "run/dbt/worker-report.json"
        if report_path.exists():
            worker_report = json.loads(report_path.read_text())
            dbt_success = worker_report["outcome"]["success"]
            dbt_statuses = worker_report["outcome"]["model_statuses"]
    test_process = None
    test_report = None
    if data_tests:
        path = output / "run/dbt-tests/process.json"
        if path.exists():
            test_process = json.loads(path.read_text())
            dbt_calls += 1
            if not test_process["owned_group_stopped"] or test_process["credentials_sent"]:
                raise ValueError("Test worker cleanup/credentials differ")
        path = output / "run/dbt-tests/worker-report.json"
        if path.exists():
            test_report = json.loads(path.read_text())
    expected_queries = {
        "success": 2,
        "replace": 2,
        "watchdog": 1,
        "wrong_caller": 0,
        "wrong_readback": 0,
        "catalog_between": 1,
        "catalog_final": 2,
        "test_quality": 9,
        "test_watchdog": 3,
    }[scenario]
    if data_tests and scenario == "success":
        expected_queries = 9
    if len(requests) != expected_queries or (report is not None) != (
        scenario in {"success", "replace"}
    ):
        raise ValueError("Wrong query count or acceptance outcome")
    if (error_type is not None) != (scenario not in {"success", "replace"}):
        raise ValueError("Unexpected runner exception")
    if dbt_calls != (
        2 if data_tests else (0 if scenario in {"wrong_caller", "wrong_readback"} else 1)
    ):
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
        or failure["stage"] != ("view_execution" if process_executor else "model_outcome")
        or (
            not process_executor
            and dbt_statuses != {"grid_proximity": "success", "release_proximity": "error"}
        )
    ):
        raise ValueError("Catalog drift must block exactly the second model")
    if scenario == "catalog_final" and (
        failure is None or failure["stage"] != "final_source_gate" or not dbt_success
    ):
        raise ValueError("Final drift must reject an otherwise successful model run")
    if scenario == "watchdog":
        if (
            failure is None
            or failure["query_control"]["queries"][0]["outcome"] != "watchdog_limit"
            or len(stopped) != 1
        ):
            raise ValueError("Worker failure lost owned-query cancellation")
        if not failure["query_control"]["queries"][0]["terminal_confirmed"]:
            raise ValueError("Cancellation was not confirmed before stopping the worker")
    test_control = (report or failure or {}).get("test_control")
    if data_tests:
        if test_control is None:
            raise ValueError("Missing test phase evidence")
        if scenario == "success" and (
            test_control["status"] != "data_tests_passed"
            or test_control["data_tests_executed"] != 7
        ):
            raise ValueError("Tests were not all accepted")
        if scenario == "test_quality" and (
            test_control["status"] != "data_tests_failed"
            or test_control["data_tests_executed"] != 7
        ):
            raise ValueError("Quality failures were not distinguished from query errors")
        if scenario == "test_watchdog" and (
            len(stopped) != 1 or test_control["queries"][0]["outcome"] != "watchdog_limit"
        ):
            raise ValueError("Data-test watchdog did not retain owned cancellation")
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
        "process_executor_used": process_executor,
        "stop_requests": len(stopped),
        "worker_process": process,
        "worker_report": worker_report,
        "test_worker_process": test_process,
        "test_worker_report": test_report,
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
        "data_tests_executed": test_control["data_tests_executed"] if test_control else 0,
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
    parser.add_argument("--process-executor", action="store_true")
    parser.add_argument("--data-tests", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            check(
                args.summary,
                args.source_run_sha256,
                args.output,
                args.scenario,
                process_executor=args.process_executor,
                data_tests=args.data_tests,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
