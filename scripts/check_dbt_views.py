"""Exercise real dbt view materialization with synthetic, SDK-validated AWS responses only."""

import argparse
import json
import os
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from typing import Any
from unittest.mock import patch

from athena_read_plan import read_plan, tooling_sdk_pins
from check_dbt_project import check_manifest
from dbt_view_plan import VIEWS, check_compiled_models, normalized_sql
from prepare_dbt_project import ARTIFACTS, verify_project

SCENARIOS = ("create", "replace", "table_collision", "full_refresh_collision", "query_failure")


@dataclass
class ViewScenario:
    """A strict fake service boundary; never an AWS client or a live execution guard."""

    plan: dict[str, Any]
    scenario: str
    calls: list[dict[str, Any]] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)

    def reply(self, service: str, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            if self.scenario not in SCENARIOS or len(self.calls) >= 40:
                raise ValueError("Unexpected scenario or excessive simulated SDK calls")
            self.calls.append({"service": service, "operation": operation, "parameters": params})
            return self._reply(service, operation, params)
        except (ValueError, KeyError, IndexError, TypeError) as error:
            self.violations.append(str(error))
            raise

    def _reply(self, service: str, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        account = self.plan["target"]["account_id"]
        schema = self.plan["binding"]["source_schema"]
        if (service, operation) == ("sts", "GetCallerIdentity") and not params:
            return {
                "Account": account,
                "Arn": f"arn:aws:iam::{account}:user/offline",
                "UserId": "offline",
            }
        if service == "glue":
            if operation == "GetDatabases" and not params:
                return {"DatabaseList": [{"Name": schema}]}
            if params.get("DatabaseName") != schema or params.get("CatalogId", account) != account:
                # GetDatabase uses Name, unlike the table APIs.
                if operation != "GetDatabase" or params != {"CatalogId": account, "Name": schema}:
                    raise ValueError("Unexpected Glue database or account")
            if operation == "GetDatabase" and params == {"CatalogId": account, "Name": schema}:
                return {"Database": {"Name": schema}}
            if operation == "GetTables" and params == {
                "CatalogId": account,
                "DatabaseName": schema,
            }:
                tables = [{"Name": "building_proximity", "TableType": "EXTERNAL_TABLE"}]
                if self.scenario == "replace":
                    tables += [{"Name": name, "TableType": "VIRTUAL_VIEW"} for name in VIEWS]
                elif self.scenario in {"table_collision", "full_refresh_collision"}:
                    tables += [{"Name": VIEWS[0], "TableType": "EXTERNAL_TABLE"}]
                return {"TableList": tables}
            name = params.get("TableName", params.get("Name"))
            if name not in VIEWS:
                raise ValueError("Unexpected Glue relation")
            expected = {"DatabaseName": schema, "TableName": name}
            if operation == "GetTableVersions" and params == expected:
                # Views hold SQL, not these placeholder S3 locations. The adapter reads
                # Location while expiring versions even when delete_s3=False.
                versions = range(1, 6) if self.scenario == "replace" else range(1, 2)
                return {
                    "TableVersions": [
                        {
                            "VersionId": str(number),
                            "Table": {
                                "Name": name,
                                "VersionId": str(number),
                                "StorageDescriptor": {
                                    "Location": "s3://offline-placeholder/no-data"
                                },
                            },
                        }
                        for number in versions
                    ]
                }
            if (
                operation == "DeleteTableVersion"
                and self.scenario == "replace"
                and params == (expected | {"CatalogId": account, "VersionId": "1"})
            ):
                return {}
            raise ValueError("Unexpected Glue operation or parameters: " + operation)
        if service == "athena":
            if operation == "GetWorkGroup" and params == {
                "WorkGroup": self.plan["workgroup"]["Name"]
            }:
                return {"WorkGroup": self.plan["workgroup"] | {"State": "ENABLED"}}
            if operation == "StartQueryExecution":
                index = len(self.queries)
                if index >= len(VIEWS):
                    raise ValueError("Too many simulated view submissions")
                expected = {
                    "QueryString": params.get("QueryString"),
                    "WorkGroup": self.plan["workgroup"]["Name"],
                    "QueryExecutionContext": self.plan["request_template"]["QueryExecutionContext"],
                    "ResultConfiguration": {
                        "OutputLocation": self.plan["request_template"]["ResultConfiguration"][
                            "OutputLocation"
                        ]
                    },
                }
                prefix = f'create or replace view "AwsDataCatalog"."{schema}"."{VIEWS[index]}" as '
                if params != expected or not normalized_sql(params["QueryString"]).startswith(
                    prefix
                ):
                    raise ValueError("Unexpected view SQL, workgroup, context or result location")
                self.queries.append(params["QueryString"])
                return {"QueryExecutionId": f"offline-view-{index + 1}"}
            if operation in {"GetQueryExecution", "GetQueryResults"} and params in [
                {"QueryExecutionId": f"offline-view-{index + 1}"}
                for index in range(len(self.queries))
            ]:
                if operation == "GetQueryResults":
                    return {
                        "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
                        "UpdateCount": 0,
                    }
                return {
                    "QueryExecution": {
                        "QueryExecutionId": params["QueryExecutionId"],
                        "Status": {
                            "State": "FAILED" if self.scenario == "query_failure" else "SUCCEEDED"
                        },
                        "StatementType": "DDL",
                        "Statistics": {"DataScannedInBytes": 0},
                    }
                }
        raise ValueError("Unexpected SDK operation: " + service + ":" + operation)

    def verify(self, manifest: dict[str, Any], results: dict[str, Any]) -> None:
        if self.violations:
            raise ValueError("Unexpected simulated API request: " + repr(self.violations))
        count = {
            "create": 2,
            "replace": 2,
            "query_failure": 1,
            "table_collision": 0,
            "full_refresh_collision": 0,
        }[self.scenario]
        if len(self.queries) != count:
            raise ValueError("Unexpected view submission count")
        schema = self.plan["binding"]["source_schema"]
        for name, sql in zip(VIEWS, self.queries, strict=False):
            compiled = manifest["nodes"]["model.urban_geo_athena." + name]["compiled_code"]
            expected = f'create or replace view "AwsDataCatalog"."{schema}"."{name}" as ' + compiled
            if normalized_sql(sql) != normalized_sql(expected):
                raise ValueError("Submitted SQL differs from the actual compiled model")
        statuses = {r["unique_id"].split(".")[-1]: r["status"] for r in results["results"]}
        expected_status = dict(
            zip(VIEWS, ("success", "success") if count == 2 else ("error", "skipped"), strict=True)
        )
        if statuses != expected_status:
            raise ValueError("Unexpected dbt model outcomes")
        if self.scenario in {"table_collision", "full_refresh_collision"} and not any(
            "Refusing to replace a physical table with a Gold view" in (r.get("message") or "")
            for r in results["results"]
        ):
            raise ValueError("Collision must fail through the explicit project guard")
        deleted = [
            call["parameters"]["TableName"]
            for call in self.calls
            if call["operation"] == "DeleteTableVersion"
        ]
        if deleted != (list(VIEWS) if self.scenario == "replace" else []):
            raise ValueError("Unexpected view version maintenance")


def check_views(project: Path, output: Path, scenario: str) -> dict[str, Any]:
    receipt = verify_project(project)
    if scenario not in SCENARIOS:
        raise ValueError("Unknown offline scenario")
    resolved = output.resolve()
    if (
        project.is_symlink()
        or output.exists()
        or output.is_symlink()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
        or resolved == ARTIFACTS.resolve()
        or resolved.is_relative_to(project.resolve())
        or project.resolve().is_relative_to(resolved)
    ):
        raise ValueError("Use fresh artifacts output separate from the prepared project")
    pins = {"dbt-core": "1.12.6", "dbt-athena": "1.11.1"} | tooling_sdk_pins()
    if any(version(name) != pin for name, pin in pins.items()):
        raise ValueError("Use the isolated locked analytics environment")
    plan = read_plan(
        deployment_name="urban-geo-test",
        account_id="123456789012",
        snapshot_id=receipt["binding"]["snapshot_id"],
        source_run_sha256=receipt["binding"]["source_run_sha256"],
    )
    if receipt["binding"] != plan["binding"]:
        raise ValueError(
            "Offline adapter scenario requires the synthetic urban_geo_test_m4 database"
        )
    output.mkdir(parents=True)
    profiles = output / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(json.dumps(plan["profile"]) + "\n", encoding="utf-8")
    fake = ViewScenario(plan, scenario)
    attempts: list[str] = []
    clients: list[Any] = []

    def denied(*args: Any, **kwargs: Any) -> Any:
        attempts.append("network request blocked")
        raise OSError("Network disabled during offline dbt view check")

    # Clear ambient credentials/configuration; only dummy keys reach real SDK clients.
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
    with patch.dict(os.environ, env, clear=True):
        from boto3.session import Session
        from botocore.client import BaseClient
        from botocore.stub import Stubber
        from dbt.cli.main import dbtRunner

        original_client = Session.client
        original_call = BaseClient._make_api_call

        def owned_client(session: Any, *args: Any, **kwargs: Any) -> Any:
            client = original_client(session, *args, **kwargs)
            clients.append(client)
            return client

        def simulated_call(client: Any, operation: str, params: dict[str, Any]) -> Any:
            response = fake.reply(client.meta.service_model.service_name, operation, params)
            method = client.meta.method_to_api_mapping
            python_name = next(name for name, api in method.items() if api == operation)
            # Real botocore validates response and serializes/validates the request;
            # Stubber intercepts before HTTP. The fake boundary above checks scope.
            with Stubber(client) as stub:
                stub.add_response(python_name, response)
                value = original_call(client, operation, params)
                stub.assert_no_pending_responses()
                return value

        try:
            with (
                patch.object(Session, "client", owned_client),
                patch.object(BaseClient, "_make_api_call", simulated_call),
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
                        "run",
                        "--project-dir",
                        str(project),
                        "--profiles-dir",
                        str(profiles),
                        "--target-path",
                        str(output / "target"),
                        "--select",
                        "grid_proximity release_proximity",
                        *(["--full-refresh"] if scenario == "full_refresh_collision" else []),
                    ]
                )
        finally:
            for client in reversed(clients):
                client.close()
            (output / "trace.json").write_text(
                json.dumps(fake.calls, indent=2) + "\n", encoding="utf-8"
            )
    if attempts or result.success != (scenario in {"create", "replace"}):
        raise ValueError("Offline dbt invocation failed unexpectedly: " + str(result.exception))
    manifest = json.loads((output / "target/manifest.json").read_text(encoding="utf-8"))
    results = json.loads((output / "target/run_results.json").read_text(encoding="utf-8"))
    counts = check_manifest(manifest, project, receipt["binding"])
    fake.verify(manifest, results)
    check_compiled_models(manifest, receipt["binding"])
    verify_project(project)
    report = {
        "scenario": scenario,
        "tooling": pins,
        **counts,
        "binding": receipt["binding"],
        "canonical_sql_sha256": receipt["canonical_sql_sha256"],
        "simulated_query_submissions": len(fake.queries),
        "simulated_sdk_calls": len(fake.calls),
        "observed_operations": sorted({c["service"] + ":" + c["operation"] for c in fake.calls}),
        "clients_closed": len(clients),
        "blocked_network_attempts": len(attempts),
        "aws_requests": 0,
        "spark_started": False,
        "data_tests_executed": 0,
        "live_build_verified": False,
        "live_execution_control_verified": False,
        "scope": (
            "Real dbt run/materialization against synthetic SDK responses; "
            "no AWS SQL execution or live timeout/recovery proof"
        ),
    }
    (output / "check.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            check_views(args.project.resolve(), args.output.resolve(), args.scenario), indent=2
        )
    )


if __name__ == "__main__":
    main()
