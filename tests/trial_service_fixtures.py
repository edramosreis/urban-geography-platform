"""Read-only dedicated service replies, including unwanted payloads that must be omitted."""

import importlib.util
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from trial_cleanup_fixtures import ROOT, inventory

for name in ("glue_control", "trial_service_inventory"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
services = sys.modules["trial_service_inventory"]
SENTINEL = "sentinel-private-contents-must-not-be-saved"


def make_services(frozen):
    scope = inventory.cleanup_scope(frozen.path, frozen.pin)
    target, expected = scope["target"], scope["expected_services"]
    owner, deployment = target["account_id"], target["deployment_name"]
    database = {
        "Name": expected["database"],
        "CatalogId": owner,
        "LocationUri": "s3://" + scope["buckets"]["lake"]["name"] + "/warehouse/",
        "Parameters": {"password": SENTINEL},
    }
    tables = [
        {
            "Name": name,
            "TableType": "EXTERNAL_TABLE" if name == "building_proximity" else "VIRTUAL_VIEW",
        }
        for name in expected["tables"]
    ]
    job = {
        "Name": expected["job"],
        "Role": f"arn:aws:iam::{owner}:role/service-role/{deployment}-glue-writer",
        "Command": {
            "ScriptLocation": "s3://"
            + scope["buckets"]["lake"]["name"]
            + "/runtime/releases/"
            + scope["release_id"]
            + "/run_glue_s3_writer.py"
        },
        "DefaultArguments": {"password": SENTINEL},
    }
    run = {
        "Id": "jr_owned",
        "JobName": expected["job"],
        "JobRunState": "SUCCEEDED",
        "Arguments": {"password": SENTINEL},
    }
    group = {
        "Name": expected["workgroup"],
        "State": "DISABLED",
        "Configuration": {
            "EnforceWorkGroupConfiguration": True,
            "ResultConfiguration": {
                "OutputLocation": "s3://" + scope["buckets"]["results"]["name"] + "/queries/",
                "ExpectedBucketOwner": owner,
            },
        },
    }
    query = {
        "QueryExecutionId": "query-owned",
        "WorkGroup": expected["workgroup"],
        "QueryExecutionContext": {"Catalog": "AwsDataCatalog", "Database": expected["database"]},
        "Status": {"State": "SUCCEEDED", "StateChangeReason": SENTINEL},
        "Query": SENTINEL,
    }
    glue = Mock(spec=[*services.GLUE_READ_CALLS, "close"])
    athena = Mock(spec=[*services.ATHENA_READ_CALLS, "close"])
    glue.get_database.return_value = {"Database": database}
    glue.get_tables.return_value = {"TableList": tables}
    glue.get_job.return_value = {"Job": job}
    glue.get_job_runs.return_value = {"JobRuns": [run]}
    athena.get_work_group.return_value = {"WorkGroup": group}
    athena.list_query_executions.return_value = {"QueryExecutionIds": [query["QueryExecutionId"]]}
    athena.get_query_execution.return_value = {"QueryExecution": query}
    return SimpleNamespace(
        scope=scope,
        database=database,
        tables=tables,
        job=job,
        run=run,
        group=group,
        query=query,
        glue=glue,
        athena=athena,
    )
