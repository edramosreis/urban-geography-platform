"""Metadata discovery and view-version mutation stay narrow and finite."""

import json
from datetime import datetime

import pytest
from dbt_broker_fixtures import broker_case, message


@pytest.fixture
def case(tmp_path, monkeypatch):
    return broker_case(tmp_path, monkeypatch)


def test_discovery_projects_only_the_dedicated_database_and_three_relations(case):
    _, clients, broker = case
    clients["glue"].get_databases.return_value = {
        "DatabaseList": [{"Name": broker.database, "CreateTime": datetime.now()}]
    }
    assert broker.dispatch(message(broker, "glue", "get_databases")) == {
        "DatabaseList": [{"Name": broker.database}]
    }
    clients["glue"].get_tables.return_value = {
        "TableList": [
            {
                "Name": "building_proximity",
                "TableType": "EXTERNAL_TABLE",
                "CreateTime": datetime.now(),
            }
        ]
    }
    result = broker.dispatch(
        message(
            broker, "glue", "get_tables", CatalogId=broker.account, DatabaseName=broker.database
        )
    )
    assert "CreateTime" not in json.dumps(result)
    clients["glue"].get_tables.assert_called_once_with(
        CatalogId=broker.account, DatabaseName=broker.database, MaxResults=10
    )


def test_only_observed_versions_older_than_the_four_kept_versions_can_be_removed(case):
    _, clients, broker = case
    view = "grid_proximity"
    clients["glue"].get_table_versions.return_value = {
        "TableVersions": [
            {
                "VersionId": str(i),
                "Table": {
                    "Name": view,
                    "VersionId": str(i),
                    "StorageDescriptor": {
                        "Location": "s3://placeholder/view",
                        "Unused": "not-forwarded",
                    },
                },
            }
            for i in range(1, 6)
        ]
    }
    versions = broker.dispatch(
        message(broker, "glue", "get_table_versions", DatabaseName=broker.database, TableName=view)
    )
    assert "Unused" not in json.dumps(versions)
    broker.dispatch(
        message(
            broker,
            "glue",
            "delete_table_version",
            CatalogId=broker.account,
            DatabaseName=broker.database,
            TableName=view,
            VersionId="1",
        )
    )
    clients["glue"].delete_table_version.assert_called_once()
    with pytest.raises(ValueError):
        broker.dispatch(
            message(
                broker,
                "glue",
                "delete_table_version",
                CatalogId=broker.account,
                DatabaseName=broker.database,
                TableName=view,
                VersionId="5",
            )
        )
    assert clients["glue"].delete_table_version.call_count == 1 and broker.failed


@pytest.mark.parametrize(
    "kind",
    [
        "unknown_database",
        "extra_table",
        "page",
        "version_page",
        "source_mutation",
        "unknown_service",
        "sequence",
    ],
)
def test_unexpected_metadata_is_refused_and_poisoned(case, kind):
    _, clients, broker = case
    request = message(broker, "glue", "get_databases")
    if kind == "unknown_database":
        clients["glue"].get_databases.return_value = {"DatabaseList": [{"Name": "other"}]}
    elif kind == "extra_table":
        request = message(
            broker, "glue", "get_tables", CatalogId=broker.account, DatabaseName=broker.database
        )
        clients["glue"].get_tables.return_value = {
            "TableList": [{"Name": "building_proximity"}, {"Name": "other"}]
        }
    elif kind == "page":
        clients["glue"].get_databases.return_value = {
            "DatabaseList": [{"Name": broker.database}],
            "NextToken": "more",
        }
    elif kind == "version_page":
        request = message(
            broker,
            "glue",
            "get_table_versions",
            DatabaseName=broker.database,
            TableName="grid_proximity",
        )
        clients["glue"].get_table_versions.return_value = {"NextToken": "more"}
    elif kind == "source_mutation":
        request = message(
            broker,
            "glue",
            "delete_table_version",
            CatalogId=broker.account,
            DatabaseName=broker.database,
            TableName="building_proximity",
            VersionId="1",
        )
    elif kind == "unknown_service":
        request["service"] = "s3"
    else:
        request["sequence"] = 99
    with pytest.raises(ValueError):
        broker.dispatch(request)
    assert broker.failed
    clients["glue"].delete_table_version.assert_not_called()
    with pytest.raises(ValueError):
        broker.dispatch(message(broker, "sts", "get_caller_identity"))


def test_repeating_metadata_has_a_request_count_ceiling(case):
    _, clients, broker = case
    clients["sts"].get_caller_identity.return_value = {
        "Account": broker.account,
        "Arn": "synthetic",
        "UserId": "synthetic",
    }
    for _ in range(32):
        broker.dispatch(message(broker, "sts", "get_caller_identity"))
    with pytest.raises(ValueError, match="ceiling"):
        broker.dispatch(message(broker, "sts", "get_caller_identity"))
    assert clients["sts"].get_caller_identity.call_count == 32


def test_cached_terminal_metadata_removes_sdk_datetime_objects_for_ipc(case):
    from dbt_control_fixtures import metadata, params

    fixture, _, broker = case

    def observed(**kwargs):
        identity = kwargs["QueryExecutionId"]
        response = metadata(fixture.requests[identity], identity)
        response["QueryExecution"]["Status"]["CompletionDateTime"] = datetime.now()
        return response

    fixture.client.get_query_execution.side_effect = observed
    with fixture.control.session():
        fixture.control.start_query_execution(**params(fixture.control))
        response = broker.dispatch(
            message(broker, "athena", "get_query_execution", QueryExecutionId="view-1")
        )
    assert response["QueryExecution"]["Status"] == {"State": "SUCCEEDED"}
    json.dumps(response, allow_nan=False)
