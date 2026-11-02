"""View metadata must bind to the exact model and immutable source snapshot."""

import base64
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from dbt_catalog_fixtures import CATALOG, encoded, fake_view
from dbt_control_fixtures import case as control_case


@pytest.fixture
def plan(tmp_path, monkeypatch):
    return control_case(tmp_path, monkeypatch).control.plan()


@pytest.mark.parametrize("name", ["grid_proximity", "release_proximity"])
def test_readback_retains_version_and_definition_hash_without_sdk_timestamps(plan, name):
    table, _ = fake_view(plan, name)
    table["UpdateTime"] = datetime.now(UTC)
    observed = CATALOG.view_state(table, plan, name)
    assert observed["name"] == name and observed["version_id"] == "1"
    assert len(observed["definition_sha256"]) == 64
    assert "UpdateTime" not in observed


@pytest.mark.parametrize(
    "bad",
    [
        "account",
        "database",
        "name",
        "table",
        "presto",
        "iceberg",
        "link",
        "federated",
        "multi",
        "lakeformation",
        "version",
        "location",
        "partitions",
        "columns",
        "expanded",
        "base64",
        "json",
        "duplicate_json",
        "oversized",
        "snapshot",
        "hash",
        "sql",
        "schema",
        "catalog",
        "encoded_columns",
        "extra_field",
        "missing_sql",
        "invalid_catalog",
        "invalid_columns",
        "invalid_sql",
    ],
)
def test_wrong_or_malformed_view_is_never_accepted(plan, bad):
    table, definition = fake_view(plan, "grid_proximity")
    if bad in {"account", "database", "name", "table"}:
        table[
            {
                "account": "CatalogId",
                "database": "DatabaseName",
                "name": "Name",
                "table": "TableType",
            }[bad]
        ] = "wrong"
    elif bad == "presto":
        table["Parameters"] = {}
    elif bad == "iceberg":
        table["Parameters"]["metadata_location"] = "s3://other/metadata.json"
    elif bad in {"link", "federated", "multi", "lakeformation"}:
        table[
            {
                "link": "TargetTable",
                "federated": "FederatedTable",
                "multi": "IsMultiDialectView",
                "lakeformation": "IsRegisteredWithLakeFormation",
            }[bad]
        ] = True
    elif bad == "version":
        del table["VersionId"]
    elif bad == "location":
        table["StorageDescriptor"]["Location"] = "s3://other/data"
    elif bad == "partitions":
        table["PartitionKeys"] = [{"Name": "area_id", "Type": "string"}]
    elif bad == "columns":
        table["StorageDescriptor"]["Columns"].reverse()
    elif bad == "expanded":
        table["ViewExpandedText"] = "SELECT * FROM other"
    elif bad == "base64":
        table["ViewOriginalText"] = "/* Presto View: !bad! */"
    elif bad in {"json", "duplicate_json"}:
        text = "{" if bad == "json" else '{"originalSql": "one", "originalSql": "two"}'
        table["ViewOriginalText"] = (
            "/* Presto View: " + base64.b64encode(text.encode()).decode() + " */"
        )
    elif bad == "oversized":
        table["ViewOriginalText"] = "x" * (65536 + 1)
    else:
        if bad == "snapshot":
            definition["originalSql"] = definition["originalSql"].replace("OF 123", "OF 456")
        elif bad == "hash":
            definition["originalSql"] = definition["originalSql"].replace("a" * 64, "b" * 64)
        elif bad == "sql":
            definition["originalSql"] = definition["originalSql"].replace(
                "COUNT(*)", "COUNT(DISTINCT building_id)"
            )
        elif bad in {"schema", "catalog"}:
            definition[bad] = "wrong"
        elif bad == "encoded_columns":
            definition["columns"][0]["type"] = "bigint"
        elif bad == "extra_field":
            definition["path"] = []
        elif bad == "missing_sql":
            del definition["originalSql"]
        elif bad == "invalid_catalog":
            definition["catalog"] = None
        elif bad == "invalid_columns":
            definition["columns"] = None
        elif bad == "invalid_sql":
            definition["originalSql"] = None
        table["ViewOriginalText"] = encoded(definition)
    with pytest.raises(ValueError):
        CATALOG.view_state(table, plan, "grid_proximity")


def test_both_reads_use_exact_same_account_names_and_stop_on_first_failure(plan):
    client = Mock()
    client.get_table.side_effect = lambda **kw: {"Table": fake_view(plan, kw["Name"])[0]}
    assert set(CATALOG.observe_views(client, plan)) == {"grid_proximity", "release_proximity"}
    assert [c.kwargs for c in client.get_table.call_args_list] == [
        {"CatalogId": "123456789012", "DatabaseName": "urban_geo_test_m4", "Name": name}
        for name in ("grid_proximity", "release_proximity")
    ]
    client.reset_mock()
    client.get_table.side_effect = ValueError("missing view")
    with pytest.raises(ValueError):
        CATALOG.observe_views(client, plan)
    client.get_table.assert_called_once()


@pytest.mark.parametrize(
    "sql", ["SELECT 'A--B/*C*/' AS value", "select 'A--B/*C*/' as value -- comment"]
)
def test_lexical_comparison_preserves_literals_and_accepts_formatting(sql):
    assert CATALOG.sql_tokens(sql) == CATALOG.sql_tokens("select 'A--B/*C*/' AS value")
    assert CATALOG.sql_tokens(sql) != CATALOG.sql_tokens("select 'a--b/*c*/' AS value")
    assert CATALOG.sql_tokens('SELECT "SELECT"') != CATALOG.sql_tokens("SELECT SELECT")
