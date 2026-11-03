"""Local planning, shared SQL arithmetic and read-only workgroup preflight; no SDK/network."""

import json
import sqlite3
from unittest.mock import Mock

import pytest
from athena_plan_fixtures import ATHENA as athena
from athena_plan_fixtures import OPTIONS, workgroup


@pytest.mark.parametrize(
    "field,value",
    [
        ("deployment_name", "unsafe;sql"),
        ("account_id", "wrong"),
        ("region", "us-east-1"),
        ("region", "eu-west-1"),
        ("snapshot_id", True),
        ("source_run_sha256", "a" * 63 + "'"),
        ("max_query_scan_bytes", True),
        ("max_query_scan_bytes", athena.MIN_SCAN_BYTES - 1),
        ("max_query_scan_bytes", athena.MAX_SCAN_BYTES + 1),
        ("max_query_scan_bytes", 10485760.5),
    ],
)
def test_invalid_plan_fails_before_creating_output(tmp_path, monkeypatch, field, value):
    monkeypatch.setattr(athena, "ARTIFACTS", tmp_path)
    with pytest.raises(ValueError):
        athena.prepare(tmp_path / "fresh", **(OPTIONS | {field: value}))
    assert not list(tmp_path.iterdir())


def test_plan_profile_and_request_share_one_target_and_remain_unverified():
    plan = athena.read_plan(**OPTIONS)
    profile = plan["profile"]["urban_geo_athena"]["outputs"]["prepared_athena"]
    request = plan["request_template"]
    assert profile["work_group"] == request["WorkGroup"] == "urban-geo-test-m4"
    assert profile["schema"] == request["QueryExecutionContext"]["Database"] == "urban_geo_test_m4"
    assert profile["s3_staging_dir"] == request["ResultConfiguration"]["OutputLocation"]
    assert profile["threads"] == 1 and profile["num_retries"] == profile["num_boto3_retries"] == 0
    assert not any("secret" in key or "access_key" in key for key in profile)
    assert request["ResultReuseConfiguration"]["ResultReuseByAgeConfiguration"]["Enabled"] is False
    assert plan["created_workgroup_state"] == "DISABLED" and not plan["execution_authorized"]
    assert plan["provenance_verified"] is False
    assert plan["terraform_inputs"]["enable_athena_reader_policy"] is True
    assert (
        "ClientRequestToken" not in request
    )  # A future runner must persist its own attempt token.


def test_preparation_is_fresh_private_output_and_does_not_construct_sdk(tmp_path, monkeypatch):
    monkeypatch.setattr(athena, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(athena, "create_athena_client", Mock(side_effect=AssertionError("no SDK")))
    output = tmp_path / "prepared"
    plan = athena.prepare(output, **OPTIONS)
    assert (output / "query.sql").read_text() == plan["request_template"]["QueryString"]
    assert json.loads((output / "profiles.yml").read_text()) == plan["profile"]
    before = {p: p.read_bytes() for p in output.iterdir()}
    with pytest.raises(ValueError, match="fresh"):
        athena.prepare(output, **OPTIONS)
    assert all(p.read_bytes() == data for p, data in before.items())
    with pytest.raises(ValueError, match="artifacts"):
        athena.prepare(tmp_path.parent / "outside", **OPTIONS)


def test_complete_query_preserves_weighted_gold_arithmetic():
    plan = athena.read_plan(**OPTIONS)
    sql = plan["request_template"]["QueryString"]
    # SQLite checks our CTE assembly/arithmetic, not Athena's version-travel implementation.
    sql = sql.replace(
        'FROM "AwsDataCatalog"."urban_geo_test_m4"."building_proximity"\nFOR VERSION AS OF 123',
        "FROM source_facts",
    )
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE source_facts (source_release, area_id, grid_id, within_threshold)")
        db.executemany(
            "INSERT INTO source_facts VALUES ('r1', 'pilot', ?, ?)",
            [("g1", 1), ("g1", 0), ("g1", 0), ("g2", 0), ("g3", 1), ("g3", 0)],
        )
        row = db.execute(sql).fetchone()
    assert row == ("r1", "pilot", "a" * 64, 6, 2, 2 / 6)


def test_preflight_reads_only_one_workgroup_without_claiming_identity_or_provenance():
    plan = athena.read_plan(**OPTIONS)
    client = Mock()
    client.get_work_group.return_value = {"WorkGroup": workgroup(plan)}
    result = athena.preflight_workgroup(client, plan)
    assert result == {
        "workgroup_settings_verified": True,
        "caller_identity_verified": False,
        "provenance_verified": False,
        "queries_submitted": 0,
    }
    assert client.mock_calls == [("get_work_group", (), {"WorkGroup": "urban-geo-test-m4"})]


@pytest.mark.parametrize(
    "path,value",
    [
        (("Name",), "primary"),
        (("State",), "DISABLED"),
        (("Configuration", "EnforceWorkGroupConfiguration"), False),
        (("Configuration", "EnforceWorkGroupConfiguration"), 1),
        (("Configuration", "BytesScannedCutoffPerQuery"), 1073741824),
        (("Configuration", "EngineVersion", "SelectedEngineVersion"), "AUTO"),
        (("Configuration", "EngineVersion", "EffectiveEngineVersion"), "Athena engine version 2"),
        (("Configuration", "ResultConfiguration", "OutputLocation"), "s3://other/results/"),
        (("Configuration", "ResultConfiguration", "ExpectedBucketOwner"), "999999999999"),
        (
            ("Configuration", "ResultConfiguration", "EncryptionConfiguration", "EncryptionOption"),
            "SSE_KMS",
        ),
        (
            ("Configuration", "ResultConfiguration", "AclConfiguration"),
            {"S3AclOption": "BUCKET_OWNER_FULL_CONTROL"},
        ),
        (("Configuration", "ManagedQueryResultsConfiguration"), {"Enabled": True}),
        (("Configuration", "RequesterPaysEnabled"), True),
        (("Configuration", "ExecutionRole"), "unexpected-role"),
        (("Configuration", "AdditionalConfiguration"), "{}"),
        (("Configuration", "EngineConfiguration"), {"MaxConcurrentDpus": 4}),
    ],
)
def test_disabled_drifted_or_unbounded_workgroup_is_rejected(path, value):
    plan = athena.read_plan(**OPTIONS)
    response = workgroup(plan)
    target = response
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    client = Mock()
    client.get_work_group.return_value = {"WorkGroup": response}
    with pytest.raises(ValueError):
        athena.preflight_workgroup(client, plan)
    client.start_query_execution.assert_not_called()


@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_sdk_factory_rejects_wrong_region_before_loading_packages(monkeypatch, region):
    monkeypatch.setattr(athena, "tooling_sdk_pins", lambda: pytest.fail("No SDK resolution"))
    with pytest.raises(ValueError, match="eu-north-1"):
        athena.create_athena_client(region=region)
