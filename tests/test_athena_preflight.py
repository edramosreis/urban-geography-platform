"""Caller/target gates around existing row readback, with no Spark startup or AWS."""

import copy
from unittest.mock import Mock

import pytest
from athena_preflight_fixtures import CALLER, make_case
from athena_preflight_fixtures import PREFLIGHT as preflight


@pytest.fixture
def case(tmp_path, monkeypatch):
    return make_case(tmp_path, monkeypatch)


def run(case):
    plan, sts, glue, reader, options = case
    return preflight.verify_query_source(sts, glue, plan, **options)


def test_success_checks_identity_and_catalog_around_exact_target_reader(case):
    plan, sts, glue, reader, options = case
    order = Mock()
    order.attach_mock(sts, "sts")
    order.attach_mock(glue, "glue")
    order.attach_mock(reader, "reader")
    result = run(case)
    assert [c[0] for c in order.mock_calls] == [
        "sts.get_caller_identity",
        "glue.get_table",
        "reader",
        "glue.get_table",
        "sts.get_caller_identity",
    ]
    assert result["caller_identity_verified"] and result["source_readback_verified"]
    assert result["queries_submitted"] == 0 and result["execution_authorized"] is False
    assert result["result_verified"] is result["writer_job_identity_verified"] is False
    assert result["binding"] == plan["binding"]
    kwargs = reader.call_args.kwargs
    assert kwargs["catalog_name"] == "glue_catalog"
    assert kwargs["namespace"] == "urban_geo_test_m4"
    assert kwargs["expected_location"].endswith("/warehouse/building_proximity")
    assert kwargs["expected_settings"]["spark.sql.catalog.glue_catalog.glue.id"] == "123456789012"
    assert kwargs["session_factory"] is options["session_factory"]
    glue.get_table.assert_called_with(
        CatalogId="123456789012", DatabaseName="urban_geo_test_m4", Name="building_proximity"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("caller_arn", "arn:aws:iam::123456789012:root"),
        ("caller_arn", "arn:aws:sts::999999999999:assumed-role/reader/offline-session"),
        ("caller_arn", "arn:aws:sts::123456789012:assumed-role/*/offline-session"),
        ("caller_user_id", "AROA" + "A" * 17 + ":another-session"),
        ("writer_receipt_sha256", "bad-pin"),
    ],
)
def test_bad_expectations_fail_before_any_aws_call(case, field, value):
    case[-1][field] = value
    with pytest.raises(ValueError):
        run(case)
    case[1].get_caller_identity.assert_not_called()
    case[2].get_table.assert_not_called()
    case[3].assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("Account", "999999999999"),
        ("Arn", "arn:aws:iam::123456789012:root"),
        ("UserId", "AROA" + "B" * 17 + ":offline-session"),
    ],
)
def test_caller_drift_fails_before_catalog_or_reader(case, field, value):
    case[1].get_caller_identity.return_value[field] = value
    with pytest.raises(ValueError, match="caller"):
        run(case)
    case[2].get_table.assert_not_called()
    case[3].assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("CatalogId", "999999999999"),
        ("DatabaseName", "other"),
        ("Name", "other"),
        ("TableType", "VIRTUAL_VIEW"),
        ("VersionId", None),
        ("Parameters", {"table_type": "PARQUET"}),
        ("StorageDescriptor", {"Location": "s3://other/warehouse/building_proximity"}),
        ("TargetTable", {"Name": "other"}),
        ("FederatedTable", {"Identifier": "other"}),
        ("IsRegisteredWithLakeFormation", True),
        ("ViewOriginalText", "SELECT 1"),
        ("PartitionKeys", [{"Name": "source_release"}]),
    ],
)
def test_bad_catalog_target_stops_before_reader(case, field, value):
    case[2].get_table.return_value["Table"][field] = value
    with pytest.raises(ValueError):
        run(case)
    case[3].assert_not_called()


@pytest.mark.parametrize(
    "suffix",
    [
        "../other.metadata.json",
        "nested/other.metadata.json",
        "%2e%2e.metadata.json",
        "other.json",
        "",
    ],
)
def test_metadata_pointer_cannot_escape_flat_pilot_metadata_prefix(case, suffix):
    table = case[2].get_table.return_value["Table"]
    table["Parameters"]["metadata_location"] = (
        table["StorageDescriptor"]["Location"] + "/metadata/" + suffix
    )
    with pytest.raises(ValueError):
        run(case)
    case[3].assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("binding", {"source_schema": "other", "snapshot_id": 123, "source_run_sha256": "a" * 64}),
        ("readback_matches", 1),
        ("metadata_stable", False),
        ("location", "file:///local/table"),
        ("verified_spark_settings", {}),
        ("table", "local.test.building_proximity"),
    ],
)
def test_readback_must_bind_to_exact_query_and_aws_settings(case, field, value):
    case[3].return_value[field] = value
    with pytest.raises(ValueError, match="readback"):
        run(case)
    assert case[2].get_table.call_count == 1


@pytest.mark.parametrize("changed", ["catalog_version", "metadata_pointer", "caller"])
def test_changed_post_readback_observation_rejects_binding(case, changed):
    if changed == "caller":
        case[1].get_caller_identity.side_effect = [
            CALLER,
            CALLER | {"UserId": "AROA" + "B" * 17 + ":offline-session"},
        ]
    else:
        before = case[2].get_table.return_value
        after = copy.deepcopy(before)
        if changed == "catalog_version":
            after["Table"]["VersionId"] = "2"
        else:
            after["Table"]["Parameters"]["metadata_location"] = after["Table"]["Parameters"][
                "metadata_location"
            ].replace("00000", "00001")
        case[2].get_table.side_effect = [before, after]
    with pytest.raises(ValueError):
        run(case)


def test_reader_failure_propagates_without_claiming_source_verified(case):
    case[3].side_effect = ValueError("row mismatch")
    with pytest.raises(ValueError, match="row mismatch"):
        run(case)
    assert case[2].get_table.call_count == 1


def test_exact_iam_user_is_supported_without_role_session_matching():
    arn, user = "arn:aws:iam::123456789012:user/operators/reader", "AIDA" + "A" * 17
    assert preflight.expected_caller("123456789012", arn, user) == {
        "Account": "123456789012",
        "Arn": arn,
        "UserId": user,
    }


def test_reader_is_delegated_to_existing_verifier_without_reimplementing_row_logic(monkeypatch):
    module = Mock()
    module.verify_writer_output.return_value = {"status": "verified_snapshot_readback"}
    monkeypatch.setattr(preflight, "import_module", Mock(return_value=module))
    result = preflight.read_snapshot("input", "pin", expected_location="s3://pilot/table")
    preflight.import_module.assert_called_once_with("writer_readback")
    module.verify_writer_output.assert_called_once_with(
        "input", "pin", expected_location="s3://pilot/table"
    )
    assert result == {"status": "verified_snapshot_readback"}


def test_changed_query_plan_rejects_before_caller_lookup(case):
    case[0]["request_template"]["QueryString"] = "SELECT 1"
    with pytest.raises(ValueError, match="recipe"):
        run(case)
    case[1].get_caller_identity.assert_not_called()
    case[2].get_table.assert_not_called()
    case[3].assert_not_called()
