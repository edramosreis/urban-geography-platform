"""Retain bounded service metadata independently from conservative view acceptance."""

import importlib.util
import json
import sys
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from dbt_catalog_fixtures import ROOT, encoded, fake_view
from dbt_control_fixtures import case as control_case

for dependency in ("aws_login_bridge", "athena_preflight"):
    if dependency not in sys.modules:
        dependency_spec = importlib.util.spec_from_file_location(
            dependency, ROOT / "scripts" / (dependency + ".py")
        )
        module = importlib.util.module_from_spec(dependency_spec)
        sys.modules[dependency] = module
        dependency_spec.loader.exec_module(module)

if "dbt_view_capture" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "dbt_view_capture", ROOT / "scripts/dbt_view_capture.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["dbt_view_capture"] = module
    spec.loader.exec_module(module)
capture = sys.modules["dbt_view_capture"]

CALLER = {
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/urban-geo-test-dbt-view-builder/dbt-builder-"
    + "a" * 32,
    "UserId": "AROA" + "B" * 17 + ":dbt-builder-" + "a" * 32,
}


@pytest.fixture
def case(tmp_path, monkeypatch):
    base = control_case(tmp_path, monkeypatch)
    monkeypatch.setattr(capture, "ARTIFACTS", tmp_path)
    plan = base.control.plan()
    tables = {name: fake_view(plan, name)[0] for name in capture.VIEWS}
    client = Mock()
    client.get_table.side_effect = lambda **kwargs: {
        "Table": tables[kwargs["Name"]],
        "ResponseMetadata": {"HTTPHeaders": {"do-not-retain": "header"}},
    }
    return plan, tables, client, tmp_path / "capture"


def run(case):
    plan, _, client, output = case
    return capture.capture_views(client, plan, "a" * 64, CALLER, output)


def test_exact_two_reads_preserve_text_and_normalize_sdk_dates_without_http_headers(case):
    plan, tables, client, output = case
    tables["grid_proximity"]["UpdateTime"] = datetime(2026, 11, 4, tzinfo=UTC)
    original = tables["grid_proximity"]["ViewOriginalText"]
    assert len(run(case)) == 64
    record = json.loads((output / "grid_proximity.json").read_bytes())
    assert record["table"]["ViewOriginalText"] == original
    assert record["table"]["UpdateTime"] == "2026-11-04T00:00:00+00:00"
    assert "ResponseMetadata" not in record and "do-not-retain" not in json.dumps(record)
    assert client.get_table.call_args_list[0].kwargs == {
        "CatalogId": "123456789012",
        "DatabaseName": "urban_geo_test_m4",
        "Name": "grid_proximity",
    }
    assert client.get_table.call_count == 2
    assert capture.load_capture(output, plan, "a" * 64, CALLER) == {
        "metadata_captured": True,
        "view_metadata_compatibility": "matches_current_checker",
    }
    manifest = json.loads((output / capture.MANIFEST).read_bytes())
    assert manifest["view_catalog_verified"] is False and manifest["result_verified"] is False


@pytest.mark.parametrize("defect", ["encoding", "new_field", "wrong_sql", "physical_table"])
def test_unsupported_view_formats_are_preserved_and_require_review(case, defect):
    plan, tables, _, output = case
    table = tables["grid_proximity"]
    if defect == "encoding":
        table["ViewOriginalText"] = "unrecognised encoding"
    elif defect == "new_field":
        table["ViewDefinition"] = {"Dialect": "future-format"}
    elif defect == "physical_table":
        table["TableType"] = "EXTERNAL_TABLE"
    else:
        _, definition = fake_view(plan, "grid_proximity")
        definition["originalSql"] = "SELECT 1"
        table["ViewOriginalText"] = encoded(definition)
    run(case)
    assert json.loads((output / "grid_proximity.json").read_bytes())["table"] == table
    assert (
        capture.load_capture(output, plan, "a" * 64, CALLER)["view_metadata_compatibility"]
        == "requires_review"
    )


@pytest.mark.parametrize("defect", ["identity", "oversize", "nan", "naive_date", "sdk_error"])
def test_second_read_failure_retains_first_record_and_has_no_complete_manifest(case, defect):
    _, tables, client, output = case
    table = tables["release_proximity"]
    if defect == "identity":
        table["Name"] = "other"
    elif defect == "oversize":
        table["ViewOriginalText"] = "x" * capture.MAX_CAPTURE_BYTES
    elif defect == "nan":
        table["extra"] = float("nan")
    elif defect == "naive_date":
        table["UpdateTime"] = datetime(2026, 11, 4)
    else:
        original = client.get_table.side_effect

        def failed(**kwargs):
            if kwargs["Name"] == "release_proximity":
                raise RuntimeError("sentinel-provider-error")
            return original(**kwargs)

        client.get_table.side_effect = failed
    with pytest.raises((ValueError, RuntimeError)):
        run(case)
    assert (output / "grid_proximity.json").is_file()
    assert not (output / capture.MANIFEST).exists()
    failure = json.loads((output / "failure.json").read_bytes())
    assert failure["records_written"] == (
        2 if defect == "identity" else 1
    ) and "sentinel" not in json.dumps(failure)
    assert client.get_table.call_count == 2


@pytest.mark.parametrize("defect", ["existing", "outside", "caller", "pin"])
def test_bad_preparation_never_calls_the_service(case, tmp_path, defect):
    plan, _, client, output = case
    caller, pin = dict(CALLER), "a" * 64
    if defect == "existing":
        output.mkdir()
    elif defect == "outside":
        output = tmp_path.parent / "outside-capture"
    elif defect == "caller":
        caller["Arn"] = caller["Arn"].replace("-dbt-view-builder/", "-athena-reader/")
    else:
        pin = "invalid"
    with pytest.raises(ValueError):
        capture.capture_views(client, plan, pin, caller, output)
    client.get_table.assert_not_called()


def test_changed_raw_record_and_forged_compatibility_are_rejected(case):
    plan, _, _, output = case
    run(case)
    manifest = output / capture.MANIFEST
    value = json.loads(manifest.read_bytes())
    value["records"]["grid_proximity"]["compatibility"] = "requires_review"
    manifest.write_bytes(capture.canonical(value))
    with pytest.raises(ValueError):
        capture.load_capture(output, plan, "a" * 64, CALLER)
