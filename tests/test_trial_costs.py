"""Pinned regional price selection and conservative first-trial arithmetic; no network."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("trial_costs", ROOT / "scripts/trial_costs.py")
costs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(costs)


@pytest.fixture
def case(tmp_path, monkeypatch):
    folder = tmp_path / "pricing"
    folder.mkdir()
    monkeypatch.setattr(costs, "PROJECT_ROOT", tmp_path)
    values = ("0.44", "5", "0.023", "0.000005", "0.0000004", "0.54", "0.028", "0.000001", "0.00001")
    catalogs = {
        s: {
            "offerCode": s,
            "publicationDate": "2026-11-10T00:00:00Z",
            "version": "20261110000000",
            "products": {},
            "terms": {"OnDemand": {}},
        }
        for s in costs.SERVICES
    }
    for (name, (service, usage, unit)), price in zip(costs.METERS.items(), values, strict=True):
        attrs = {"regionCode": costs.REGION, "servicecode": service, "usagetype": "EUN1-" + usage}
        if name == "glue_dpu_hour":
            attrs.update(group="ETL Job run", operation="Jobrun")
        if name == "s3_gb_month":
            attrs.update(storageClass="General Purpose", volumeType="Standard")
        catalogs[service]["products"][name] = {"sku": name, "attributes": attrs}
        catalogs[service]["terms"]["OnDemand"][name] = {
            "offer": {
                "effectiveDate": "2026-11-01T00:00:00Z",
                "priceDimensions": {
                    "rate": {
                        "unit": unit,
                        "beginRange": "0",
                        "endRange": "Inf",
                        "pricePerUnit": {"USD": price},
                    }
                },
            }
        }

    def save():
        index = {}
        for service, value in catalogs.items():
            content = json.dumps(value).encode()
            (folder / (service + ".json")).write_bytes(content)
            index[service] = {
                "url": f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/eu-north-1/index.json",
                "sha256": hashlib.sha256(content).hexdigest(),
                "bytes": len(content),
                "publication_date": value["publicationDate"],
                "version": value["version"],
                "retrieved_at_utc": "2026-11-11T00:00:00Z",
            }
        return save_index(index)

    def save_index(index):
        content = json.dumps(index).encode()
        (folder / "index.json").write_bytes(content)
        return hashlib.sha256(content).hexdigest()

    return folder, catalogs, save, save_index, tmp_path / "artifacts" / "quote.json"


def test_realistic_decimal_rate_scenarios_round_up_without_claiming_authority(case):
    folder, _, save, _, output = case
    value = costs.prepare(folder, save(), output)
    assert value == json.loads(output.read_text())
    assert value["scenarios"]["ten_minute_job"]["glue_compute_usd"] == "0.146667"
    assert value["scenarios"]["ten_minute_job"]["rounded_up_cents_usd"] == "0.27"
    assert (
        value["scenarios"]["seventeen_minute_supervision_window"]["rounded_up_cents_usd"] == "0.38"
    )
    assert value["ancillary_usd"]["athena_three_scans"] == "0.001575"
    assert value["assumptions"]["athena_decimal_mb_rounded_up_per_scan"] == 105
    assert not value["spending_authorized"] and not value["hard_cost_cap"]
    assert not value["credits_and_free_allowances_applied"]
    assert not value["tax_and_currency_conversion_included"]
    assert not value["transfer_and_unmodeled_services_included"]
    assert value["aws_requests"] == 0


@pytest.mark.parametrize("pin", [None, "", "0" * 64, "G" * 64])
def test_invalid_or_wrong_index_pin_fails_before_output(case, pin):
    folder, _, save, _, output = case
    save()
    with pytest.raises(ValueError):
        costs.prepare(folder, pin, output)
    assert not output.parent.exists()


def test_changed_catalog_bytes_are_rejected(case):
    folder, _, save, _, output = case
    pin = save()
    path = folder / "AWSGlue.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError):
        costs.prepare(folder, pin, output)
    assert not output.exists()


@pytest.mark.parametrize("kind", ["region", "gen2", "flex", "unit", "storage_class", "duplicate"])
def test_wrong_or_ambiguous_product_is_not_a_fallback(case, kind):
    folder, catalogs, save, _, output = case
    service, meter = (
        ("AmazonS3", "s3_gb_month") if kind == "storage_class" else ("AWSGlue", "glue_dpu_hour")
    )
    catalog = catalogs[service]
    attrs = catalog["products"][meter]["attributes"]
    dimension = catalog["terms"]["OnDemand"][meter]["offer"]["priceDimensions"]["rate"]
    if kind == "region":
        attrs["regionCode"] = "eu-west-1"
    elif kind == "gen2":
        attrs["usagetype"] += "-Gen2"
    elif kind == "flex":
        attrs["group"] = "ETL Flex Job Run"
    elif kind == "unit":
        dimension["unit"] = "M-DPU-Hour"
    elif kind == "storage_class":
        attrs["storageClass"] = "Intelligent-Tiering"
    else:
        catalog["products"]["duplicate"] = copy.deepcopy(catalog["products"][meter])
        catalog["terms"]["OnDemand"]["duplicate"] = copy.deepcopy(
            catalog["terms"]["OnDemand"][meter]
        )
    with pytest.raises(ValueError):
        costs.prepare(folder, save(), output)
    assert not output.exists()


@pytest.mark.parametrize(
    "price",
    [
        {"EUR": "0.44"},
        {"USD": "0.44", "EUR": "0.40"},
        {"USD": "NaN"},
        {"USD": "Infinity"},
        {"USD": "-0.44"},
        {"USD": "0"},
        {"USD": 0.44},
        {"USD": "invalid"},
    ],
)
def test_invalid_currency_or_rate_is_rejected(case, price):
    folder, catalogs, save, _, output = case
    catalogs["AWSGlue"]["terms"]["OnDemand"]["glue_dpu_hour"]["offer"]["priceDimensions"]["rate"][
        "pricePerUnit"
    ] = price
    with pytest.raises(ValueError):
        costs.prepare(folder, save(), output)
    assert not output.exists()


def test_higher_storage_tier_and_s3_table_requests_do_not_reduce_quote(case):
    folder, catalogs, save, _, _ = case
    catalog = catalogs["AmazonS3"]
    dimensions = catalog["terms"]["OnDemand"]["s3_gb_month"]["offer"]["priceDimensions"]
    dimensions["cheaper"] = copy.deepcopy(dimensions["rate"])
    dimensions["cheaper"].update(beginRange="51200", pricePerUnit={"USD": "0.001"})
    catalog["products"]["s3_table"] = copy.deepcopy(catalog["products"]["s3_put_list"])
    catalog["products"]["s3_table"]["attributes"]["usagetype"] = "EUN1-Tables-Requests-Tier1"
    catalog["terms"]["OnDemand"]["s3_table"] = copy.deepcopy(
        catalog["terms"]["OnDemand"]["s3_put_list"]
    )
    value = costs.estimate(folder, save())
    assert value["meters"]["s3_gb_month"]["usd_per_unit"] == "0.023"
    assert value["meters"]["s3_put_list"]["sku"] == "s3_put_list"


@pytest.mark.parametrize(
    "change", ["url", "bytes", "publication_date", "version", "missing_service"]
)
def test_index_must_match_raw_source_identity(case, change):
    folder, _, save, save_index, output = case
    save()
    index = json.loads((folder / "index.json").read_text())
    if change == "missing_service":
        del index["AWSGlue"]
    else:
        index["AWSGlue"][change] = 1 if change == "bytes" else "different"
    with pytest.raises(ValueError):
        costs.prepare(folder, save_index(index), output)
    assert not output.exists()


def test_informational_selected_rates_cannot_override_pinned_raw_prices(case):
    folder, _, save, save_index, _ = case
    save()
    index = json.loads((folder / "index.json").read_text())
    index["AWSGlue"]["selected"] = [{"usd": "0.000001"}]
    assert (
        costs.estimate(folder, save_index(index))["meters"]["glue_dpu_hour"]["usd_per_unit"]
        == "0.44"
    )


def test_existing_quote_is_preserved(case):
    folder, _, save, _, output = case
    output.parent.mkdir()
    output.write_text("preserve")
    with pytest.raises(ValueError):
        costs.prepare(folder, save(), output)
    assert output.read_text() == "preserve"


def test_quote_cannot_be_written_inside_catalogs(case):
    folder, _, save, _, _ = case
    with pytest.raises(ValueError):
        costs.prepare(folder, save(), folder / "quote.json")
    assert not (folder / "quote.json").exists()


@pytest.mark.parametrize("value", ["2026-12-01T00:00:00Z", "2026-11-01T00:00:00", "invalid"])
def test_future_or_unqualified_effective_date_is_not_used(case, value):
    folder, catalogs, save, _, output = case
    catalogs["AWSGlue"]["terms"]["OnDemand"]["glue_dpu_hour"]["offer"]["effectiveDate"] = value
    with pytest.raises(ValueError):
        costs.prepare(folder, save(), output)
    assert not output.exists()


def test_source_publication_cannot_follow_retrieval(case):
    folder, catalogs, save, _, output = case
    catalogs["AWSGlue"]["publicationDate"] = "2026-12-01T00:00:00Z"
    with pytest.raises(ValueError):
        costs.prepare(folder, save(), output)
    assert not output.exists()
