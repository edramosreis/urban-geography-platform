"""Price the fixed first-trial scenarios from pinned Stockholm AWS price catalogs."""

import argparse
import hashlib
import json
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from urban_geography.glue_input import PROJECT_ROOT

REGION = "eu-north-1"
SERVICES = ("AWSGlue", "AmazonAthena", "AmazonS3", "AmazonCloudWatch")
MAX_CATALOG_BYTES = 32 * 1024 * 1024
METERS = {
    "glue_dpu_hour": ("AWSGlue", "ETL-DPU-Hour", "DPU-Hour"),
    "athena_tb": ("AmazonAthena", "DataScannedInTB", "Terabytes"),
    "s3_gb_month": ("AmazonS3", "TimedStorage-ByteHrs", "GB-Mo"),
    "s3_put_list": ("AmazonS3", "Requests-Tier1", "Requests"),
    "s3_get": ("AmazonS3", "Requests-Tier2", "Requests"),
    "logs_ingest_gb": ("AmazonCloudWatch", "DataProcessing-Bytes", "GB"),
    "logs_gb_month": ("AmazonCloudWatch", "TimedStorage-ByteHrs", "GB-Mo"),
    "catalog_request": ("AWSGlue", "Catalog-Request", "Request"),
    "catalog_object_month": ("AWSGlue", "Catalog-Storage", "Obj-Month"),
}


def read_pinned(path: Path, pin: str, limit: int) -> dict[str, Any]:
    if not isinstance(pin, str) or len(pin) != 64 or any(c not in "0123456789abcdef" for c in pin):
        raise ValueError("An explicit lowercase SHA256 pin is required")
    if not path.is_file() or any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("Pricing records must be regular local files")
    with path.open("rb") as stream:
        content = stream.read(limit + 1)
    if len(content) > limit or hashlib.sha256(content).hexdigest() != pin:
        raise ValueError("Pricing record checksum or size differs")
    value = json.loads(content)
    if not isinstance(value, dict):
        raise ValueError("Pricing records must contain objects")
    return value


def timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("Price evidence requires an aware timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("Invalid price evidence timestamp") from None
    if result.utcoffset() is None:
        raise ValueError("Price evidence timestamps must include a timezone")
    return result


def rates(folder: Path, index_pin: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Never fetch prices or choose another region, runtime, storage class or tier."""
    index = read_pinned(folder / "index.json", index_pin, 128 * 1024)
    if set(index) != set(SERVICES):
        raise ValueError("All four regional catalogs are required")
    selected, sources = {}, {}
    for service in SERVICES:
        source = index[service]
        url = f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/{REGION}/index.json"
        if source["url"] != url or type(source["bytes"]) is not int:
            raise ValueError("Price source is not the reviewed regional offer")
        path = folder / (service + ".json")
        catalog = read_pinned(path, source["sha256"], MAX_CATALOG_BYTES)
        if (
            catalog["offerCode"] != service
            or path.stat().st_size != source["bytes"]
            or catalog["publicationDate"] != source["publication_date"]
            or catalog["version"] != source["version"]
        ):
            raise ValueError("Price catalog identity differs from its index")
        retrieved = timestamp(source["retrieved_at_utc"])
        if timestamp(catalog["publicationDate"]) > retrieved:
            raise ValueError("Catalog publication is later than retrieval")
        sources[service] = {
            k: source[k]
            for k in ("url", "sha256", "bytes", "publication_date", "version", "retrieved_at_utc")
        }
        for name, (expected_service, usage, unit) in METERS.items():
            if expected_service != service:
                continue
            matches = []
            for sku, product in catalog["products"].items():
                attrs = product["attributes"]
                if (
                    attrs.get("regionCode") != REGION
                    or attrs.get("servicecode") != service
                    or attrs.get("usagetype") != "EUN1-" + usage
                ):
                    continue
                if name == "glue_dpu_hour" and (
                    attrs.get("group") != "ETL Job run" or attrs.get("operation") != "Jobrun"
                ):
                    continue
                if name == "s3_gb_month" and (
                    attrs.get("storageClass") != "General Purpose"
                    or attrs.get("volumeType") != "Standard"
                ):
                    continue
                for term in catalog["terms"]["OnDemand"].get(sku, {}).values():
                    if timestamp(term["effectiveDate"]) > retrieved:
                        continue
                    for dimension in term["priceDimensions"].values():
                        if dimension["beginRange"] != "0" or dimension["unit"] != unit:
                            continue
                        try:
                            raw = dimension["pricePerUnit"]
                            if set(raw) != {"USD"} or not isinstance(raw["USD"], str):
                                raise ValueError("Use a USD string rate")
                            price = Decimal(raw["USD"])
                            if not price.is_finite() or price <= 0:
                                raise ValueError("Use a positive finite list rate")
                        except (InvalidOperation, KeyError, TypeError):
                            raise ValueError("Invalid USD price") from None
                        matches.append(
                            {
                                "sku": sku,
                                "usage_type": attrs["usagetype"],
                                "unit": unit,
                                "usd_per_unit": str(price),
                                "effective_date": term["effectiveDate"],
                            }
                        )
            if len(matches) != 1:
                raise ValueError("Required regional meter is missing or ambiguous: " + name)
            selected[name] = matches[0]
    return selected, sources


def dollars(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000001"), rounding=ROUND_UP), "f")


def estimate(folder: Path, index_pin: str) -> dict[str, Any]:
    selected, sources = rates(folder, index_pin)
    price = {k: Decimal(v["usd_per_unit"]) for k, v in selected.items()}
    # Planning quantities, not enforcement. Use seven days and three charged scans
    # conservatively even though the intended teardown is two days and DDL is free.
    month = Decimal(7) / 30
    scan_mb = (Decimal(104857600) / 1000000).to_integral_value(rounding=ROUND_CEILING)
    ancillary = {
        "athena_three_scans": 3 * scan_mb / 1000000 * price["athena_tb"],
        "s3_storage": 2 * month * price["s3_gb_month"],
        "s3_put_list": 5000 * price["s3_put_list"],
        "s3_get": 25000 * price["s3_get"],
        "logs_ingest": Decimal("0.125") * price["logs_ingest_gb"],
        "logs_storage": Decimal("0.125") * month * price["logs_gb_month"],
        "catalog_requests": 5000 * price["catalog_request"],
        "catalog_storage": 32 * month * price["catalog_object_month"],
    }
    scenarios = {}
    for name, seconds in (("ten_minute_job", 600), ("seventeen_minute_supervision_window", 1020)):
        compute = Decimal(2 * seconds) / 3600 * price["glue_dpu_hour"]
        total = compute + sum(ancillary.values())
        scenarios[name] = {
            "glue_seconds_assumed": seconds,
            "glue_compute_usd": dollars(compute),
            "itemized_total_usd": dollars(total),
            "rounded_up_cents_usd": str(total.quantize(Decimal("0.01"), rounding=ROUND_UP)),
            "compute_plus_075_ancillary_reserve_usd": dollars(compute + Decimal("0.75")),
        }
    return {
        "status": "stockholm_first_trial_cost_scenarios",
        "region": REGION,
        "pricing_index_sha256": index_pin,
        "sources": sources,
        "meters": selected,
        "assumptions": {
            "glue_version": "5.1",
            "worker_type": "G.1X",
            "workers": 2,
            "dpu_per_worker": 1,
            "glue_billing_minimum_seconds": 60,
            "retention_days_modeled": 7,
            "billing_month_days_assumed": 30,
            "s3_billable_gb_assumed": "2",
            "logs_billable_gb_assumed": "0.125",
            "athena_scans_modeled": 3,
            "athena_bytes_per_scan_assumed": 104857600,
            "athena_decimal_mb_rounded_up_per_scan": int(scan_mb),
            "athena_decimal_bytes_per_tb_assumed": 1000000000000,
            "s3_put_list_requests": 5000,
            "s3_get_requests": 25000,
            "catalog_requests": 5000,
            "catalog_objects": 32,
        },
        "ancillary_usd": {k: dollars(v) for k, v in ancillary.items()},
        "scenarios": scenarios,
        "proposed_planning_reserve_usd": "2.00",
        "proposed_account_spend_review_usd": "5.00",
        "credits_and_free_allowances_applied": False,
        "tax_and_currency_conversion_included": False,
        "transfer_and_unmodeled_services_included": False,
        "hard_cost_cap": False,
        "spending_authorized": False,
        "aws_requests": 0,
    }


def prepare(folder: Path, index_pin: str, output: Path) -> dict[str, Any]:
    destination = output.resolve()
    if (
        output.exists()
        or any(p.is_symlink() for p in (output, *output.parents))
        or output.suffix != ".json"
        or not destination.is_relative_to(PROJECT_ROOT / "artifacts")
        or destination.is_relative_to(folder.resolve())
    ):
        raise ValueError("Use a fresh private JSON output under artifacts, outside the catalogs")
    result = estimate(folder, index_pin)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--pricing", type=Path, required=True)
    parser.add_argument("--pricing-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = prepare(args.pricing, args.pricing_sha256, args.output)
    print(
        json.dumps(
            {
                "status": value["status"],
                "scenarios": value["scenarios"],
                "spending_authorized": False,
            }
        )
    )


if __name__ == "__main__":
    main()
