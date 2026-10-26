"""Publish matched local dimension snapshots through an immutable completion manifest."""

import hashlib
import json
import re
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from urban_geography.dimension_storage import (
    KEYS,
    DimensionKind,
    prepare_dimension_table,
    read_dimension_table,
    same_rows,
    write_dimension_table,
)
from urban_geography.dimensions import (
    DimensionContract,
    ReleaseDimension,
    SourceRunBinding,
    StudyAreaDimension,
    build_dimension_contract,
    dimension_frames,
)
from urban_geography.lakehouse import (
    HISTORY_TYPES,
    CompletedRun,
    _history_table_contract,
    _history_types,
    _table_metadata,
    _validate_facts,
)

MAX_PUBLICATION_FACTS = 40_000
DIMENSION_KINDS: tuple[DimensionKind, ...] = ("releases", "study_areas")
BINDING_KEYS = ["source_release", "area_id", "source_run_sha256"]


@dataclass(frozen=True)
class PublishedDimensions:
    releases: DataFrame
    study_areas: DataFrame
    facts: DataFrame
    manifest: dict[str, Any]


def _digest(value: Any) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def validate_publication_request(
    prefix: str, fact_table_name: str, snapshot_id: int | None
) -> None:
    if not all(re.fullmatch(r"[a-z][a-z0-9_]*", name) for name in (prefix, fact_table_name)):
        raise ValueError("Publication tables require safe lowercase identifiers")
    if snapshot_id is not None and snapshot_id <= 0:
        raise ValueError("Fact snapshot ID must be positive")


def _warehouse(spark: SparkSession) -> str:
    location = urlparse(spark.conf.get("spark.sql.catalog.local.warehouse"))
    if location.scheme != "file" or location.netloc:
        raise ValueError("Dimension publication requires a local filesystem warehouse")
    return Path(unquote(location.path)).resolve().as_uri()


def _fact_snapshot(
    spark: SparkSession, table_name: str, snapshot_id: int | None, analysis_hash: str
) -> tuple[DataFrame, int]:
    table = "local.analytics." + table_name
    if not spark.catalog.tableExists(table):
        raise ValueError("Fact history table does not exist")
    metadata = _table_metadata(spark, table)
    _history_table_contract(metadata)
    _history_types(metadata)
    properties = metadata["properties"]
    if (
        properties.get("analysis-sha256") != analysis_hash
        or _digest(json.loads(properties["analysis-definition"])) != analysis_hash
    ):
        raise ValueError("Fact history analysis differs from the dimension contract")
    pinned = snapshot_id if snapshot_id is not None else metadata.get("current-snapshot-id")
    if pinned is None or pinned not in {s["snapshot-id"] for s in metadata["snapshots"]}:
        raise ValueError("Fact snapshot is not retained")
    facts = spark.read.option("snapshot-id", str(pinned)).table(table).select(*HISTORY_TYPES)
    if not 0 < facts.limit(MAX_PUBLICATION_FACTS + 1).count() <= MAX_PUBLICATION_FACTS:
        raise ValueError("Dimension publication exceeds the pilot fact limit or has no facts")
    return facts, int(pinned)


def _decode_contract(value: dict[str, Any]) -> DimensionContract:
    return DimensionContract(
        value["analysis_sha256"],
        tuple(
            ReleaseDimension(
                **{**row, "release_label_date": date.fromisoformat(row["release_label_date"])}
            )
            for row in value["releases"]
        ),
        tuple(StudyAreaDimension(**row) for row in value["study_areas"]),
        tuple(SourceRunBinding(**row) for row in value["source_runs"]),
    )


def _open_manifest(spark: SparkSession, manifest: dict[str, Any]) -> PublishedDimensions:
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    payload = manifest["payload"]
    if (
        manifest.get("status") != "complete"
        or payload.get("contract_version") != 1
        or manifest.get("manifest_sha256") != _digest(body)
        or manifest.get("publication_sha256") != _digest(payload)
    ):
        raise ValueError("Dimension completion manifest is invalid")
    validate_publication_request(
        payload["table_prefix"], payload["fact_table_name"], payload["fact_snapshot_id"]
    )
    if payload["warehouse"] != _warehouse(spark):
        raise ValueError("Publication belongs to a different warehouse")
    contract = _decode_contract(payload["dimensions"])
    expected = dimension_frames(spark, contract)
    selected = []
    for kind, incoming in zip(DIMENSION_KINDS, expected, strict=True):
        descriptor = manifest["tables"][kind]
        table = f"local.analytics.{payload['table_prefix']}_{kind}"
        if descriptor["table"] != table:
            raise ValueError("Dimension table binding differs from the publication")
        stored = read_dimension_table(spark, table, kind, descriptor["snapshot_id"])
        subset = stored.join(incoming.select(KEYS[kind]), KEYS[kind], "left_semi").select(
            *incoming.columns
        )
        same_rows(subset, incoming)
        selected.append(subset)
    releases, areas = selected
    facts, _ = _fact_snapshot(
        spark, payload["fact_table_name"], payload["fact_snapshot_id"], contract.analysis_sha256
    )
    bindings = spark.createDataFrame(
        [asdict(row) for row in contract.source_runs],
        "source_release STRING, area_id STRING, source_run_sha256 STRING, study_area_sha256 STRING",
    )
    same_rows(facts.select(*BINDING_KEYS).distinct(), bindings.select(*BINDING_KEYS))
    enriched = (
        facts.join(bindings, BINDING_KEYS, "inner")
        .join(releases, "source_release", "inner")
        .join(
            areas.select(
                "area_id", "study_area_sha256", "west", "south", "east", "north", "metric_crs"
            ),
            ["area_id", "study_area_sha256"],
            "inner",
        )
    )
    if enriched.count() != facts.count():
        raise ValueError("Published dimension joins changed the fact count")
    return PublishedDimensions(releases, areas, enriched, manifest)


def read_dimension_publication(spark: SparkSession, path: Path) -> PublishedDimensions:
    """Read a completed pair using recorded snapshots, including after later registry appends."""
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if path.name != manifest.get("publication_sha256"):
        raise ValueError("Publication directory differs from its manifest identity")
    return _open_manifest(spark, manifest)


def _receipt(path: Path, manifest: dict[str, Any], status: str) -> dict[str, Any]:
    dimensions = manifest["payload"]["dimensions"]
    return {
        "status": status,
        "path": str(path),
        "publication_sha256": manifest["publication_sha256"],
        "fact_snapshot_id": manifest["payload"]["fact_snapshot_id"],
        "dimension_snapshots": manifest["tables"],
        "release_rows": len(dimensions["releases"]),
        "study_area_rows": len(dimensions["study_areas"]),
    }


def publish_dimensions(
    spark: SparkSession,
    runs: Sequence[CompletedRun],
    *,
    publication_root: Path,
    table_prefix: str = "dim",
    fact_table_name: str = "building_proximity_history",
    fact_snapshot_id: int | None = None,
) -> dict[str, Any]:
    """Preflight the whole pair; append keys; publish completion only after verified joins."""
    validate_publication_request(table_prefix, fact_table_name, fact_snapshot_id)
    contract = build_dimension_contract(runs)
    unique_runs = list({run.summary_sha256: run for run in runs}.values())
    if not 0 < sum(run.summary["mapped_buildings"] for run in unique_runs) <= MAX_PUBLICATION_FACTS:
        raise ValueError("Retained source runs exceed the pilot fact limit or have no facts")
    facts, pinned = _fact_snapshot(
        spark, fact_table_name, fact_snapshot_id, contract.analysis_sha256
    )
    payload = json.loads(
        json.dumps(
            {
                "contract_version": 1,
                "warehouse": _warehouse(spark),
                "fact_table_name": fact_table_name,
                "fact_snapshot_id": pinned,
                "table_prefix": table_prefix,
                "dimensions": asdict(contract),
            },
            default=str,
            allow_nan=False,
        )
    )
    publication_hash = _digest(payload)
    destination = publication_root / publication_hash
    if destination.exists():
        opened = read_dimension_publication(spark, destination)
        if opened.manifest["payload"] != payload:
            raise ValueError("Existing publication has different source bindings")
        return _receipt(destination, opened.manifest, "reused")

    expected_facts = None
    for run in unique_runs:
        incoming = spark.read.parquet(str(run.path / "building_proximity"))
        _validate_facts(incoming, run.summary)
        incoming = incoming.withColumn("source_run_sha256", F.lit(run.summary_sha256)).select(
            *HISTORY_TYPES
        )
        expected_facts = (
            incoming if expected_facts is None else expected_facts.unionByName(incoming)
        )
    if expected_facts is None:
        raise ValueError("Publication requires retained source runs")
    same_rows(facts, expected_facts)
    frames = dimension_frames(spark, contract)
    plans = [
        prepare_dimension_table(
            spark, frame, table=f"local.analytics.{table_prefix}_{kind}", kind=kind
        )
        for kind, frame in zip(DIMENSION_KINDS, frames, strict=True)
    ]
    descriptors = {}
    for plan in plans:
        receipt = write_dimension_table(spark, plan, publication_hash)
        descriptors[plan.kind] = {key: receipt[key] for key in ("table", "snapshot_id")}
    manifest = {
        "status": "complete",
        "publication_sha256": publication_hash,
        "payload": payload,
        "tables": descriptors,
    }
    manifest["manifest_sha256"] = _digest(manifest)
    _open_manifest(spark, manifest)
    publication_root.mkdir(parents=True, exist_ok=True)
    staging = publication_root / (".staging-" + uuid.uuid4().hex)
    staging.mkdir()
    (staging / "manifest.json").write_text(
        json.dumps(manifest, indent=2, allow_nan=False), encoding="utf-8"
    )
    staging.rename(destination)
    return _receipt(destination, manifest, "published")
