"""Small release-pinned Overture extraction with explicit transfer limits."""

import hashlib
import io
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from urban_geography.config import RunConfig
from urban_geography.http import DownloadBudget, RangeReader


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def config_key(config: RunConfig) -> str:
    return hashlib.sha256(json.dumps(config.as_dict(), sort_keys=True).encode()).hexdigest()[:16]


def overlap(bbox: dict[str, float], area: tuple[float, float, float, float]) -> bool:
    return (
        bbox["xmin"] <= area[2]
        and bbox["xmax"] >= area[0]
        and bbox["ymin"] <= area[3]
        and bbox["ymax"] >= area[1]
    )


def candidate_groups(metadata: Any, bbox: tuple[float, float, float, float]) -> list[int]:
    selected = []
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        stats = {
            group.column(j).path_in_schema: group.column(j).statistics
            for j in range(group.num_columns)
        }
        separated = False
        for field, bound, side in (
            ("xmin", bbox[2], "min"),
            ("xmax", bbox[0], "max"),
            ("ymin", bbox[3], "min"),
            ("ymax", bbox[1], "max"),
        ):
            stat = stats.get(f"bbox.{field}")
            if stat is not None and stat.has_min_max:
                value = getattr(stat, side)
                if (side == "min" and value > bound) or (side == "max" and value < bound):
                    separated = True
        if not separated:
            selected.append(index)
    return selected


def validate_raw(path: Path, config: RunConfig) -> dict[str, Any]:
    manifest = json.loads((path / "manifest.json").read_text())
    if manifest["config"] != json.loads(json.dumps(config.as_dict())):
        raise ValueError("Raw manifest configuration mismatch")
    for kind in ("building", "land_use"):
        if digest(path / f"{kind}.parquet") != manifest["extracts"][kind]["sha256"]:
            raise ValueError(f"Raw checksum mismatch: {kind}")
    return manifest


def extract(config: RunConfig, root: Path) -> Path:
    destination = root / config.release / config.area_id / config_key(config)
    if destination.exists():
        validate_raw(destination, config)
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".staging-{uuid.uuid4().hex}"
    staging.mkdir()
    budget = DownloadBudget(config.max_download_bytes)
    index_url = f"https://stac.overturemaps.org/{config.release}/collections.parquet"
    index_bytes = budget.get(index_url)
    index = pq.read_table(io.BytesIO(index_bytes))
    manifest: dict[str, Any] = {
        "contract_version": 1,
        "config": config.as_dict(),
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "index_url": index_url,
        "index_sha256": hashlib.sha256(index_bytes).hexdigest(),
        "park_bbox": config.park_bbox(),
        "extracts": {},
        "attribution": "© OpenStreetMap contributors, Overture Maps Foundation",
    }
    file_count = 0
    output_bytes = 0
    for kind, bbox in (("building", config.bbox), ("land_use", config.park_bbox())):
        entries = [
            row
            for row in index.to_pylist()
            if row["collection"] == kind and row["type"] == "Feature" and overlap(row["bbox"], bbox)
        ]
        file_count += len(entries)
        if file_count > config.max_source_files:
            raise ValueError("STAC selection exceeds file limit")
        tables = []
        provenance = []
        row_count = 0
        for entry in entries:
            asset = entry["assets"]["aws"]
            source = asset["alternate"]["s3"]["href"]
            prefix = f"s3://overturemaps-us-west-2/release/{config.release}/"
            if not source.startswith(prefix):
                raise ValueError("Unexpected upstream release path")
            url = source.replace(
                "s3://overturemaps-us-west-2/",
                "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/",
                1,
            )
            with budget.session.head(url, timeout=(10, 30), allow_redirects=False) as response:
                response.raise_for_status()
                size = int(response.headers["Content-Length"])
                etag = response.headers.get("ETag")
            reader = pq.ParquetFile(RangeReader(url, size, budget))
            groups = candidate_groups(reader.metadata, bbox)
            columns = ["id", "geometry", "bbox", "sources"]
            if kind == "land_use":
                columns.append("class")
            predicate = (
                (pc.field("bbox", "xmin") <= bbox[2])
                & (pc.field("bbox", "xmax") >= bbox[0])
                & (pc.field("bbox", "ymin") <= bbox[3])
                & (pc.field("bbox", "ymax") >= bbox[1])
            )
            if kind == "land_use":
                predicate = predicate & (pc.field("class") == "park")
            for group in groups:
                table = reader.read_row_group(group, columns=columns, use_threads=False)
                table = table.filter(predicate)
                row_count += table.num_rows
                if row_count > config.max_features:
                    raise ValueError("Selected features exceed pilot limit")
                if table.num_rows:
                    tables.append(table)
            provenance.append(
                {
                    "url": url,
                    "etag": etag,
                    "size": size,
                    "row_groups": groups,
                    "total_row_groups": reader.metadata.num_row_groups,
                }
            )
        if not tables:
            raise ValueError(f"No {kind} records found; do not publish an empty extraction")
        combined = pa.concat_tables(tables)
        output = staging / f"{kind}.parquet"
        pq.write_table(combined, output, compression="zstd")
        output_bytes += output.stat().st_size
        if output_bytes > config.max_output_bytes:
            raise ValueError("Extracted output exceeds pilot limit")
        manifest["extracts"][kind] = {
            "rows": row_count,
            "sha256": digest(output),
            "bytes": output.stat().st_size,
            "source_files": provenance,
            "schema": str(combined.schema),
        }
    manifest["downloaded_bytes"] = budget.transferred
    manifest["output_bytes"] = output_bytes
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    staging.rename(destination)
    return destination
