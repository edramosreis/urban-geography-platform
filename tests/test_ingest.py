import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from urban_geography.config import load_config
from urban_geography.ingest import candidate_groups, digest, overlap, validate_raw


def test_bbox_overlap_includes_enclosing_features_and_edges():
    area = (1, 1, 2, 2)
    assert overlap(dict(xmin=0, ymin=0, xmax=3, ymax=3), area)
    assert overlap(dict(xmin=2, ymin=1, xmax=3, ymax=2), area)
    assert not overlap(dict(xmin=3, ymin=3, xmax=4, ymax=4), area)


def test_row_group_pruning_is_conservative(tmp_path):
    table = pa.table(
        {"bbox": [{"xmin": x, "ymin": 0.0, "xmax": x + 1, "ymax": 1.0} for x in (0.0, 10.0, 20.0)]}
    )
    path = tmp_path / "stats.parquet"
    pq.write_table(table, path, row_group_size=1)
    assert candidate_groups(pq.read_metadata(path), (9.5, 0, 11, 1)) == [1]
    pq.write_table(table, path, row_group_size=1, write_statistics=False)
    assert candidate_groups(pq.read_metadata(path), (9.5, 0, 11, 1)) == [0, 1, 2]


def test_raw_replay_detects_corruption(tmp_path):
    config = load_config(Path("configs/coimbra_pilot.json"))
    extracts = {}
    for kind in ("building", "land_use"):
        path = tmp_path / f"{kind}.parquet"
        path.write_bytes(b"content")
        extracts[kind] = {"sha256": digest(path)}
    (tmp_path / "manifest.json").write_text(
        json.dumps({"config": config.as_dict(), "extracts": extracts})
    )
    validate_raw(tmp_path, config)
    (tmp_path / "building.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        validate_raw(tmp_path, config)
