from dataclasses import replace
from pathlib import Path

import pytest
from pyproj import Transformer

from urban_geography.config import load_config


@pytest.fixture
def config():
    return load_config(Path("configs/coimbra_pilot.json"))


@pytest.mark.parametrize(
    "updates",
    [
        {"release": "latest"},
        {"area_id": "../escape"},
        {"bbox": (-10, 40, -8, 41)},
        {"bbox": (-8.4, 40.1, -8.5, 40.2)},
        {"bbox": (float("nan"), 40, -8, 41)},
        {"threshold_m": -1},
        {"threshold_m": float("nan")},
        {"max_download_bytes": 0},
        {"metric_crs": "EPSG:4326"},
    ],
)
def test_reject_unbounded_or_ambiguous_config(config, updates):
    with pytest.raises(ValueError):
        replace(config, **updates)


def test_park_halo_contains_threshold_on_each_side(config):
    forward = Transformer.from_crs(4326, 3763, always_xy=True)
    inner = forward.transform_bounds(*config.bbox)
    outer = forward.transform_bounds(*config.park_bbox())
    assert inner[0] - outer[0] >= config.threshold_m
    assert inner[1] - outer[1] >= config.threshold_m
    assert outer[2] - inner[2] >= config.threshold_m
    assert outer[3] - inner[3] >= config.threshold_m
