"""Explicit, bounded run contracts; no implicit latest release or global extraction."""

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pyproj import Transformer


@dataclass(frozen=True)
class RunConfig:
    area_id: str
    release: str
    bbox: tuple[float, float, float, float]
    metric_crs: str = "EPSG:3763"
    threshold_m: float = 500.0
    max_download_bytes: int = 64 * 1024 * 1024
    max_output_bytes: int = 16 * 1024 * 1024
    max_features: int = 20000
    max_source_files: int = 12

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9_]+", self.area_id):
            raise ValueError("area_id must be a safe lowercase identifier")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.\d+", self.release):
            raise ValueError("Pin an explicit release, never latest")
        if len(self.bbox) != 4 or not all(math.isfinite(x) for x in self.bbox):
            raise ValueError("bbox requires four finite coordinates")
        west, south, east, north = self.bbox
        if not (-9.6 <= west < east <= -6.1 and 36.8 <= south < north <= 42.2):
            raise ValueError("This pilot supports mainland Portugal only")
        if east - west > 0.05 or north - south > 0.05:
            raise ValueError("Pilot bbox must be <= 0.05 degrees per axis")
        if self.metric_crs != "EPSG:3763":
            raise ValueError("Pilot distances require Portugal TM06 / EPSG:3763")
        if not math.isfinite(self.threshold_m) or not 0 < self.threshold_m <= 1000:
            raise ValueError("threshold_m must be in (0, 1000]")
        for name in ("max_download_bytes", "max_output_bytes", "max_features", "max_source_files"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def park_bbox(self) -> tuple[float, float, float, float]:
        forward = Transformer.from_crs("EPSG:4326", self.metric_crs, always_xy=True)
        reverse = Transformer.from_crs(self.metric_crs, "EPSG:4326", always_xy=True)
        west, south, east, north = forward.transform_bounds(*self.bbox, densify_pts=21)
        pad = self.threshold_m + 5.0
        left, bottom, right, top = reverse.transform_bounds(
            west - pad, south - pad, east + pad, north + pad, densify_pts=21
        )
        return left, bottom, right, top


def load_config(path: Path) -> RunConfig:
    values = json.loads(path.read_text(encoding="utf-8"))
    values["bbox"] = tuple(values["bbox"])
    return RunConfig(**values)
