"""Named execution ceilings; a profile is configuration, never authorization to acquire data."""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class WorkloadLimits:
    max_bbox_axis_degrees: float
    max_download_bytes: int
    max_output_bytes: int
    max_features: int
    max_source_files: int
    max_pairs: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def check_pairs(self, buildings: int, parks: int) -> None:
        if buildings * parks > self.max_pairs:
            raise ValueError(
                f"Workload logical pair budget exceeds {self.max_pairs / 1_000_000:g} million pairs"
            )


PILOT_LIMITS = WorkloadLimits(0.05, 64 * 1024**2, 16 * 1024**2, 20000, 12, 2_000_000)
COIMBRA_EXTENDED_LIMITS = WorkloadLimits(0.16, 256 * 1024**2, 64 * 1024**2, 100000, 24, 20_000_000)
COIMBRA_EXTENDED_BBOX = (-8.505, 40.125, -8.345, 40.285)


def workload_limits(profile: str) -> WorkloadLimits:
    if profile == "pilot":
        return PILOT_LIMITS
    if profile == "coimbra_extended_v1":
        return COIMBRA_EXTENDED_LIMITS
    raise ValueError("Unknown workload profile; use a named bounded configuration")
