import importlib.util
from pathlib import Path

import pytest
from shapely.geometry import Point, box

SPEC = importlib.util.spec_from_file_location(
    "review_pilot", Path(__file__).resolve().parents[1] / "scripts" / "review_pilot.py"
)
assert SPEC is not None and SPEC.loader is not None
REVIEW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REVIEW)


def test_outside_park_can_be_the_only_service_for_an_inside_building():
    area = box(0, 0, 100, 100)
    points = {"inside": Point(20, 20), "edge": Point(95, 50), "unmatched": Point(50, 90)}
    parks = {"inside_park": box(10, 10, 25, 25), "outside_park": box(110, 40, 130, 60)}

    audit, served = REVIEW.audit_geometry(points, parks, area, 20)

    assert served == {"inside", "edge"}
    assert audit["matched_without_wholly_outside_parks"] == 1
    assert audit["buildings_relying_on_wholly_outside_parks"] == 1
    assert audit["park_relations"] == {"inside": 1, "outside": 1}


def test_crossing_parks_remain_whole_and_threshold_is_inclusive():
    area = box(0, 0, 100, 100)
    points = {"at_threshold": Point(70, 50), "just_beyond": Point(69.999, 50)}
    parks = {"crossing": box(90, 40, 120, 60)}

    audit, served = REVIEW.audit_geometry(points, parks, area, 20)

    assert served == {"at_threshold"}
    assert audit["park_relations"] == {"crosses_boundary": 1}
    assert audit["parks"][0]["area_m2"] == 600


def test_review_rejects_empty_denominator():
    with pytest.raises(ValueError, match="requires selected buildings"):
        REVIEW.audit_geometry({}, {}, box(0, 0, 100, 100), 20)


def test_outside_candidates_are_not_needed_when_an_inside_park_also_qualifies():
    points = {"shared": Point(95, 50)}
    parks = {"inside": box(70, 40, 80, 60), "outside": box(110, 40, 130, 60)}

    audit, served = REVIEW.audit_geometry(points, parks, box(0, 0, 100, 100), 20)

    assert served == {"shared"}
    assert audit["buildings_relying_on_wholly_outside_parks"] == 0
    assert audit["boundary_dependent_building_ids"] == []
