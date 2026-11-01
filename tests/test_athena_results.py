"""Gold comparison against a pinned summary, including bad rows and weighted-count semantics."""

import json

import pytest
from athena_results_fixtures import RESULTS as results
from athena_results_fixtures import csv_bytes, summary_reference


@pytest.fixture
def reference(tmp_path):
    return summary_reference(tmp_path)


def test_summary_and_csv_match_integer_totals_and_count_weighted_share(reference):
    path, pin, expected = reference
    assert expected["mapped_buildings"] == 6 and expected["buildings_within"] == 2
    assert expected["share_within"] == 2 / 6
    assert results.compare_gold_csv(csv_bytes(expected), expected) == expected


@pytest.mark.parametrize(
    "field,value",
    [
        ("mapped_buildings", True),
        ("mapped_buildings", 0),
        ("mapped_buildings", 20001),
        ("buildings_within_500m", True),
        ("buildings_within_500m", -1),
        ("buildings_within_500m", 7),
        ("share_within", True),
        ("share_within", float("nan")),
        ("share_within", 0.5),
        ("status", "incomplete"),
    ],
)
def test_invalid_expected_summary_fails_even_when_its_bytes_are_pinned(reference, field, value):
    path, pin, expected = reference
    summary = json.loads(path.read_text())
    summary[field] = value
    path.write_text(json.dumps(summary))
    pin = results.hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        results.expected_gold(path, pin)


def test_changed_summary_bytes_fail_the_independent_pin(reference):
    path, pin, expected = reference
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="SHA256"):
        results.expected_gold(path, pin)


@pytest.mark.parametrize(
    "index,value",
    [
        (0, "2026-10-21.0"),
        (1, "other_area"),
        (2, "b" * 64),
        (3, "5"),
        (3, "06"),
        (3, "6.0"),
        (4, "3"),
        (4, "-1"),
        (5, "0.5"),
        (5, "NaN"),
        (5, "Infinity"),
        (5, "1e999"),
        (5, "-0.1"),
        (5, "1.1"),
    ],
)
def test_bad_result_values_are_rejected(reference, index, value):
    path, pin, expected = reference
    row = [str(expected[name]) for name in results.COLUMNS]
    row[index] = value
    with pytest.raises(ValueError):
        results.compare_gold_csv(csv_bytes(expected, row=row), expected)


@pytest.mark.parametrize(
    "mode",
    [
        "wrong_header",
        "no_data",
        "second_group",
        "truncated_row",
        "invalid_utf8",
        "empty",
        "oversized",
        "broken_csv",
    ],
)
def test_result_shape_and_byte_bounds_are_enforced(reference, mode):
    path, pin, expected = reference
    data = csv_bytes(expected)
    if mode == "wrong_header":
        data = csv_bytes(expected, header=list(reversed(results.COLUMNS)))
    elif mode == "no_data":
        data = data.splitlines(keepends=True)[0]
    elif mode == "second_group":
        data = csv_bytes(expected, extra=[expected[name] for name in results.COLUMNS])
    elif mode == "truncated_row":
        data = csv_bytes(expected, row=["release"])
    elif mode == "invalid_utf8":
        data = b"\xff"
    elif mode == "empty":
        data = b""
    elif mode == "oversized":
        data = b"a" * (results.MAX_RESULT_BYTES + 1)
    else:
        data = b'"unterminated'
    with pytest.raises(ValueError):
        results.compare_gold_csv(data, expected)


def test_scientific_share_is_supported_but_count_rounding_is_not(reference):
    path, pin, expected = reference
    row = [str(expected[name]) for name in results.COLUMNS]
    row[-1] = "3.333333333333333e-1"
    assert results.compare_gold_csv(csv_bytes(expected, row=row), expected)["share_within"] == 2 / 6


def test_narrow_float_tolerance_never_changes_integer_count_comparison(reference):
    path, pin, expected = reference
    row = [str(expected[name]) for name in results.COLUMNS]
    row[-1] = str(expected["share_within"] + 5e-13)
    results.compare_gold_csv(csv_bytes(expected, row=row), expected)
    row[-1] = str(expected["share_within"] + 2e-12)
    with pytest.raises(ValueError):
        results.compare_gold_csv(csv_bytes(expected, row=row), expected)
