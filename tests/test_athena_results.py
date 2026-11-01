"""Gold comparison against a pinned summary, including bad rows and weighted-count semantics."""

import copy
import json
from unittest.mock import Mock

import pytest
from athena_results_fixtures import RESULTS as results
from athena_results_fixtures import csv_bytes, make_download_case, summary_reference


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


@pytest.fixture
def download(tmp_path, monkeypatch):
    return make_download_case(tmp_path, monkeypatch)


def verify(case):
    return results.verify_query_result(
        case.athena, case.s3, case.attempt, case.receipt, summary_path=case.path
    )


def test_result_is_bound_to_saved_query_and_exact_owned_s3_object(download):
    result = verify(download)
    assert result["result_verified"] is True and result["row"] == download.expected
    assert result["caller_identity_verified"] is result["source_readback_verified"] is False
    assert result["data_scanned_bytes"] == 4096
    target = {
        "Bucket": "urban-geo-test-results-123456789012-eu-west-1",
        "Key": "queries/query-001.csv",
        "ExpectedBucketOwner": "123456789012",
    }
    download.s3.head_object.assert_called_once_with(**target)
    download.s3.get_object.assert_called_once_with(
        **target, IfMatch='"offline-etag"', Range=f"bytes=0-{len(download.data) - 1}"
    )
    assert download.body.closed and download.body.requests == [len(download.data) + 1]
    assert download.athena.get_query_execution.call_count == 2
    download.athena.start_query_execution.assert_not_called()
    download.athena.stop_query_execution.assert_not_called()
    download.athena.get_query_results.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("query_id", "another-query"),
        ("intent_sha256", "0" * 64),
        ("query_sha256", "0" * 64),
        ("terminal_confirmed", 1),
        ("outcome", "query_failed"),
    ],
)
def test_foreign_or_unsuccessful_execution_fails_before_aws(download, field, value):
    download.receipt[field] = value
    with pytest.raises(ValueError, match="attempt"):
        verify(download)
    download.athena.get_query_execution.assert_not_called()
    download.s3.head_object.assert_not_called()


def test_changed_saved_query_identity_is_rejected(download):
    (download.attempt.output / "query.json").write_text('{"query_id":"another-query"}')
    with pytest.raises(ValueError, match="attempt"):
        verify(download)
    download.s3.head_object.assert_not_called()


@pytest.mark.parametrize(
    "change", ["running", "scan_limit", "wrong_query", "reused", "missing_stats"]
)
def test_current_query_state_and_scan_contract_gate_all_s3_reads(download, change):
    response = download.athena.get_query_execution.return_value["QueryExecution"]
    if change == "running":
        response["Status"]["State"] = "RUNNING"
    elif change == "scan_limit":
        response["Statistics"]["DataScannedInBytes"] = 100 * 1024**2 + 1
    elif change == "wrong_query":
        response["Query"] = "SELECT 1"
    elif change == "reused":
        response["Statistics"]["ResultReuseInformation"]["ReusedPreviousResult"] = True
    else:
        del response["Statistics"]
    with pytest.raises(ValueError):
        verify(download)
    download.s3.head_object.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("ContentLength", results.MAX_RESULT_BYTES + 1),
        ("ContentLength", 0),
        ("ContentLength", True),
        ("ETag", None),
        ("ETag", '"bad\r\netag"'),
        ("ServerSideEncryption", "aws:kms"),
        ("ContentEncoding", "gzip"),
        ("ResponseMetadata", {"HTTPStatusCode": 206}),
    ],
)
def test_head_rejects_bad_objects_before_downloading_body(download, field, value):
    download.s3.head_object.return_value[field] = value
    with pytest.raises(ValueError, match="HEAD"):
        verify(download)
    download.s3.get_object.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("ContentLength", 1),
        ("ETag", '"other-etag"'),
        ("ContentRange", "bytes 0-1/100000"),
        ("ServerSideEncryption", "aws:kms"),
        ("ContentEncoding", "gzip"),
        ("VersionId", "different-version"),
        ("ResponseMetadata", {"HTTPStatusCode": 200}),
    ],
)
def test_bad_get_headers_close_body_without_reading_it(download, field, value):
    download.s3.get_object.return_value[field] = value
    with pytest.raises(ValueError, match="range"):
        verify(download)
    assert download.body.closed and download.body.requests == []


@pytest.mark.parametrize("error", [TimeoutError(), KeyboardInterrupt()])
def test_interrupted_body_read_still_closes_stream(download, error, monkeypatch):
    monkeypatch.setattr(download.body, "read", Mock(side_effect=error))
    with pytest.raises(type(error)):
        verify(download)
    assert download.body.closed


def test_actual_byte_count_must_match_head_even_with_good_headers(download, monkeypatch):
    monkeypatch.setattr(download.body, "read", Mock(return_value=download.data[:-1]))
    with pytest.raises(ValueError, match="body length"):
        verify(download)
    assert download.body.closed


def test_conditional_get_failure_is_never_retried(download):
    download.s3.get_object.side_effect = RuntimeError("IfMatch failed")
    with pytest.raises(RuntimeError, match="IfMatch"):
        verify(download)
    assert download.s3.get_object.call_count == 1


def test_final_query_observation_cannot_change_after_result_read(download):
    before = download.athena.get_query_execution.return_value
    after = copy.deepcopy(before)
    after["QueryExecution"]["Statistics"]["DataScannedInBytes"] = 4097
    download.athena.get_query_execution.side_effect = [before, after]
    with pytest.raises(ValueError, match="changed"):
        verify(download)
    assert download.body.closed


@pytest.mark.parametrize("changed", ["intent", "query", "summary"])
def test_local_identity_or_expectation_changes_during_download_are_rejected(download, changed):
    response = download.s3.get_object.return_value

    def fetched(**kwargs):
        if changed == "intent":
            (download.attempt.output / "intent.json").write_text("{}")
        elif changed == "query":
            (download.attempt.output / "query.json").write_text('{"query_id":"different"}')
        else:
            download.path.write_bytes(download.path.read_bytes() + b" ")
        return response

    download.s3.get_object.side_effect = fetched
    with pytest.raises(ValueError):
        verify(download)
    assert download.body.closed
