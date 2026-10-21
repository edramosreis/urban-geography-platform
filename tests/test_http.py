from unittest.mock import MagicMock

import pytest

from urban_geography.http import DownloadBudget, RangeReader

URL = "https://stac.overturemaps.org/test.parquet"


def response(budget, status, headers, chunks):
    result = MagicMock()
    result.status_code = status
    result.headers = headers
    result.iter_content.return_value = iter(chunks)
    result.__enter__.return_value = result
    budget.session.get = MagicMock(return_value=result)
    return result


def test_rejects_whole_file_fallback_before_body():
    budget = DownloadBudget(100)
    result = response(budget, 200, {}, [b"large"])
    with pytest.raises(ValueError, match="bounded"):
        budget.get(URL, 0, 10)
    result.iter_content.assert_not_called()


def test_budget_is_shared_and_range_is_validated():
    budget = DownloadBudget(10)
    response(budget, 206, {"Content-Range": "bytes 2-7/100"}, [b"123456"])
    assert budget.get(URL, 2, 6) == b"123456"
    with pytest.raises(ValueError, match="budget"):
        budget.get(URL, 8, 5)
    response(budget, 206, {"Content-Range": "bytes 1-4/100"}, [b"1234"])
    with pytest.raises(ValueError, match="Content-Range"):
        budget.get(URL, 8, 4)


def test_large_metadata_response_aborts():
    budget = DownloadBudget(10)
    result = response(budget, 200, {"Content-Length": "11"}, [b"x" * 11])
    with pytest.raises(ValueError, match="budget"):
        budget.get(URL)
    result.iter_content.assert_not_called()


def test_streaming_overrun_and_truncated_range():
    budget = DownloadBudget(4)
    response(budget, 200, {}, [b"12345"])
    with pytest.raises(ValueError, match="budget"):
        budget.get(URL)
    budget = DownloadBudget(20)
    response(budget, 206, {"Content-Range": "bytes 0-3/100"}, [b"12"])
    with pytest.raises(ValueError, match="Truncated"):
        budget.get(URL, 0, 4)


def test_range_reader_seeks_and_handles_eof():
    budget = DownloadBudget(20)
    budget.get = MagicMock(return_value=b"xyz")
    reader = RangeReader(URL, 10, budget)
    assert reader.seek(-3, 2) == 7
    assert reader.read(20) == b"xyz"
    budget.get.assert_called_once_with(URL, 7, 3)
    assert reader.tell() == 10
    assert reader.read() == b""


def test_rejects_untrusted_source_url():
    with pytest.raises(ValueError, match="Unsupported"):
        DownloadBudget(100).get("http://example.com/file")
