"""Budgeted HTTPS range reads. Refuse servers that silently return whole objects."""

import io
import re
from typing import Any
from urllib.parse import urlparse

import requests
from requests.structures import CaseInsensitiveDict

OVERTURE_HOSTS = frozenset(
    {
        "stac.overturemaps.org",
        "overturemaps-us-west-2.s3.amazonaws.com",
        "overturemaps-us-west-2.s3.us-west-2.amazonaws.com",
    }
)


class DownloadBudget:
    def __init__(self, limit: int, *, allowed_hosts: frozenset[str] | None = None) -> None:
        self.allowed_hosts = OVERTURE_HOSTS if allowed_hosts is None else allowed_hosts
        self.last_response_headers: CaseInsensitiveDict[str] = CaseInsensitiveDict()
        self.limit = limit
        self.transferred = 0
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "urban-geography-portfolio/0.1"

    def get(self, url: str, start: int | None = None, length: int | None = None) -> bytes:
        if (start is None) != (length is None):
            raise ValueError("Range requires both start and length")
        if start is not None and (start < 0 or length is None or length <= 0):
            raise ValueError("Range must be nonnegative with positive length")
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in self.allowed_hosts:
            raise ValueError(f"Unsupported source host: {parsed.hostname}")
        remaining = self.limit - self.transferred
        if remaining <= 0 or (length is not None and length > remaining):
            raise ValueError("Download budget exhausted; no automatic larger download")
        headers = {"Accept-Encoding": "identity"}
        if start is not None and length is not None:
            headers["Range"] = f"bytes={start}-{start + length - 1}"
        with self.session.get(
            url, headers=headers, timeout=(10, 60), stream=True, allow_redirects=False
        ) as response:
            response.raise_for_status()
            expected_status = 206 if start is not None else 200
            if response.status_code != expected_status:
                raise ValueError("Server did not honor the bounded request")
            if start is not None and length is not None:
                match = re.fullmatch(
                    r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", "")
                )
                if not match or int(match[1]) != start or int(match[2]) != start + length - 1:
                    raise ValueError("Unexpected Content-Range")
            advertised = int(response.headers.get("Content-Length", 0))
            if advertised > remaining:
                raise ValueError("Response exceeds remaining download budget")
            result = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                self.transferred += len(chunk)
                if self.transferred > self.limit:
                    raise ValueError("Download budget exceeded")
                result.extend(chunk)
            if length is not None and len(result) != length:
                raise ValueError("Truncated range response")
            self.last_response_headers = CaseInsensitiveDict(response.headers)
            return bytes(result)


class RangeReader(io.RawIOBase):
    def __init__(self, url: str, size: int, budget: DownloadBudget) -> None:
        self.url, self.size, self.budget = url, size, budget
        self.position = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = 0) -> int:
        position = (
            offset
            if whence == 0
            else (self.position + offset if whence == 1 else self.size + offset)
        )
        if whence not in (0, 1, 2) or position < 0:
            raise ValueError("Invalid seek")
        self.position = position
        return position

    def read(self, size: int = -1) -> bytes:
        length = max(0, self.size - self.position)
        if size >= 0:
            length = min(length, size)
        if not length:
            return b""
        result = self.budget.get(self.url, self.position, length)
        self.position += len(result)
        return result

    def readinto(self, buffer: Any) -> int:
        content = self.read(len(buffer))
        buffer[: len(content)] = content
        return len(content)
