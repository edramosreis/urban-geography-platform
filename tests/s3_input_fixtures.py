"""Small in-memory implementation of only the S3 GET shape used by input transfer."""

import io
from pathlib import Path
from typing import Any


class ShortBody(io.BytesIO):
    def __init__(self, content: bytes):
        super().__init__(content)
        self.fail_reads = False

    def read(self, size: int = -1) -> bytes:
        if self.fail_reads:
            raise OSError("synthetic interrupted response")
        return super().read(min(size, 37) if size >= 0 else size)


class MemoryS3:
    def __init__(self, prepared: Path, pin: str):
        self.prefix = "inputs/" + pin + "/"
        self.objects = {
            self.prefix + path.relative_to(prepared).as_posix(): path.read_bytes()
            for path in prepared.rglob("*")
            if path.is_file()
        }
        self.requests: list[dict[str, Any]] = []
        self.bodies: list[ShortBody] = []
        self.response_change = None
        self.closed = False

    def get_object(self, **request):
        if self.closed:
            raise RuntimeError("The object client is closed")
        self.requests.append(request)
        key = request["Key"]
        if key not in self.objects:
            raise FileNotFoundError(key)
        end = int(request["Range"].split("-", 1)[1])
        content = self.objects[key][: end + 1]
        body = ShortBody(content)
        self.bodies.append(body)
        response = {"Body": body, "ContentLength": len(content), "VersionId": "version-1"}
        if self.response_change is not None:
            self.response_change(response)
        return response

    def close(self):
        self.closed = True
