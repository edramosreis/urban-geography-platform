"""Bounded JSON messages over an inherited local socket; never pickle or credentials."""

import json
import socket
import struct
import threading
import time
from typing import Any

MAX_FRAME_BYTES = 64 * 1024
METHODS = {
    "athena": {"start_query_execution", "get_query_execution", "get_query_results"},
    "sts": {"get_caller_identity"},
    "glue": {"get_databases", "get_tables", "get_table_versions", "delete_table_version"},
}


def send(sock: socket.socket, value: dict[str, Any], deadline: float) -> None:
    content = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    if len(content) > MAX_FRAME_BYTES:
        raise ValueError("dbt IPC message exceeds its byte ceiling")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("dbt IPC deadline reached")
    sock.settimeout(remaining)
    sock.sendall(struct.pack("!I", len(content)) + content)


def receive(sock: socket.socket, deadline: float) -> dict[str, Any]:
    def exact(size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("dbt IPC deadline reached")
            sock.settimeout(remaining)
            part = sock.recv(size - len(chunks))
            if not part:
                raise EOFError("dbt IPC peer closed")
            chunks.extend(part)
        return bytes(chunks)

    size = struct.unpack("!I", exact(4))[0]
    if not 0 < size <= MAX_FRAME_BYTES:
        raise ValueError("Invalid dbt IPC message size")
    value = json.loads(exact(size))
    if not isinstance(value, dict):
        raise ValueError("dbt IPC message must be an object")
    return value


class RemoteSession:
    def __init__(self, sock: socket.socket, deadline: float) -> None:
        self.sock, self.deadline = sock, deadline
        self.sequence = 0
        self.lock = threading.Lock()

    def client(self, service: str, **kwargs: Any) -> Any:
        if service not in METHODS:
            raise ValueError("Unsupported dbt service")
        return RemoteClient(self, service)

    def call(self, service: str, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            self.sequence += 1
            send(
                self.sock,
                {
                    "version": 1,
                    "sequence": self.sequence,
                    "service": service,
                    "operation": operation,
                    "parameters": params,
                },
                self.deadline,
            )
            response = receive(self.sock, self.deadline)
            if (
                set(response) != {"sequence", "ok", "value"}
                or response["sequence"] != self.sequence
                or type(response["ok"]) is not bool
            ):
                raise ValueError("Unexpected dbt IPC response")
            if not response["ok"]:
                raise RuntimeError("Parent refused dbt request")
            if not isinstance(response["value"], dict):
                raise ValueError("Invalid dbt IPC result")
            return response["value"]


class RemoteClient:
    def __init__(self, session: RemoteSession, service: str) -> None:
        self.session, self.service = session, service

    def __getattr__(self, name: str) -> Any:
        if name not in METHODS[self.service]:
            raise AttributeError(name)
        return lambda **kwargs: self.session.call(self.service, name, kwargs)

    def get_paginator(self, name: str) -> Any:
        if self.service != "glue" or name not in {
            "get_databases",
            "get_tables",
            "get_table_versions",
        }:
            raise ValueError("Unsupported dbt paginator")
        from botocore.paginate import Paginator
        from botocore.session import Session

        operation = "".join(part.title() for part in name.split("_"))
        sdk = Session()
        return Paginator(
            getattr(self, name),
            sdk.get_paginator_model("glue").get_paginator(operation),
            sdk.get_service_model("glue").operation_model(operation),
        )
