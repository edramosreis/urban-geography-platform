"""IPC framing rejects oversized, truncated and non-object messages."""

import socket
import struct
import time

import pytest
from dbt_broker_fixtures import RPC


def test_json_frame_round_trip():
    first, second = socket.socketpair()
    with first, second:
        RPC.send(first, {"sequence": 1, "value": "hello"}, time.monotonic() + 1)
        assert RPC.receive(second, time.monotonic() + 1) == {"sequence": 1, "value": "hello"}


@pytest.mark.parametrize("kind", ["oversized", "closed", "array", "deadline"])
def test_bad_frames_fail_without_waiting_for_unbounded_payloads(kind):
    first, second = socket.socketpair()
    with first, second:
        if kind == "oversized":
            first.sendall(struct.pack("!I", RPC.MAX_FRAME_BYTES + 1))
        elif kind == "closed":
            first.close()
        elif kind == "array":
            first.sendall(struct.pack("!I", 2) + b"[]")
        with pytest.raises((ValueError, EOFError, TimeoutError)):
            RPC.receive(
                second, time.monotonic() - 1 if kind == "deadline" else time.monotonic() + 1
            )
