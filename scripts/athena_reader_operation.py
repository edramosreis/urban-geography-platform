"""Join one scoped reader session to the existing source/query/result workflow."""

import os
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from athena_clients import query_resources
from athena_control import STOP_SECONDS, WATCH_SECONDS, Attempt
from athena_preflight import expected_caller
from athena_read_plan import ARTIFACTS, same
from athena_reader import READER_SECONDS, WORKER_PYTHON, ReaderProcess
from athena_reader_session import READER_REQUIRED_SECONDS, reader_credentials, reader_plan
from athena_reader_worker import verified_jars
from athena_runner import check_inputs, load_plan, run_trial
from aws_login_bridge import BridgeError, TemporaryCredentials

READ_SECONDS = 90
QUERY_SECONDS = WATCH_SECONDS + STOP_SECONDS + 180
CALL_SECONDS = {
    "sts": {"get_caller_identity": READ_SECONDS},
    "glue": {"get_table": READ_SECONDS},
    "athena": {
        "get_work_group": READ_SECONDS,
        "start_query_execution": QUERY_SECONDS,
        "get_query_execution": 60,
        "stop_query_execution": 60,
    },
    "s3": {"head_object": READ_SECONDS, "get_object": READ_SECONDS},
}


class _LifetimeClient:
    """Check at invocation, including when a caller has retained a bound method."""

    def __init__(self, client: Any, snapshot: TemporaryCredentials, service: str) -> None:
        self._client = client
        self._snapshot = snapshot
        self._calls = CALL_SECONDS[service]

    def __getattr__(self, name: str) -> Any:
        if name not in self._calls:
            raise AttributeError(name)

        def invoke(**kwargs: Any) -> Any:
            self._snapshot.require_operation(self._calls[name])
            return getattr(self._client, name)(**kwargs)

        return invoke


def _caller(identity: dict[str, Any], plan: dict[str, Any]) -> dict[str, str]:
    try:
        arn, user_id = identity.get("Arn"), identity.get("UserId")
        if not isinstance(arn, str) or not isinstance(user_id, str):
            raise ValueError("missing caller fields")
        caller = expected_caller(plan["owner"], arn, user_id)
        pattern = (
            re.escape(f"arn:aws:sts::{plan['owner']}:assumed-role/{plan['role_name']}/")
            + r"athena-reader-[0-9a-f]{32}"
        )
        if identity.get("Account") != plan["owner"] or not re.fullmatch(pattern, caller["Arn"]):
            raise ValueError("different scoped reader")
    except ValueError:
        raise BridgeError("reader_query_identity_differs") from None
    return caller


def prepare_operation(
    plan_path: Path,
    plan_sha256: str,
    principal_arn: str,
    *,
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    output: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate local inputs/output without credentials, SDKs or a Linux runtime."""
    plan = load_plan(plan_path, plan_sha256)
    identity_plan = reader_plan(plan_path, plan_sha256, principal_arn)
    check_inputs(plan, input_path, writer_receipt, writer_receipt_sha256)
    destination = output.resolve()
    source = input_path.resolve()
    protected = [source, writer_receipt.parent.resolve(), plan_path.parent.resolve()]
    if (source.parent / "release.json").is_file():
        protected.append(source.parent)
    if (
        output.is_symlink()
        or output.exists()
        or destination == ARTIFACTS.resolve()
        or not destination.is_relative_to(ARTIFACTS.resolve())
        or any(destination.is_relative_to(p) or p.is_relative_to(destination) for p in protected)
    ):
        raise ValueError(
            "Keep fresh artifacts output outside input, receipt, plan and frozen release folders"
        )
    return plan, identity_plan


def run_operation(
    plan_path: Path,
    plan_sha256: str,
    principal_arn: str,
    content: bytes,
    *,
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    output: Path,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """No credential discovery, role refresh, second submission or query recovery loop."""
    plan, identity_plan = prepare_operation(
        plan_path,
        plan_sha256,
        principal_arn,
        input_path=input_path,
        writer_receipt=writer_receipt,
        writer_receipt_sha256=writer_receipt_sha256,
        output=output,
    )
    if os.name != "posix" or not Path(WORKER_PYTHON).is_file():
        raise ValueError("Use the existing Linux Docker analytics/reader runtime")
    verified_jars()
    acquisition = reader_credentials(plan_path, plan_sha256, principal_arn, content)
    content = b""
    with acquisition as snapshot:
        snapshot.require_operation(READER_REQUIRED_SECONDS)
        with query_resources(snapshot=snapshot, include_results=True) as resources:
            clients = {
                service: _LifetimeClient(client, snapshot, service)
                for service, client in resources.clients.items()
            }
            caller = _caller(clients["sts"].get_caller_identity(), identity_plan)
            if not same(load_plan(plan_path, plan_sha256), plan):
                raise BridgeError("reader_query_plan_changed")
            attempt = Attempt(output, plan)
            reader = ReaderProcess(output / "reader", resources.reader_environment, plan["target"])

            def read_snapshot(*args: Any, **kwargs: Any) -> dict[str, Any]:
                snapshot.require_operation(READER_SECONDS + QUERY_SECONDS)
                report = reader(*args, **kwargs)
                snapshot.require_operation(READ_SECONDS)
                return report

            return run_trial(
                clients,
                attempt,
                input_path=input_path,
                writer_receipt=writer_receipt,
                writer_receipt_sha256=writer_receipt_sha256,
                caller_arn=caller["Arn"],
                caller_user_id=caller["UserId"],
                snapshot_reader=read_snapshot,
                now=now,
                pause=pause,
            )
