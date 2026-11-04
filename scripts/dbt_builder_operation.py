"""Connect a scoped builder snapshot to the existing bounded two-view dbt runner."""

import hashlib
import os
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from athena_clients import query_resources
from athena_control import STOP_SECONDS, WATCH_SECONDS, canonical
from athena_preflight import expected_caller
from athena_read_plan import ARTIFACTS, same
from athena_reader import READER_SECONDS, WORKER_PYTHON, ReaderProcess
from athena_reader_worker import verified_jars
from athena_runner import check_inputs, pinned_record
from aws_login_bridge import BridgeError
from aws_role_session import LifetimeClient
from dbt_builder_session import (
    BUILDER_REQUIRED_SECONDS,
    builder_credentials,
    builder_plan,
    load_builder_plan,
)
from dbt_model_executor import ModelExecutor
from dbt_model_worker import tooling_ready
from dbt_view_runner import run_view_build, save_record

READ_SECONDS = 90
MODEL_SECONDS = 720
FINAL_RESERVE_SECONDS = 180
QUERY_SECONDS = WATCH_SECONDS + STOP_SECONDS + 180
CALL_SECONDS = {
    "sts": {"get_caller_identity": READ_SECONDS},
    "glue": {
        "get_table": READ_SECONDS,
        "get_databases": READ_SECONDS,
        "get_tables": READ_SECONDS,
        "get_table_versions": READ_SECONDS,
        "delete_table_version": READ_SECONDS,
    },
    "athena": {
        "get_work_group": READ_SECONDS,
        "start_query_execution": QUERY_SECONDS,
        "get_query_execution": 60,
        "stop_query_execution": 60,
        "get_query_results": READ_SECONDS,
    },
}


def prepare_operation(
    project: Path,
    plan_path: Path,
    plan_sha256: str,
    principal_arn: str,
    *,
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    output: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Check pinned project/plan/source and fresh output before credentials or SDKs."""
    plan = load_builder_plan(project, plan_path, plan_sha256)
    identity = builder_plan(project, plan_path, plan_sha256, principal_arn)
    check_inputs(plan["source_plan"], input_path, writer_receipt, writer_receipt_sha256)
    destination = output.resolve()
    source = input_path.resolve()
    protected = [
        project.resolve(),
        source,
        writer_receipt.parent.resolve(),
        plan_path.parent.resolve(),
    ]
    if (source.parent / "release.json").is_file():
        protected.append(source.parent)
    if (
        output.is_symlink()
        or output.exists()
        or destination == ARTIFACTS.resolve()
        or not destination.is_relative_to(ARTIFACTS.resolve())
        or any(destination.is_relative_to(p) or p.is_relative_to(destination) for p in protected)
    ):
        raise ValueError("Keep fresh artifacts output separate from project, inputs and plans")
    return plan, identity


def _caller(identity: dict[str, Any], plan: dict[str, Any]) -> dict[str, str]:
    try:
        arn, user_id = identity.get("Arn"), identity.get("UserId")
        if not isinstance(arn, str) or not isinstance(user_id, str):
            raise ValueError("missing caller fields")
        caller = expected_caller(plan["owner"], arn, user_id)
        pattern = (
            re.escape(f"arn:aws:sts::{plan['owner']}:assumed-role/{plan['role_name']}/")
            + r"dbt-builder-[0-9a-f]{32}"
        )
        if identity.get("Account") != plan["owner"] or not re.fullmatch(pattern, caller["Arn"]):
            raise ValueError("different scoped builder")
    except ValueError:
        raise BridgeError("builder_query_identity_differs") from None
    return caller


def run_operation(
    project: Path,
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
    """Build exactly two views; no ambient credentials, refresh, retry or live CLI."""
    plan, identity = prepare_operation(
        project,
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
    tooling_ready()

    def unchanged() -> None:
        if not same(load_builder_plan(project, plan_path, plan_sha256), plan):
            raise BridgeError("builder_view_plan_changed")
        check_inputs(plan["source_plan"], input_path, writer_receipt, writer_receipt_sha256)

    acquisition = builder_credentials(project, plan_path, plan_sha256, principal_arn, content)
    content = b""
    with acquisition as snapshot:
        snapshot.require_operation(BUILDER_REQUIRED_SECONDS)
        unchanged()
        with query_resources(snapshot=snapshot) as resources:
            clients = {
                service: LifetimeClient(client, snapshot, CALL_SECONDS[service])
                for service, client in resources.clients.items()
            }
            caller = _caller(clients["sts"].get_caller_identity(), identity)
            unchanged()
            reader = ReaderProcess(
                output / "reader", resources.reader_environment, plan["source_plan"]["target"]
            )
            executor = ModelExecutor(clients, max_seconds=MODEL_SECONDS)

            def read_snapshot(*args: Any, **kwargs: Any) -> dict[str, Any]:
                unchanged()
                snapshot.require_operation(READER_SECONDS + MODEL_SECONDS + FINAL_RESERVE_SECONDS)
                result = reader(*args, **kwargs)
                snapshot.require_operation(READ_SECONDS)
                return result

            def execute_models(*args: Any, **kwargs: Any) -> dict[str, Any]:
                unchanged()
                snapshot.require_operation(MODEL_SECONDS + FINAL_RESERVE_SECONDS)
                result = executor(*args, **kwargs)
                snapshot.require_operation(READ_SECONDS)
                unchanged()
                return result

            report = run_view_build(
                clients,
                project,
                output,
                target=plan["source_plan"]["target"],
                input_path=input_path,
                writer_receipt=writer_receipt,
                writer_receipt_sha256=writer_receipt_sha256,
                caller_arn=caller["Arn"],
                caller_user_id=caller["UserId"],
                snapshot_reader=read_snapshot,
                execute_models=execute_models,
                now=now,
                pause=pause,
            )
            unchanged()
    # Publish the outer binding only after client closure and scoped credential cleanup.
    unchanged()
    receipt_pin = hashlib.sha256(canonical(report)).hexdigest()
    if not same(pinned_record(output / "receipt.json", receipt_pin), report):
        raise ValueError("Saved view receipt differs from the completed operation")
    save_record(
        output / "builder-operation.json",
        {
            "version": 1,
            "status": "scoped_view_build_completed",
            "view_plan_sha256": plan_sha256,
            "project_receipt_sha256": plan["project_receipt_sha256"],
            "writer_receipt_sha256": writer_receipt_sha256,
            "binding": plan["source_plan"]["binding"],
            "caller": caller,
            "run_intent_sha256": report["run_intent_sha256"],
            "receipt_sha256": receipt_pin,
        },
    )
    return report
