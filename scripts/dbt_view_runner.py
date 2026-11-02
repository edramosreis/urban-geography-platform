"""Join source/caller proof to owned dbt view control; no live execution CLI."""

import hashlib
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from athena_clients import query_resources
from athena_control import canonical, write_once
from athena_preflight import caller_identity, catalog_state, expected_caller, verify_query_source
from athena_read_plan import same
from athena_reader import WORKER_PYTHON, ReaderProcess
from athena_reader_worker import verified_jars
from athena_runner import MAX_PLAN_BYTES, check_inputs, pinned_record
from dbt_test_control import TestController
from dbt_test_plan import TEST_NAMES, test_plan
from dbt_view_catalog import observe_views
from dbt_view_control import ViewController
from dbt_view_plan import VIEWS, view_build_plan
from prepare_dbt_project import ARTIFACTS

ModelExecutor = Callable[[ViewController, Path], dict[str, Any]]


def local_plan(
    project: Path,
    output: Path,
    *,
    target: dict[str, str],
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    caller_arn: str,
    caller_user_id: str,
) -> dict[str, Any]:
    plan = view_build_plan(project, **target)
    source = plan["source_plan"]
    expected_caller(source["target"]["account_id"], caller_arn, caller_user_id)
    check_inputs(source, input_path, writer_receipt, writer_receipt_sha256)
    resolved = output.resolve()
    protected = (project.resolve(), input_path.resolve(), writer_receipt.parent.resolve())
    if (
        output.exists()
        or output.is_symlink()
        or resolved == ARTIFACTS.resolve()
        or not resolved.is_relative_to(ARTIFACTS.resolve())
        or any(resolved.is_relative_to(p) or p.is_relative_to(resolved) for p in protected)
    ):
        raise ValueError(
            "Use fresh run output under artifacts, separate from project/input/receipt"
        )
    return plan


def save_record(path: Path, value: dict[str, Any]) -> str:
    content = canonical(value)
    if len(content) > MAX_PLAN_BYTES:
        raise ValueError("View runner record exceeds its byte ceiling")
    write_once(path, content)
    return hashlib.sha256(content).hexdigest()


def run_view_build(
    clients: dict[str, Any],
    project: Path,
    output: Path,
    *,
    target: dict[str, str],
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    caller_arn: str,
    caller_user_id: str,
    snapshot_reader: Callable[..., dict[str, Any]],
    execute_models: ModelExecutor,
    execute_tests: ModelExecutor | None = None,
    view_readback: bool = False,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Injected reader/executor; prove source before queries and reobserve around the build."""
    plan = local_plan(
        project,
        output,
        target=target,
        input_path=input_path,
        writer_receipt=writer_receipt,
        writer_receipt_sha256=writer_receipt_sha256,
        caller_arn=caller_arn,
        caller_user_id=caller_user_id,
    )
    if not callable(snapshot_reader) or not callable(execute_models):
        raise ValueError("Explicit snapshot reader and model executor are required")
    if type(view_readback) is not bool:
        raise ValueError("view_readback must be an explicit boolean")
    output = output.resolve()
    source_plan = plan["source_plan"]
    expected = expected_caller(source_plan["target"]["account_id"], caller_arn, caller_user_id)
    intent = {
        "contract_version": 1,
        "view_plan": plan,
        "view_readback": view_readback,
        "data_test_plan": test_plan(project, **target) if execute_tests is not None else None,
        "input_path": str(input_path.resolve()),
        "writer_receipt": str(writer_receipt.resolve()),
        "writer_receipt_sha256": writer_receipt_sha256,
        "expected_caller": expected,
    }
    output.mkdir(parents=True)
    intent_pin = save_record(output / "intent.json", intent)
    stage = "source_preflight"
    initial_verified = False
    control: ViewController | None = None
    tests: TestController | None = None
    initial_view_pin = final_view_pin = None
    try:
        source = verify_query_source(
            clients["sts"],
            clients["glue"],
            source_plan,
            caller_arn=caller_arn,
            caller_user_id=caller_user_id,
            input_path=input_path,
            writer_receipt=writer_receipt,
            writer_receipt_sha256=writer_receipt_sha256,
            snapshot_reader=snapshot_reader,
        )
        source_pin = save_record(
            output / "source-verification.json", {"run_intent_sha256": intent_pin, "source": source}
        )
        initial_verified = True
        gate_count = 0

        def gate(name: str) -> str:
            if not same(pinned_record(output / "intent.json", intent_pin), intent):
                raise ValueError("View runner intent changed")
            pinned_record(output / "source-verification.json", source_pin)
            if not same(view_build_plan(project, **target), plan):
                raise ValueError("Prepared view project changed after source readback")
            check_inputs(source_plan, input_path, writer_receipt, writer_receipt_sha256)
            caller = caller_identity(clients["sts"], expected)
            catalog = catalog_state(clients["glue"], source_plan)
            if not same(catalog, source["catalog"]):
                raise ValueError("Source catalog changed after snapshot readback")
            # Detect local changes made during the current service observations too.
            pinned_record(output / "intent.json", intent_pin)
            pinned_record(output / "source-verification.json", source_pin)
            check_inputs(source_plan, input_path, writer_receipt, writer_receipt_sha256)
            if not same(view_build_plan(project, **target), plan):
                raise ValueError("Prepared view project changed during source gate")
            return save_record(
                output / name,
                {
                    "run_intent_sha256": intent_pin,
                    "source_verification_sha256": source_pin,
                    "caller": caller,
                    "catalog": catalog,
                },
            )

        def before_submit() -> None:
            nonlocal gate_count
            gate(f"submission-gate-{gate_count + 1:02}.json")
            gate_count += 1

        stage = "view_execution"
        control = ViewController(
            clients["athena"],
            project,
            output / "queries",
            **source_plan["target"],
            now=now,
            pause=pause,
            before_submit=before_submit,
        )
        with control.session():
            result = execute_models(control, output / "dbt")
        stage = "model_outcome"
        expected_result = {"success": True, "model_statuses": {name: "success" for name in VIEWS}}
        if (
            not same(result, expected_result)
            or control.report()["status"] != "view_queries_completed"
        ):
            raise ValueError("Model executor or controlled view queries did not complete")
        save_record(output / "model-outcome.json", result)
        if view_readback:
            stage = "view_catalog_readback"
            initial_view_pin = save_record(
                output / "view-catalog-initial.json",
                {"run_intent_sha256": intent_pin, "views": observe_views(clients["glue"], plan)},
            )
        if execute_tests is not None:
            stage = "data_tests"
            tests = TestController(
                clients["athena"],
                project,
                output / "tests",
                **source_plan["target"],
                now=now,
                pause=pause,
                before_submit=before_submit,
            )
            with tests.session():
                test_result = execute_tests(tests, output / "dbt-tests")
            save_record(output / "test-outcome.json", test_result)
            expected_tests = {
                "success": True,
                "test_statuses": {name: "pass" for name in TEST_NAMES},
                "failure_counts": {name: 0 for name in TEST_NAMES},
            }
            if (
                not same(test_result, expected_tests)
                or tests.report()["status"] != "data_tests_passed"
            ):
                raise ValueError("dbt data tests did not all pass")
        if view_readback:
            stage = "final_view_catalog_readback"
            assert initial_view_pin is not None
            initial_views = pinned_record(output / "view-catalog-initial.json", initial_view_pin)
            final_views = observe_views(clients["glue"], plan)
            if not same(initial_views["views"], final_views):
                raise ValueError("View catalog changed after initial readback")
            final_view_pin = save_record(
                output / "view-catalog-final.json",
                {
                    "run_intent_sha256": intent_pin,
                    "initial_view_sha256": initial_view_pin,
                    "views": final_views,
                },
            )
        stage = "final_source_gate"
        final_pin = gate("final-gate.json")
        if view_readback:
            assert initial_view_pin is not None and final_view_pin is not None
            pinned_record(output / "view-catalog-initial.json", initial_view_pin)
            pinned_record(output / "view-catalog-final.json", final_view_pin)
        report = {
            "status": "source_checked_view_build",
            "run_intent_sha256": intent_pin,
            "source_verification_sha256": source_pin,
            "final_gate_sha256": final_pin,
            "binding": source_plan["binding"],
            "caller_identity_verified": True,
            "source_readback_verified": True,
            "source_catalog_stable": True,
            "controlled_view_queries_completed": True,
            "models_succeeded": True,
            "data_tests_executed": tests.report()["data_tests_executed"] if tests else 0,
            "data_tests_passed": tests is not None,
            "test_control": tests.report() if tests else None,
            "view_catalog_verified": view_readback,
            "view_catalog_initial_sha256": initial_view_pin,
            "view_catalog_final_sha256": final_view_pin,
            "result_verified": False,
            "writer_job_identity_verified": False,
            "query_control": control.report(),
            "scope": (
                "Observed caller/source and controlled model completion; "
                "view catalog is checked only when opted in; "
                "Gold result acceptance remains separate"
            ),
        }
        save_record(output / "receipt.json", report)
        return report
    except BaseException as error:
        # Query control owns cancellation. Never start another query or stop it twice.
        try:
            save_record(
                output / "failure.json",
                {
                    "run_intent_sha256": intent_pin,
                    "stage": stage,
                    "error_type": type(error).__name__,
                    "initial_source_verified": initial_verified,
                    "result_verified": False,
                    "query_control": control.report() if control else None,
                    "test_control": tests.report() if tests else None,
                    "view_catalog_verified": False,
                    "view_catalog_initial_sha256": initial_view_pin,
                    "view_catalog_final_sha256": final_view_pin,
                },
            )
        except Exception:
            pass
        raise


def run_owned_views(
    project: Path,
    output: Path,
    *,
    target: dict[str, str],
    input_path: Path,
    writer_receipt: Path,
    writer_receipt_sha256: str,
    caller_arn: str,
    caller_user_id: str,
    execute_models: ModelExecutor | None = None,
    data_tests: bool = False,
    view_readback: bool = False,
    session: Any = None,
    reader_factory: Callable[..., Any] = ReaderProcess,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Own frozen clients, isolated reader and the default supervised dbt executor."""
    plan = local_plan(
        project,
        output,
        target=target,
        input_path=input_path,
        writer_receipt=writer_receipt,
        writer_receipt_sha256=writer_receipt_sha256,
        caller_arn=caller_arn,
        caller_user_id=caller_user_id,
    )
    if type(view_readback) is not bool:
        raise ValueError("view_readback must be an explicit boolean")
    if type(data_tests) is not bool or (data_tests and execute_models is not None):
        raise ValueError("Data tests require the default supervised model executor")
    if (execute_models is not None and not callable(execute_models)) or not callable(
        reader_factory
    ):
        raise ValueError("Explicit model executor and reader factory are required")
    if reader_factory is ReaderProcess:
        if os.name != "posix" or not Path(WORKER_PYTHON).is_file():
            raise ValueError("Use the existing Linux Docker worker runtime")
        verified_jars()  # No credentials or AWS calls if the local reader runtime is stale.
    if execute_models is None:
        from dbt_model_executor import ModelExecutor as ProcessExecutor
        from dbt_model_worker import tooling_ready

        tooling_ready()
    with query_resources(session=session) as resources:
        executor = ProcessExecutor(resources.clients) if execute_models is None else execute_models
        reader = reader_factory(
            output / "reader", resources.reader_environment, plan["source_plan"]["target"]
        )
        return run_view_build(
            resources.clients,
            project,
            output,
            target=target,
            input_path=input_path,
            writer_receipt=writer_receipt,
            writer_receipt_sha256=writer_receipt_sha256,
            caller_arn=caller_arn,
            caller_user_id=caller_user_id,
            snapshot_reader=reader,
            execute_models=executor,
            execute_tests=ProcessExecutor(resources.clients) if data_tests else None,
            view_readback=view_readback,
            now=now,
            pause=pause,
        )
