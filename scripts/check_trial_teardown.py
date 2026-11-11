"""Join the actual cleanup receiver and readback against evolving synthetic AWS state."""

import argparse
import io
import json
import os
import sys
from contextlib import ExitStack, redirect_stdout
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import run_trial_cleanup as receiver
import trial_cleanup_operation as inventory
import trial_cleanup_session as session
import trial_data_cleanup as data
import trial_infrastructure_operation as infrastructure
import trial_quiescence as quiescence
from athena_control import canonical, write_once
from aws_login_bridge import envelope, read_record, temporary
from trial_cleanup_command import INFRA_PHASE, Operation, arguments, prepare
from trial_cleanup_inventory import validate_inventory_output
from trial_cleanup_receipt import load_completion, verify_reply
from trial_teardown_simulation import MODES, SOURCE_KEY, SimulatedError, Simulation, saved


class Client:
    """A model adapter; optional real SDK calls are always intercepted by Stubber."""

    def __init__(self, world: Simulation, snapshot: Any, service: str, raw: Any = None) -> None:
        self.world, self.snapshot, self.service, self.raw = world, snapshot, service, raw
        self.source = snapshot.access_key == SOURCE_KEY
        self.closed = 0
        self.protocol_errors: list[str] = []

    def close(self) -> None:
        self.closed += 1
        if self.raw is not None:
            self.raw.close()

    def __getattr__(self, method: str) -> Any:
        def call(**params: Any) -> dict[str, Any]:
            if self.closed:
                self.protocol_errors.append("call_after_close")
                raise ValueError("Synthetic client already closed")
            try:
                response = self.world.respond(self.service, method, params, self.source)
                failure = None
            except SimulatedError as error:
                response, failure = {}, error
            if self.raw is None:
                if failure is not None:
                    raise failure
                return response
            from botocore.exceptions import ClientError
            from botocore.stub import Stubber

            try:
                with Stubber(self.raw) as stub:
                    if failure is None:
                        stub.add_response(method, response, params)
                    else:
                        stub.add_client_error(
                            method,
                            service_error_code=failure.code,
                            http_status_code=failure.status,
                            expected_params=params,
                        )
                    try:
                        result = getattr(self.raw, method)(**params)
                    except ClientError:
                        if failure is None:
                            raise
                        stub.assert_no_pending_responses()
                        raise
                    stub.assert_no_pending_responses()
                    return result
            except ClientError:
                raise
            except Exception:
                self.protocol_errors.append(self.service + "." + method)
                raise

        return call


class Harness:
    """Replace only client factories, retaining real session/controller/receipt logic."""

    def __init__(self, world: Simulation, sdk: bool) -> None:
        self.world, self.sdk = world, sdk
        self.clients: list[Client] = []

    def bind(self, stack: ExitStack, module: Any, name: str, service: str | None = None) -> None:
        original = getattr(module, name)

        def factory(snapshot: Any, selected: str | None = None) -> Client:
            selected = service if selected is None else selected
            assert selected is not None
            raw = (
                (original(snapshot) if service is not None else original(snapshot, selected))
                if self.sdk
                else None
            )
            client = Client(self.world, snapshot, selected, raw)
            self.clients.append(client)
            return client

        stack.enter_context(patch.object(module, name, factory))

    def install(self, stack: ExitStack) -> None:
        self.bind(stack, session, "create_sts_client", "sts")
        self.bind(stack, inventory, "create_s3_client", "s3")
        self.bind(stack, inventory, "create_service_client")
        self.bind(stack, quiescence, "create_service_client")
        self.bind(stack, data, "create_s3_client", "s3")
        self.bind(stack, data, "create_service_client")
        self.bind(stack, infrastructure, "create_client")

    def phase(self, op: Operation, output: Path, *, expect_refusal: bool = False) -> dict[str, Any]:
        self.world.phase = op.phase
        prepared = prepare(op, output)
        source = temporary(
            {
                "Version": 1,
                "AccessKeyId": SOURCE_KEY,
                "SecretAccessKey": "s" * 40,
                "SessionToken": "dummyRehearsalSource",
                "AccountId": self.world.owner,
                "Expiration": (datetime.now(UTC) + timedelta(seconds=3500)).isoformat(),
            }
        )
        try:
            content = envelope(source, self.world.owner)
        finally:
            source.clear()
        argv = [
            "run_trial_cleanup.py",
            *arguments(op, output),
            "--execute",
            "--request-sha256",
            prepared["request_sha256"],
            "--credentials-stdin",
        ]
        captured = io.StringIO()
        with (
            patch.object(sys, "argv", argv),
            patch.object(
                sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content), isatty=lambda: False)
            ),
            redirect_stdout(captured),
        ):
            receiver.main()
        content = b""
        assert not self.world.unexpected, self.world.unexpected
        assert all(
            c.closed == 1
            and not c.protocol_errors
            and not c.snapshot.access_key
            and not c.snapshot.secret_key
            and not c.snapshot.token
            for c in self.clients
        ), "Client/protocol/key ownership check failed"
        reply = verify_reply(captured.getvalue().encode(), prepared["request_sha256"], op.phase)
        if expect_refusal:
            assert reply["status"] == "operation_failed" and reply["proof_sha256"] is None
            assert not (output / "transport-proof.json").exists()
            assert not (output / "session-complete.json").exists()
            failure, failure_pin = read_record(output / "attempt/failure.json")
            assert failure["stage"] == "fresh_selection" and failure["changes_attempted"] == 0
            assert failure["data_cleanup_observed"] is False
            return {
                "phase": op.phase,
                "request_sha256": prepared["request_sha256"],
                "receiver_status": "operation_failed",
                "failure_sha256": failure_pin,
                "requires_review": True,
                "data_cleanup_observed": False,
            }
        return load_completion(output, op, prepared, reply) | {"receiver_status": "phase_completed"}


def run_case(world: Simulation, output: Path, *, sdk: bool = False) -> dict[str, Any]:
    """Use fresh records at each stage and explicit resume; no automatic recovery."""
    validate_inventory_output(world.release, output)
    output.mkdir(parents=True)
    op = world.inputs(output / "inputs")
    harness = Harness(world, sdk)
    phases = []

    def execute(operation: Operation, name: str) -> tuple[dict[str, Any], Path]:
        destination = output / "phases" / name
        proof = harness.phase(
            operation,
            destination,
            expect_refusal=world.mode == "changed_target" and operation.phase == "delete-data",
        )
        phases.append(
            {
                "name": name,
                "phase": operation.phase,
                "request_sha256": proof["request_sha256"],
                "receiver_status": proof["receiver_status"],
                "requires_review": proof["requires_review"],
                "data_cleanup_observed": proof["data_cleanup_observed"],
                "infrastructure_absence_observed": proof.get(
                    "named_infrastructure_absence_observed", False
                ),
            }
        )
        return proof, destination

    with ExitStack() as stack:
        stack.enter_context(
            patch.dict(
                os.environ,
                {
                    "AWS_CONFIG_FILE": "/dev/null",
                    "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
                    "AWS_ACCESS_KEY_ID": "ambient-must-not-win",
                    "AWS_SECRET_ACCESS_KEY": "ambient-must-not-win",
                    "AWS_SESSION_TOKEN": "ambient-must-not-win",
                    "AWS_ENDPOINT_URL": "https://unapproved.invalid",
                },
            )
        )
        os.environ.pop("AWS_PROFILE", None)
        os.environ.pop("AWS_DEFAULT_PROFILE", None)
        harness.install(stack)
        initial, _ = execute(
            replace(op, phase="inventory", job=None, queries=(), paused=False), "inventory"
        )
        assert initial["requires_review"]  # Active work is expected before shutdown.
        quiet, quiet_output = execute(op, "quiesce")
        if world.mode == "unbound_work":
            assert quiet["requires_review"] and not quiet["operation_observed"]
            assert world.runs["jr_unbound"]["JobRunState"] == "RUNNING"
            assert world.counts() == {
                "catalog": 3,
                "versions": 1,
                "markers": 1,
                "objects": 1,
                "uploads": 1,
                "infrastructure": 16,
            }
        else:
            assert not quiet["requires_review"] and quiet["operation_observed"]
            prepared_op = replace(
                op,
                phase="prepare-data",
                quiet_receipt=saved(quiet_output / "attempt/receipt.json"),
                quiet_completion=saved(quiet_output / "session-complete.json"),
            )
            prepared, prepared_output = execute(prepared_op, "prepare-data")
            assert not prepared["requires_review"] and prepared["prepared_plan_sha256"]
            approved = saved(prepared_output / "attempt/plan.json")
            delete_op = replace(prepared_op, phase="delete-data", approved=approved)
            if world.mode == "changed_target":
                world.versions.append(world.versions[0] | {"VersionId": "new-unapproved-version"})
            deleted, _ = execute(delete_op, "delete-data")
            if world.mode == "partial_delete":
                assert deleted["requires_review"] and not deleted["data_cleanup_observed"]
                assert world.counts() == {
                    "catalog": 0,
                    "versions": 1,
                    "markers": 1,
                    "objects": 1,
                    "uploads": 0,
                    "infrastructure": 16,
                }
                deleted, _ = execute(replace(delete_op, resume=True), "resume-data")
                assert phases[-1]["request_sha256"] != phases[-2]["request_sha256"]
                assert saved(prepared_output / "attempt/plan.json") == approved
            if world.mode == "changed_target":
                assert deleted["requires_review"] and not deleted["data_cleanup_observed"]
                assert world.counts() == {
                    "catalog": 3,
                    "versions": 2,
                    "markers": 1,
                    "objects": 1,
                    "uploads": 1,
                    "infrastructure": 16,
                }
                assert not any(
                    r["method"] in {"delete_table", "delete_object", "abort_multipart_upload"}
                    for r in world.journal
                )
            else:
                assert deleted["data_cleanup_observed"] and not deleted["requires_review"]
                world.remove_infrastructure(deleted)
                inspected, _ = execute(
                    replace(op, phase=INFRA_PHASE, job=None, queries=(), paused=False), INFRA_PHASE
                )
                absent = world.mode in {"clean", "partial_delete"}
                assert inspected["named_infrastructure_absence_observed"] is absent
                assert inspected["requires_review"] is (not absent)
                assert not inspected["data_cleanup_observed"]
                assert all(
                    r["identity"] == "source" and r["method"] != "assume_role"
                    for r in world.journal
                    if r["phase"] == INFRA_PHASE
                )
    assert not world.unexpected
    for path in output.rglob("*.json"):
        content = path.read_bytes()
        assert all(
            secret not in content
            for secret in (
                SOURCE_KEY.encode(),
                b"ASIA" + b"B" * 16,
                b"s" * 40,
                b"dummyRehearsalSource",
                b"dummyTeardownToken",
            )
        ), "Dummy credential persisted"
    return {
        "scenario": world.mode,
        "expected_outcome_verified": True,
        "synthetic_inputs": True,
        "sdk_stubber_used": sdk,
        "phases": phases,
        "clients_closed": len(harness.clients),
        "owned_keys_cleared": True,
        "simulated_calls": len(world.journal),
        "journal": world.journal,
        "remaining": world.counts(),
        "infrastructure_transition_simulated": world.removed,
        "terraform_invoked": False,
        "live_aws_requests": 0,
        "live_teardown_verified": False,
    }


def check(release: Path, pin: str, principal: str, output: Path) -> dict[str, Any]:
    validate_inventory_output(release, output)
    # Validate retained inputs before creating the rehearsal directory or SDK clients.
    Simulation(release, pin, principal, "clean")
    output.mkdir(parents=True)
    cases = {}
    for mode in MODES:
        result = run_case(Simulation(release, pin, principal, mode), output / mode, sdk=True)
        write_once(output / mode / "rehearsal.json", canonical(result))
        cases[mode] = {k: v for k, v in result.items() if k != "journal"}
    result = {
        "status": "offline_joined_teardown_rehearsed",
        "cases": cases,
        "live_aws_requests": 0,
        "terraform_invoked": False,
        "live_teardown_verified": False,
    }
    write_once(output / "rehearsal.json", canonical(result))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = check(args.release, args.release_sha256, args.source_role_arn, args.output)
    print(
        json.dumps(
            {
                "status": result["status"],
                "scenarios": len(result["cases"]),
                "live_aws_requests": 0,
                "terraform_invoked": False,
            }
        )
    )


if __name__ == "__main__":
    main()
