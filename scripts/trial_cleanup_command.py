"""Explicit phase inputs and portable pins for the existing bounded cleanup operations."""

import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from athena_control import canonical
from aws_login_bridge import read_record
from trial_cleanup_operation import prepare_operation
from trial_cleanup_plan import CleanupEvidence, context, load_approved, require_paused
from trial_quiescence import check_output
from trial_quiescence_plan import QueryEvidence, Record, evidence_records, quiescence_plan

PHASES = ("inventory", "quiesce", "prepare-data", "delete-data")


@dataclass(frozen=True)
class Operation:
    release: Path
    release_pin: str
    inventory: Record
    principal: str
    phase: str
    job: Record | None = None
    queries: tuple[QueryEvidence, ...] = ()
    quiet_receipt: Record | None = None
    quiet_completion: Record | None = None
    approved: Record | None = None
    paused: bool = False
    resume: bool = False

    def evidence(self) -> CleanupEvidence:
        if self.quiet_receipt is None or self.quiet_completion is None:
            raise ValueError("Data cleanup requires explicit scoped quiescence records")
        return CleanupEvidence(
            self.inventory, self.quiet_receipt, self.quiet_completion, self.job, self.queries
        )

    def records(self) -> list[Record]:
        return [self.inventory, *evidence_records(self.job, self.queries)] + [
            r for r in (self.quiet_receipt, self.quiet_completion, self.approved) if r is not None
        ]


def prepare(operation: Operation, output: Path) -> dict[str, Any]:
    """Local-only validation; no credentials, SDK calls, directory creation or deletion."""
    op = operation
    if (
        op.phase not in PHASES
        or type(op.paused) is not bool
        or type(op.resume) is not bool
        or (op.resume and op.phase != "delete-data")
        or ((op.approved is not None) != (op.phase == "delete-data"))
        or (
            op.phase in {"inventory", "quiesce"}
            and (op.quiet_receipt is not None or op.quiet_completion is not None)
        )
        or (op.phase == "inventory" and (op.job is not None or op.queries or op.paused))
    ):
        raise ValueError("Cleanup phase and explicit evidence/options disagree")
    if op.phase != "inventory":
        require_paused(op.paused)
    scope, identity = prepare_operation(
        op.release, op.release_pin, op.inventory.path, op.inventory.sha256, op.principal, output
    )
    check_output(op.release, op.job, op.queries, output)
    for record in op.records():
        _, pin = read_record(record.path)
        if pin != record.sha256:
            raise ValueError("Explicit cleanup record checksum differs")
        protected, destination = record.path.parent.resolve(), output.resolve()
        if destination.is_relative_to(protected) or protected.is_relative_to(destination):
            raise ValueError("Keep cleanup output separate from every input record")
    if op.phase == "inventory":
        payload = {"scope": scope, "include_services": True}
    elif op.phase == "quiesce":
        payload = quiescence_plan(op.release, op.release_pin, op.job, op.queries)
    else:
        payload = context(op.release, op.release_pin, op.evidence())
        if op.approved is not None:
            load_approved(op.release, op.release_pin, op.evidence(), op.approved)
    recipe = {
        "version": 1,
        "phase": op.phase,
        "release_manifest_sha256": op.release_pin,
        "inventory_plan_sha256": op.inventory.sha256,
        "source_role_arn": op.principal,
        "payload": payload,
        "approved_plan_sha256": None if op.approved is None else op.approved.sha256,
        "coordinator_paused_assertion": op.paused,
        "resume": op.resume,
    }
    return {
        "recipe": recipe,
        "request_sha256": hashlib.sha256(canonical(recipe)).hexdigest(),
        "identity": identity,
        "scope": scope,
    }


def checked(operation: Operation, output: Path, pin: str | None) -> dict[str, Any]:
    value = prepare(operation, output)
    if not isinstance(pin, str) or value["request_sha256"] != pin:
        raise ValueError("Execution requires the reviewed phase request SHA256")
    return value


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--phase", choices=PHASES, required=True)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--inventory-plan", nargs=2, metavar=("PATH", "SHA256"), required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-receipt", nargs=2, metavar=("PATH", "SHA256"))
    parser.add_argument("--fact-query", nargs=4, metavar=("INTENT", "PIN", "RECEIPT", "PIN"))
    parser.add_argument(
        "--view-query",
        nargs=6,
        action="append",
        default=[],
        metavar=("INTENT", "PIN", "RECEIPT", "PIN", "PARENT", "PIN"),
    )
    for flag in ("quiescence-receipt", "quiescence-completion", "approved-plan"):
        parser.add_argument("--" + flag, nargs=2, metavar=("PATH", "SHA256"))
    parser.add_argument("--coordinator-paused", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--request-sha256")
    parser.add_argument("--execute", action="store_true")


def from_arguments(args: argparse.Namespace) -> Operation:
    def record(value: list[str] | None) -> Record | None:
        return None if value is None else Record(Path(value[0]), value[1])

    inventory = record(args.inventory_plan)
    assert inventory is not None
    queries = []
    if args.fact_query:
        queries.append(
            QueryEvidence(
                Record(Path(args.fact_query[0]), args.fact_query[1]),
                Record(Path(args.fact_query[2]), args.fact_query[3]),
            )
        )
    if len(args.view_query) > 2:
        raise ValueError("The first trial permits only two explicit view-query records")
    for row in args.view_query:
        queries.append(
            QueryEvidence(
                Record(Path(row[0]), row[1]),
                Record(Path(row[2]), row[3]),
                Record(Path(row[4]), row[5]),
            )
        )
    return Operation(
        args.release,
        args.release_sha256,
        inventory,
        args.source_role_arn,
        args.phase,
        record(args.job_receipt),
        tuple(queries),
        record(args.quiescence_receipt),
        record(args.quiescence_completion),
        record(args.approved_plan),
        args.coordinator_paused,
        args.resume,
    )


def arguments(op: Operation, output: Path, *, posix: bool = False) -> list[str]:
    """Explicit fixed flags only; paths are never discovered from receipt contents."""

    def path(value: Path) -> str:
        return value.as_posix() if posix else str(value)

    result = [
        "--phase",
        op.phase,
        "--release",
        path(op.release),
        "--release-sha256",
        op.release_pin,
        "--inventory-plan",
        path(op.inventory.path),
        op.inventory.sha256,
        "--source-role-arn",
        op.principal,
        "--output",
        path(output),
    ]
    for flag, value in [
        ("job-receipt", op.job),
        ("quiescence-receipt", op.quiet_receipt),
        ("quiescence-completion", op.quiet_completion),
        ("approved-plan", op.approved),
    ]:
        if value is not None:
            result += ["--" + flag, path(value.path), value.sha256]
    for query in op.queries:
        result += [
            "--fact-query" if query.parent is None else "--view-query",
            path(query.intent.path),
            query.intent.sha256,
            path(query.receipt.path),
            query.receipt.sha256,
        ]
        if query.parent is not None:
            result += [path(query.parent.path), query.parent.sha256]
    if op.paused:
        result.append("--coordinator-paused")
    if op.resume:
        result.append("--resume")
    return result
