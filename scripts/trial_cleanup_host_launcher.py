"""Local-only planning or one supervised Windows-to-Docker cleanup phase handoff."""

import argparse
import json
import re
import shutil
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

import aws_login_bridge as bridge
from athena_control import canonical, write_once
from aws_role_session import SOURCE_REQUIRED_SECONDS
from trial_cleanup_command import (
    INFRA_PHASE,
    Operation,
    add_arguments,
    arguments,
    checked,
    from_arguments,
    prepare,
)
from trial_cleanup_receipt import REPLY_BYTES, load_completion, verify_reply
from trial_quiescence_plan import QueryEvidence, Record

SUPERVISOR_SECONDS = 2100
CONTAINER_RELEASE = Path("/workspace/artifacts/frozen-release")
CONTAINER_OUTPUT = Path("/workspace/artifacts/cleanup-operation/attempt")


def prepare_launch(
    op: Operation, sdk: Path, output: Path, pin: str | None = None
) -> dict[str, Any]:
    prepared = prepare(op, output) if pin is None else checked(op, output, pin)
    sdk = bridge.sdk_directory(sdk, "analytics")
    protected = [op.release.resolve(), output.resolve(), *(r.path.resolve() for r in op.records())]
    if any(p.is_relative_to(sdk) or sdk.is_relative_to(p) for p in protected):
        raise bridge.BridgeError("cleanup_sdk_overlap")
    return prepared


def _remap(op: Operation) -> tuple[Operation, list[tuple[Path, str, bool]]]:
    mounts = [(op.release.resolve(), CONTAINER_RELEASE.as_posix(), True)]
    mapping: dict[Record, Record] = {}
    for record in op.records():
        if record not in mapping:
            target = Path(f"/workspace/artifacts/cleanup-inputs/record-{len(mapping):02}.json")
            mapping[record] = Record(target, record.sha256)
            mounts.append((record.path.resolve(), target.as_posix(), True))

    def mapped(record: Record | None) -> Record | None:
        return None if record is None else mapping[record]

    return replace(
        op,
        release=CONTAINER_RELEASE,
        inventory=mapping[op.inventory],
        job=mapped(op.job),
        queries=tuple(
            QueryEvidence(mapping[q.intent], mapping[q.receipt], mapped(q.parent))
            for q in op.queries
        ),
        quiet_receipt=mapped(op.quiet_receipt),
        quiet_completion=mapped(op.quiet_completion),
        approved=mapped(op.approved),
    ), mounts


def docker_command(
    docker: list[str],
    sdk: Path,
    image: str,
    name: str,
    op: Operation,
    output: Path,
    pin: str,
    *,
    offline: bool = False,
) -> list[str]:
    prepare_launch(op, sdk, output, pin)
    command = bridge.docker_command(docker, "analytics", sdk, image, name, offline=offline)
    lock_mount = next(
        i
        for i, value in enumerate(command)
        if value.endswith(",target=/workspace/analytics/athena/uv.lock,readonly")
    )
    del command[lock_mount - 1 : lock_mount + 1]
    mapped, mounts = _remap(op)
    mounts.append((bridge.ROOT / "analytics/athena", "/workspace/analytics/athena", True))
    mounts.append((output.resolve(), "/workspace/artifacts/cleanup-operation", False))
    extra = []
    for source, target, readonly in mounts:
        if "," in str(source):
            raise bridge.BridgeError("invalid_mount_path")
        extra += [
            "--mount",
            f"type=bind,source={source},target={target}" + (",readonly" if readonly else ""),
        ]
    command[command.index(image) : command.index(image)] = extra
    if offline:
        command.pop()
    if command[-3:] != ["/workspace/scripts/aws_login_probe.py", "--sdk", "analytics"]:
        raise bridge.BridgeError("unexpected_base_container_command")
    # Release validation needs the image's PyArrow; use the pinned analytics SDK via PYTHONPATH.
    command[command.index(image) + 1] = "/opt/venv/bin/python"
    index = next(i for i, value in enumerate(command) if value.startswith("PYTHONPATH="))
    command[index] = (
        "PYTHONPATH=/tooling/lib/python3.11/site-packages:/workspace/src:/workspace/scripts"
    )
    command[-3:] = [
        "/workspace/scripts/run_trial_cleanup.py",
        *arguments(mapped, CONTAINER_OUTPUT, posix=True),
        "--request-sha256",
        pin,
        "--execute",
        "--credentials-stdin",
    ]
    return command


def launch(
    op: Operation, sdk: Path, output: Path, pin: str, profile: str, aws: str, docker: str
) -> dict[str, Any]:
    prepared = prepare_launch(op, sdk, output, pin)
    selected, environment = bridge.local_docker(docker)
    image = (
        bridge.capture(
            [*selected, "image", "inspect", "urban-geography:local", "--format", "{{.Id}}"],
            maximum=128,
            environment=environment,
        )
        .decode("ascii")
        .strip()
    )
    name = "urban-geo-login-" + uuid.uuid4().hex[:16]
    command = docker_command(selected, sdk, image, name, op, output, pin)
    if bridge.capture(
        [*selected, "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"],
        maximum=256,
        seconds=10,
        environment=environment,
    ).strip():
        raise bridge.BridgeError("owned_container_name_already_exists")
    output.mkdir(parents=True)
    write_once(
        output / "launch.json",
        canonical(
            {
                "version": 1,
                "phase": op.phase,
                "request_sha256": pin,
                "container": name,
                "image": image,
                "supervisor_seconds": prepared["recipe"].get(
                    "supervisor_seconds", SUPERVISOR_SECONDS
                ),
            }
        ),
    )
    credentials = None
    attempted = False
    try:
        credentials = bridge.export_login(aws, profile, prepared["identity"]["owner"])
        credentials.require_operation(
            prepared["recipe"].get("source_required_seconds", SOURCE_REQUIRED_SECONDS)
        )
        attempted = True
        content = bridge.capture(
            command,
            content=bridge.envelope(credentials, prepared["identity"]["owner"]),
            maximum=REPLY_BYTES,
            seconds=prepared["recipe"].get("supervisor_seconds", SUPERVISOR_SECONDS),
            environment=environment,
        )
        value = verify_reply(content, pin, op.phase)
        proof = (
            None
            if value["status"] == "operation_failed"
            else load_completion(output / "attempt", op, prepared, value)
        )
        review = proof is None or proof["requires_review"]
        result = {
            "status": value["status"],
            "phase": op.phase,
            "request_sha256": pin,
            "proof_sha256": value["proof_sha256"],
            "requires_review": review,
            "operation_observed": False if proof is None else proof["operation_observed"],
            "data_cleanup_observed": False if proof is None else proof["data_cleanup_observed"],
            "prepared_plan_sha256": None if proof is None else proof["prepared_plan_sha256"],
            "reconcile_before_retry": review,
        }
        if op.phase == INFRA_PHASE:
            result["named_infrastructure_absence_observed"] = (
                False if proof is None else proof["named_infrastructure_absence_observed"]
            )
        return result
    except Exception:
        raise bridge.BridgeError("cleanup_handoff_failed_reconcile_before_retry") from None
    finally:
        if credentials is not None:
            credentials.clear()
        if attempted:
            bridge.cleanup(selected, name, environment)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    add_arguments(parser)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--profile", default="urban-geo")
    parser.add_argument("--aws-cli")
    args = parser.parse_args()
    try:
        op = from_arguments(args)
        prepared = prepare_launch(op, args.sdk_root, args.output, args.request_sha256)
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "plan_only",
                        "phase": op.phase,
                        "request_sha256": prepared["request_sha256"],
                        "region": prepared["scope"]["target"]["region"],
                        "credential_exported": False,
                        "supervisor_seconds": prepared["recipe"].get(
                            "supervisor_seconds", SUPERVISOR_SECONDS
                        ),
                    }
                )
            )
            return
        if args.request_sha256 is None:
            raise ValueError("Execution needs a reviewed phase request SHA256")
        aws, docker = args.aws_cli or shutil.which("aws"), shutil.which("docker")
        if not aws or not docker:
            raise bridge.BridgeError("aws_cli_and_docker_required")
        result = launch(
            op, args.sdk_root, args.output, args.request_sha256, args.profile, aws, docker
        )
    except Exception as error:
        result = {
            "status": "handoff_failed",
            "requires_review": True,
            "reconcile_before_retry": True,
        }
        if (
            isinstance(error, bridge.BridgeError)
            and len(error.args) == 2
            and error.args[0] == "owned_container_cleanup_unconfirmed"
            and re.fullmatch(r"urban-geo-login-[a-f0-9]{16}", error.args[1])
        ):
            result.update(status="owned_container_cleanup_unconfirmed", container=error.args[1])
    print(json.dumps(result))
    if result.get("requires_review") is not False:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
