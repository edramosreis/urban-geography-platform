"""Plan by default; pass one browser snapshot to a fixed local Athena reader container."""

import argparse
import hashlib
import json
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

import aws_login_bridge as bridge
from athena_reader_operation import prepare_operation
from athena_reader_receipt import REPLY_BYTES, load_completion, verify_reply
from aws_role_session import SOURCE_REQUIRED_SECONDS

from urban_geography.glue_runtime import JARS

SUPERVISOR_SECONDS = 1200
BUNDLE_NAME = "iceberg-aws-bundle-1.10.2.jar"
CONTAINER_PLAN = "/workspace/artifacts/reader-plan/plan.json"
CONTAINER_INPUT = "/workspace/artifacts/reader-input"
CONTAINER_WRITER = "/workspace/artifacts/writer-receipt/writer.json"
CONTAINER_OUTPUT = "/workspace/artifacts/reader-operation/attempt"


def prepare_launch(
    plan_path: Path,
    pin: str,
    principal: str,
    sdk_root: Path,
    input_path: Path,
    writer: Path,
    writer_pin: str,
    bundle: Path,
    output: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan, identity = prepare_operation(
        plan_path,
        pin,
        principal,
        input_path=input_path,
        writer_receipt=writer,
        writer_receipt_sha256=writer_pin,
        output=output,
    )
    sdk = bridge.sdk_directory(sdk_root, "analytics")
    if sdk.is_relative_to(output.resolve()) or output.resolve().is_relative_to(sdk):
        raise bridge.BridgeError("output_overlaps_sdk")
    if (
        bundle.is_symlink()
        or not bundle.is_file()
        or bundle.name != BUNDLE_NAME
        or not bundle.resolve().is_relative_to(bridge.ROOT / ".private/tools/glue-runtime")
    ):
        raise bridge.BridgeError("use_existing_verified_reader_bundle")
    with bundle.open("rb") as source:
        actual = hashlib.file_digest(source, "sha256").hexdigest()
    if actual != next(pin for url, pin in JARS if url.endswith("/" + BUNDLE_NAME)):
        raise bridge.BridgeError("reader_bundle_checksum_differs")
    if output.resolve().is_relative_to(bundle.parent.resolve()) or bundle.resolve().is_relative_to(
        output.resolve()
    ):
        raise bridge.BridgeError("output_overlaps_bundle")
    return plan, identity


def docker_command(
    docker: list[str],
    sdk: Path,
    image: str,
    name: str,
    plan: Path,
    pin: str,
    principal: str,
    input_path: Path,
    writer: Path,
    writer_pin: str,
    bundle: Path,
    output: Path,
    *,
    offline: bool = False,
) -> list[str]:
    prepare_launch(plan, pin, principal, sdk, input_path, writer, writer_pin, bundle, output)
    command = bridge.docker_command(docker, "analytics", sdk, image, name, offline=offline)
    mounts = [
        (plan.resolve(), CONTAINER_PLAN, True),
        (input_path.resolve(), CONTAINER_INPUT, True),
        (writer.resolve(), CONTAINER_WRITER, True),
        (bundle.resolve(), "/workspace/.private/tools/glue-runtime/" + BUNDLE_NAME, True),
        (output.resolve(), "/workspace/artifacts/reader-operation", False),
    ]
    extra = ["--tmpfs", "/tmp:rw,nosuid,nodev,size=536870912"]
    for source, target, readonly in mounts:
        if "," in str(source):
            raise bridge.BridgeError("invalid_mount_path")
        extra += [
            "--mount",
            f"type=bind,source={source},target={target}" + (",readonly" if readonly else ""),
        ]
    index = command.index(image)
    command[index:index] = extra
    if offline:
        command.pop()
    if command[-3:] != ["/workspace/scripts/aws_login_probe.py", "--sdk", "analytics"]:
        raise bridge.BridgeError("unexpected_base_container_command")
    command[-3:] = [
        "/workspace/scripts/run_athena_reader.py",
        "--plan",
        CONTAINER_PLAN,
        "--plan-sha256",
        pin,
        "--source-role-arn",
        principal,
        "--input",
        CONTAINER_INPUT,
        "--writer-receipt",
        CONTAINER_WRITER,
        "--writer-receipt-sha256",
        writer_pin,
        "--output",
        CONTAINER_OUTPUT,
        "--execute",
        "--credentials-stdin",
    ]
    return command


def launch(
    plan_path: Path,
    pin: str,
    principal: str,
    sdk: Path,
    input_path: Path,
    writer: Path,
    writer_pin: str,
    bundle: Path,
    output: Path,
    profile: str,
    aws: str,
    docker: str,
) -> dict[str, Any]:
    plan, identity = prepare_launch(
        plan_path, pin, principal, sdk, input_path, writer, writer_pin, bundle, output
    )
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
    command = docker_command(
        selected,
        sdk,
        image,
        name,
        plan_path,
        pin,
        principal,
        input_path,
        writer,
        writer_pin,
        bundle,
        output,
    )
    if bridge.capture(
        [*selected, "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"],
        maximum=256,
        seconds=10,
        environment=environment,
    ).strip():
        raise bridge.BridgeError("owned_container_name_already_exists")
    output.mkdir(parents=True)
    with (output / "launch.json").open("x", encoding="utf-8") as stream:
        json.dump(
            {
                "plan_sha256": pin,
                "writer_receipt_sha256": writer_pin,
                "reader_role_arn": identity["role_arn"],
                "source_role_arn": principal,
                "container": name,
                "image": image,
                "supervisor_seconds": SUPERVISOR_SECONDS,
            },
            stream,
            indent=2,
        )
    credentials = None
    attempted = False
    try:
        credentials = bridge.export_login(aws, profile, identity["owner"])
        credentials.require_operation(SOURCE_REQUIRED_SECONDS)
        attempted = True
        content = bridge.capture(
            command,
            content=bridge.envelope(credentials, identity["owner"]),
            maximum=REPLY_BYTES,
            seconds=SUPERVISOR_SECONDS,
            environment=environment,
        )
        reply = verify_reply(content, pin)
        if reply["status"] != "operation_failed":
            load_completion(output / "attempt", plan, pin, input_path, writer_pin, reply)
        return {
            "status": reply["status"],
            "terminal_confirmed": reply["terminal_confirmed"],
            "result_verified": reply["status"] == "verified_trial",
            "proof_sha256": reply["proof_sha256"],
            "reconcile_before_retry": reply["status"] != "verified_trial",
        }
    except Exception:
        raise bridge.BridgeError("reader_handoff_failed_reconcile_before_retry") from None
    finally:
        if credentials is not None:
            credentials.clear()
        if attempted:
            bridge.cleanup(selected, name, environment)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for flag in ("plan", "sdk-root", "input", "writer-receipt", "bundle", "output"):
        parser.add_argument("--" + flag, type=Path, required=True)
    for flag in ("plan-sha256", "source-role-arn", "writer-receipt-sha256"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--profile", default="urban-geo")
    parser.add_argument("--aws-cli")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    paths = (
        args.plan,
        args.plan_sha256,
        args.source_role_arn,
        args.sdk_root,
        args.input,
        args.writer_receipt,
        args.writer_receipt_sha256,
        args.bundle,
        args.output,
    )
    try:
        plan, _ = prepare_launch(*paths)
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "plan_only",
                        "plan_sha256": args.plan_sha256,
                        "region": plan["target"]["region"],
                        "credential_exported": False,
                        "supervisor_seconds": SUPERVISOR_SECONDS,
                    }
                )
            )
            return
        aws, docker = args.aws_cli or shutil.which("aws"), shutil.which("docker")
        if not aws or not docker:
            raise bridge.BridgeError("aws_cli_and_docker_required")
        result = launch(*paths, args.profile, aws, docker)
    except Exception as error:
        result = {"status": "handoff_failed", "reconcile_before_retry": True}
        if (
            isinstance(error, bridge.BridgeError)
            and len(error.args) == 2
            and error.args[0] == "owned_container_cleanup_unconfirmed"
            and re.fullmatch(r"urban-geo-login-[a-f0-9]{16}", error.args[1])
        ):
            result.update(status="owned_container_cleanup_unconfirmed", container=error.args[1])
    print(json.dumps(result))
    if result.get("result_verified") is not True:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
