"""Plan by default; hand one browser snapshot to a fixed local Glue controller container."""

import argparse
import json
import re
import shutil
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

import aws_login_bridge as bridge
from glue_control import load_plan, same, trial_output
from glue_operator_session import SOURCE_REQUIRED_SECONDS, operator_plan
from glue_writer_receipt import load_capture
from run_glue_operator import OUTCOMES

from urban_geography.ingest import digest

SUPERVISOR_SECONDS = 1800
REPLY_BYTES = 4096
RECEIPT_BYTES = 65536
CONTAINER_RELEASE = PurePosixPath("/workspace/artifacts/frozen-release")
CONTAINER_OUTPUT = PurePosixPath("/workspace/artifacts/glue-operation/attempt")


def prepare_launch(
    release: Path, pin: str, principal_arn: str, sdk_root: Path, output: Path
) -> dict[str, Any]:
    plan = operator_plan(release, pin, principal_arn)
    release = release.resolve()
    sdk_root = bridge.sdk_directory(sdk_root, "glue")
    output = trial_output(output, release)
    if output.is_relative_to(sdk_root) or sdk_root.is_relative_to(output):
        raise bridge.BridgeError("output_overlaps_sdk")
    if release.is_relative_to(sdk_root) or sdk_root.is_relative_to(release):
        raise bridge.BridgeError("release_overlaps_sdk")
    return plan


def docker_command(
    docker: list[str],
    sdk_root: Path,
    image: str,
    name: str,
    release: Path,
    pin: str,
    principal_arn: str,
    output: Path,
    *,
    offline: bool = False,
) -> list[str]:
    prepare_launch(release, pin, principal_arn, sdk_root, output)
    command = bridge.docker_command(docker, "glue", sdk_root, image, name, offline=offline)
    for source, target, readonly in [
        (release.resolve(), str(CONTAINER_RELEASE), True),
        (output.resolve(), "/workspace/artifacts/glue-operation", False),
    ]:
        if "," in str(source):
            raise bridge.BridgeError("invalid_mount_path")
        mount = f"type=bind,source={source},target={target}" + (",readonly" if readonly else "")
        index = command.index(image)
        command[index:index] = ["--mount", mount]
    if offline:
        command.pop()  # The identity probe's --offline flag is not an operator argument.
    if command[-3:] != ["/workspace/scripts/aws_login_probe.py", "--sdk", "glue"]:
        raise bridge.BridgeError("unexpected_base_container_command")
    command[-3:] = [
        "/workspace/scripts/run_glue_operator.py",
        "--release",
        str(CONTAINER_RELEASE),
        "--release-sha256",
        pin,
        "--source-role-arn",
        principal_arn,
        "--output",
        str(CONTAINER_OUTPUT),
        "--execute",
        "--credentials-stdin",
    ]
    return command


def verify_reply(content: bytes, pin: str) -> dict[str, Any]:
    value = bridge.decode(content, REPLY_BYTES)
    if (
        set(value)
        != {
            "version",
            "release_sha256",
            "status",
            "outcome",
            "terminal_confirmed",
            "writer_receipt_captured",
            "writer_receipt_sha256",
            "capture_receipt_sha256",
        }
        or type(value["version"]) is not int
        or value["version"] != 2
        or value["release_sha256"] != pin
        or type(value["terminal_confirmed"]) is not bool
        or type(value["writer_receipt_captured"]) is not bool
    ):
        raise bridge.BridgeError("unexpected_operation_reply")
    if value["status"] == "operation_failed":
        if value["outcome"] is not None or value["terminal_confirmed"]:
            raise bridge.BridgeError("unexpected_operation_reply")
    elif value["status"] == "controller_completed":
        if (
            not isinstance(value["outcome"], str)
            or value["outcome"] not in OUTCOMES
            or value["terminal_confirmed"] != (value["outcome"] != "stop_unconfirmed")
        ):
            raise bridge.BridgeError("unexpected_operation_reply")
    else:
        raise bridge.BridgeError("unexpected_operation_reply")
    if value["writer_receipt_captured"]:
        if value["status"] != "controller_completed" or value["outcome"] != "job_succeeded":
            raise bridge.BridgeError("writer_receipt_without_successful_controller")
        if any(
            not isinstance(value[key], str) or not re.fullmatch(r"[0-9a-f]{64}", value[key])
            for key in ("writer_receipt_sha256", "capture_receipt_sha256")
        ):
            raise bridge.BridgeError("invalid_writer_receipt_pins")
    elif value["writer_receipt_sha256"] is not None or value["capture_receipt_sha256"] is not None:
        raise bridge.BridgeError("writer_pins_without_capture")
    return value


def verify_writer_receipt(output: Path, release: Path, pin: str, reply: dict[str, Any]) -> None:
    controller = output / "attempt/receipt.json"
    load_capture(
        release,
        pin,
        controller,
        digest(controller),
        output / "attempt-writer-capture",
        reply["capture_receipt_sha256"],
        reply["writer_receipt_sha256"],
    )


def verify_receipt(output: Path, plan: dict[str, Any], reply: dict[str, Any]) -> None:
    path = output / "attempt/receipt.json"
    if (output / "attempt").is_symlink() or path.is_symlink():
        raise bridge.BridgeError("controller_receipt_unverified")
    with path.open("rb") as source:
        receipt = bridge.decode(source.read(RECEIPT_BYTES + 1), RECEIPT_BYTES)
    expected = {
        "job_name": plan["job"]["Name"],
        "release_id": plan["release_id"],
        "release_sha256": plan["release_sha256"],
        "outcome": reply["outcome"],
        "terminal_confirmed": reply["terminal_confirmed"],
    }
    if (
        any(not same(receipt.get(key), value) for key, value in expected.items())
        or not isinstance(receipt.get("run_id"), str)
        or not 0 < len(receipt["run_id"]) <= 255
        or any(ord(char) < 32 for char in receipt["run_id"])
    ):
        raise bridge.BridgeError("controller_receipt_unverified")


def launch(
    release: Path,
    pin: str,
    principal_arn: str,
    sdk_root: Path,
    output: Path,
    profile: str,
    aws: str,
    docker: str,
) -> dict[str, Any]:
    session = prepare_launch(release, pin, principal_arn, sdk_root, output)
    plan = load_plan(release, pin)
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
    command = docker_command(selected, sdk_root, image, name, release, pin, principal_arn, output)
    if bridge.capture(
        [*selected, "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"],
        maximum=256,
        seconds=10,
        environment=environment,
    ).strip():
        raise bridge.BridgeError("owned_container_name_already_exists")
    output.mkdir(parents=True)
    with (output / "launch.json").open("x", encoding="utf-8") as destination:
        json.dump(
            {
                "release_sha256": pin,
                "operator_role_arn": session["role_arn"],
                "source_role_arn": session["source_role_arn"],
                "container": name,
                "image": image,
                "supervisor_seconds": SUPERVISOR_SECONDS,
            },
            destination,
            indent=2,
        )
    credentials = None
    attempted = False
    try:
        credentials = bridge.export_login(aws, profile, session["owner"])
        credentials.require_operation(SOURCE_REQUIRED_SECONDS)
        attempted = True
        content = bridge.capture(
            command,
            content=bridge.envelope(credentials, session["owner"]),
            maximum=REPLY_BYTES,
            seconds=SUPERVISOR_SECONDS,
            environment=environment,
        )
        reply = verify_reply(content, pin)
        if reply["status"] == "controller_completed":
            verify_receipt(output, plan, reply)
        if reply["writer_receipt_captured"]:
            verify_writer_receipt(output, release, pin, reply)
        return {
            "status": reply["status"],
            "outcome": reply["outcome"],
            "terminal_confirmed": reply["terminal_confirmed"],
            "controller_receipt_verified": reply["status"] == "controller_completed",
            "writer_receipt_verified": reply["writer_receipt_captured"],
            "writer_receipt_sha256": reply["writer_receipt_sha256"],
            "capture_receipt_sha256": reply["capture_receipt_sha256"],
            "output_readback_verified": False,
        }
    except Exception:
        raise bridge.BridgeError("glue_handoff_failed_reconcile_before_retry") from None
    finally:
        if credentials is not None:
            credentials.clear()
        if attempted:
            bridge.cleanup(selected, name, environment)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", default="urban-geo")
    parser.add_argument("--aws-cli")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        plan = prepare_launch(
            args.release, args.release_sha256, args.source_role_arn, args.sdk_root, args.output
        )
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "plan_only",
                        "region": plan["region"],
                        "release_sha256": plan["release_sha256"],
                        "credential_exported": False,
                        "supervisor_seconds": SUPERVISOR_SECONDS,
                    }
                )
            )
            return
        aws, docker = args.aws_cli or shutil.which("aws"), shutil.which("docker")
        if not aws or not docker:
            raise bridge.BridgeError("aws_cli_and_docker_required")
        result = launch(
            args.release,
            args.release_sha256,
            args.source_role_arn,
            args.sdk_root,
            args.output,
            args.profile,
            aws,
            docker,
        )
    except Exception as exc:
        result = {"status": "handoff_failed", "reconcile_before_retry": True}
        if (
            isinstance(exc, bridge.BridgeError)
            and len(exc.args) == 2
            and exc.args[0] == "owned_container_cleanup_unconfirmed"
            and re.fullmatch(r"urban-geo-login-[a-f0-9]{16}", exc.args[1])
        ):
            result.update(status="owned_container_cleanup_unconfirmed", container=exc.args[1])
    print(json.dumps(result))
    if (
        result.get("outcome") != "job_succeeded"
        or result["status"] != "controller_completed"
        or result.get("writer_receipt_verified") is not True
    ):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
