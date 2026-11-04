"""Pass one temporary CLI credential snapshot to a fixed Docker identity probe."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REGION = "eu-north-1"
MAX_BYTES = 32 * 1024


class BridgeError(ValueError):
    """Only constant reason codes cross the credential boundary."""


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BridgeError("duplicate_json_key")
        result[key] = value
    return result


def decode(content: bytes, maximum: int = MAX_BYTES) -> dict[str, Any]:
    if not 0 < len(content) <= maximum:
        raise BridgeError("invalid_record_size")
    try:
        value = json.loads(content, object_pairs_hook=_object)
    except (ValueError, UnicodeError, RecursionError):
        raise BridgeError("invalid_json_record") from None
    if not isinstance(value, dict):
        raise BridgeError("invalid_json_record")
    return value


def read_record(path: Path) -> tuple[dict[str, Any], str]:
    """Read one bounded regular JSON evidence record, preserving its original-byte hash."""
    if path.is_symlink() or not path.is_file() or path.parent.is_symlink():
        raise BridgeError("operation_record_not_regular")
    with path.open("rb") as stream:
        content = stream.read(64 * 1024 + 1)
    return decode(content, 64 * 1024), hashlib.sha256(content).hexdigest()


def analytics_runtime(
    sdk_root: Path, bundle: Path, output: Path, jars: Sequence[tuple[str, str]]
) -> Path:
    """Verify the cached analytics SDK/JAR and keep writable output separate."""
    sdk = sdk_directory(sdk_root, "analytics")
    if sdk.is_relative_to(output.resolve()) or output.resolve().is_relative_to(sdk):
        raise BridgeError("output_overlaps_sdk")
    bundle_name = "iceberg-aws-bundle-1.10.2.jar"
    if (
        bundle.is_symlink()
        or not bundle.is_file()
        or bundle.name != bundle_name
        or not bundle.resolve().is_relative_to(ROOT / ".private/tools/glue-runtime")
    ):
        raise BridgeError("use_existing_verified_reader_bundle")
    with bundle.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != next(pin for url, pin in jars if url.endswith("/" + bundle_name)):
        raise BridgeError("reader_bundle_checksum_differs")
    if output.resolve().is_relative_to(bundle.parent.resolve()) or bundle.resolve().is_relative_to(
        output.resolve()
    ):
        raise BridgeError("output_overlaps_bundle")
    return sdk


def account_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{12}", value):
        raise BridgeError("invalid_expected_account")
    return value


@dataclass(repr=False, slots=True)
class TemporaryCredentials:
    access_key: str
    secret_key: str
    token: str
    expiration: datetime
    owner: str | None

    def require_lifetime(self, now: datetime | None = None) -> None:
        remaining = (self.expiration - (now or datetime.now(UTC))).total_seconds()
        if not 60 <= remaining <= 12 * 3600:
            raise BridgeError("temporary_credentials_expired_or_outside_window")

    def require_operation(self, seconds: int, now: datetime | None = None) -> None:
        """Require enough snapshot lifetime without extending it or starting a refresh."""
        if type(seconds) is not int or not 60 <= seconds <= 12 * 3600:
            raise BridgeError("invalid_operation_lifetime")
        checked_at = now or datetime.now(UTC)
        self.require_lifetime(checked_at)
        if (self.expiration - checked_at).total_seconds() < seconds:
            raise BridgeError("credential_lifetime_too_short_for_operation")

    def record(self) -> dict[str, Any]:
        result = {
            "Version": 1,
            "AccessKeyId": self.access_key,
            "SecretAccessKey": self.secret_key,
            "SessionToken": self.token,
            "Expiration": self.expiration.isoformat(),
        }
        if self.owner is not None:
            result["AccountId"] = self.owner
        return result

    def session_options(self) -> dict[str, str]:
        return {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_session_token": self.token,
        }

    def clear(self) -> None:
        self.access_key = self.secret_key = self.token = ""


def temporary(value: dict[str, Any], now: datetime | None = None) -> TemporaryCredentials:
    required = {"Version", "AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"}
    if not required <= value.keys() or not value.keys() <= required | {"AccountId"}:
        raise BridgeError("invalid_temporary_credential_fields")
    if type(value["Version"]) is not int or value["Version"] != 1:
        raise BridgeError("unsupported_credential_version")
    for key, pattern in [
        ("AccessKeyId", r"ASIA[A-Z0-9]{16}"),
        ("SecretAccessKey", r"[A-Za-z0-9/+=]{40}"),
        ("SessionToken", r"[A-Za-z0-9/+=]{1,16384}"),
    ]:
        if not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]):
            raise BridgeError("invalid_temporary_credential_value")
    expiration = value["Expiration"]
    if not isinstance(expiration, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+]00:00)",
        expiration,
    ):
        raise BridgeError("invalid_credential_expiration")
    try:
        expires = datetime.fromisoformat(expiration.replace("Z", "+00:00"))
    except ValueError:
        raise BridgeError("invalid_credential_expiration") from None
    owner = account_id(value["AccountId"]) if "AccountId" in value else None
    result = TemporaryCredentials(
        value["AccessKeyId"], value["SecretAccessKey"], value["SessionToken"], expires, owner
    )
    result.require_lifetime(now)
    return result


def envelope(credentials: TemporaryCredentials, expected_account: str) -> bytes:
    expected_account = account_id(expected_account)
    credentials.require_lifetime()
    if credentials.owner is not None and credentials.owner != expected_account:
        raise BridgeError("credential_account_differs")
    content = json.dumps(
        {"version": 1, "expected_account": expected_account, "credentials": credentials.record()},
        separators=(",", ":"),
    ).encode()
    if len(content) > MAX_BYTES:
        raise BridgeError("invalid_record_size")
    return content


def receive(content: bytes) -> tuple[TemporaryCredentials, str]:
    value = decode(content)
    if (
        set(value) != {"version", "expected_account", "credentials"}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise BridgeError("invalid_handoff_envelope")
    owner = account_id(value["expected_account"])
    if not isinstance(value["credentials"], dict):
        raise BridgeError("invalid_temporary_credential_fields")
    credentials = temporary(value["credentials"])
    if credentials.owner is not None and credentials.owner != owner:
        credentials.clear()
        raise BridgeError("credential_account_differs")
    return credentials, owner


def capture(
    command: list[str],
    *,
    content: bytes | None = None,
    maximum: int = MAX_BYTES,
    seconds: float = 30,
    environment: dict[str, str] | None = None,
) -> bytes:
    """Bound stdout before collecting it; discard stderr rather than echo provider errors."""
    process = None
    timer = None
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE if content is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
        )
        owned_process = process

        def stop() -> None:
            try:
                owned_process.kill()
            except OSError:
                pass

        timer = threading.Timer(seconds, stop)
        timer.start()
        if content is not None:
            assert process.stdin is not None
            process.stdin.write(content)
            process.stdin.close()
        assert process.stdout is not None
        result = process.stdout.read(maximum + 1)
        if len(result) > maximum:
            process.kill()
            raise BridgeError("command_output_exceeds_limit")
        if process.wait(timeout=5) != 0:
            raise BridgeError("owned_command_failed")
        return result
    except Exception:
        raise BridgeError("owned_command_failed_or_exceeded_limit") from None
    finally:
        if timer is not None:
            timer.cancel()
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()


def export_login(aws: str, profile: str, owner: str) -> TemporaryCredentials:
    owner = account_id(owner)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", profile):
        raise BridgeError("invalid_profile_name")
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.upper().startswith("AWS_") and k.upper() != "BOTO_CONFIG"
    }
    environment.update(AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off")
    options = ["--profile", profile, "--region", REGION, "--no-cli-auto-prompt", "--no-cli-pager"]
    session = (
        capture(
            [aws, "configure", "get", "login_session", *options],
            maximum=2048,
            environment=environment,
        )
        .decode("ascii")
        .strip()
    )
    if not re.fullmatch(r"arn:aws:(iam|sts)::" + owner + r":[A-Za-z0-9+=,.@_/-]+", session):
        raise BridgeError("browser_login_profile_required_for_expected_account")
    raw = capture(
        [aws, "configure", "export-credentials", "--format", "process", *options],
        environment=environment,
    )
    result = temporary(decode(raw))
    if result.owner is not None and result.owner != owner:
        result.clear()
        raise BridgeError("credential_account_differs")
    return result


def sdk_directory(path: Path, sdk: str) -> Path:
    resolved = path.resolve()
    if (
        sdk not in {"glue", "analytics"}
        or path.is_symlink()
        or not resolved.is_dir()
        or not resolved.is_relative_to(ROOT / "artifacts")
    ):
        raise BridgeError("use_existing_project_sdk_directory")
    return resolved


def docker_command(
    docker: list[str], sdk: str, sdk_root: Path, image: str, name: str, *, offline: bool = False
) -> list[str]:
    sdk_root = sdk_directory(sdk_root, sdk)
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", image) or not re.fullmatch(
        r"urban-geo-login-[a-f0-9]{16}", name
    ):
        raise BridgeError("invalid_owned_container")
    command = [
        *docker,
        "run",
        "--rm",
        "--interactive",
        "--pull",
        "never",
        "--name",
        name,
        "--network",
        "none" if offline else "bridge",
        "--cpus",
        "2",
        "--memory",
        "4g",
        "--read-only",
        "--workdir",
        "/workspace",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
        "--env",
        "AWS_EC2_METADATA_DISABLED=true",
        "--env",
        "AWS_CONFIG_FILE=/dev/null",
        "--env",
        "AWS_SHARED_CREDENTIALS_FILE=/dev/null",
        "--env",
        "AWS_REGION=" + REGION,
        "--env",
        "AWS_DEFAULT_REGION=" + REGION,
    ]
    for source, target in [
        (ROOT / "scripts", "/workspace/scripts"),
        (ROOT / "src", "/workspace/src"),
        (sdk_root, "/tooling"),
        (ROOT / "analytics/athena/uv.lock", "/workspace/analytics/athena/uv.lock"),
    ]:
        if "," in str(source):
            raise BridgeError("invalid_mount_path")
        command += ["--mount", f"type=bind,source={source},target={target},readonly"]
    command += [
        "--env",
        "PYTHONPATH="
        + ("/tooling:" if sdk == "glue" else "")
        + "/workspace/src:/workspace/scripts",
        image,
        "/opt/venv/bin/python" if sdk == "glue" else "/tooling/bin/python",
        "/workspace/scripts/aws_login_probe.py",
        "--sdk",
        sdk,
    ]
    if offline:
        command.append("--offline")
    return command


def reply(sdk: str, offline: bool) -> dict[str, Any]:
    return {
        "version": 1,
        "sdk": sdk,
        "account_matches": True,
        "assumed_role": True,
        "sts_calls": 1,
        "sdk_pins_verified": True,
        "simulated": offline,
    }


def cleanup(docker: list[str], name: str, environment: dict[str, str]) -> None:
    command = [*docker, "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"]
    try:
        if capture(command, maximum=256, seconds=10, environment=environment).strip():
            capture(
                [*docker, "container", "rm", "--force", name],
                maximum=256,
                seconds=10,
                environment=environment,
            )
            if capture(command, maximum=256, seconds=10, environment=environment).strip():
                raise BridgeError("still_present")
    except Exception:
        raise BridgeError("owned_container_cleanup_unconfirmed", name) from None


def local_docker(docker: str) -> tuple[list[str], dict[str, str]]:
    """Pin the selected local socket context; never hand credentials to a remote daemon."""
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.upper().startswith(("AWS_", "DOCKER_")) and k.upper() != "BOTO_CONFIG"
    }
    context = (
        capture([docker, "context", "show"], maximum=128, environment=environment)
        .decode("ascii")
        .strip()
    )
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", context):
        raise BridgeError("invalid_docker_context")
    endpoint = (
        capture(
            [docker, "context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"],
            maximum=2048,
            environment=environment,
        )
        .decode("ascii")
        .strip()
    )
    if not endpoint.startswith("unix:///") and not re.fullmatch(
        r"npipe:////[.]/pipe/[A-Za-z0-9_.-]+", endpoint
    ):
        raise BridgeError("local_docker_socket_required")
    return [docker, "--context", context], environment


def run_handoff(
    owner: str, profile: str, sdk: str, sdk_root: Path, aws: str, docker: str
) -> dict[str, Any]:
    owner = account_id(owner)
    sdk_root = sdk_directory(sdk_root, sdk)
    selected, environment = local_docker(docker)
    image = (
        capture(
            [*selected, "image", "inspect", "urban-geography:local", "--format", "{{.Id}}"],
            maximum=128,
            environment=environment,
        )
        .decode("ascii")
        .strip()
    )
    name = "urban-geo-login-" + uuid.uuid4().hex[:16]
    command = docker_command(selected, sdk, sdk_root, image, name)
    existing = capture(
        [*selected, "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"],
        maximum=256,
        seconds=10,
        environment=environment,
    )
    if existing.strip():
        raise BridgeError("owned_container_name_already_exists")
    credentials = export_login(aws, profile, owner)
    try:
        received = capture(
            command,
            content=envelope(credentials, owner),
            maximum=2048,
            seconds=45,
            environment=environment,
        )
        if json.dumps(decode(received, 2048), sort_keys=True) != json.dumps(
            reply(sdk, False), sort_keys=True
        ):
            raise BridgeError("unexpected_identity_probe_reply")
        credentials.require_lifetime()
        return {
            "status": "identity_handoff_verified",
            "sdk": sdk,
            "account_matches": True,
            "live_identity_verified": True,
            "credential_expiration": credentials.expiration.isoformat(),
            "pipeline_execution_verified": False,
        }
    finally:
        credentials.clear()
        cleanup(selected, name, environment)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument("--profile", default="urban-geo")
    parser.add_argument("--sdk", choices=("glue", "analytics"), required=True)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--aws-cli")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        account_id(args.expected_account_id)
        sdk_directory(args.sdk_root, args.sdk)
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "plan_only",
                        "sdk": args.sdk,
                        "region": REGION,
                        "operations": ["sts:GetCallerIdentity"],
                        "credential_exported": False,
                    }
                )
            )
            return
        aws, docker = args.aws_cli or shutil.which("aws"), shutil.which("docker")
        if not aws or not docker:
            raise BridgeError("aws_cli_and_docker_required")
        print(
            json.dumps(
                run_handoff(
                    args.expected_account_id, args.profile, args.sdk, args.sdk_root, aws, docker
                )
            )
        )
    except Exception as exc:
        failure = {"status": "failed", "error": "login_handoff_failed"}
        if (
            isinstance(exc, BridgeError)
            and len(exc.args) == 2
            and exc.args[0] == "owned_container_cleanup_unconfirmed"
            and re.fullmatch(r"urban-geo-login-[a-f0-9]{16}", exc.args[1])
        ):
            failure.update(error="owned_container_cleanup_unconfirmed", container=exc.args[1])
        print(json.dumps(failure))
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
