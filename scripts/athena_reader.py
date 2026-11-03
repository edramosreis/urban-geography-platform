"""Launch only the pinned Spark interpreter and stop its owned process group on every exit."""

import hashlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from athena_read_plan import ARTIFACTS, ROOT
from athena_reader_worker import MAX_RECORD_BYTES, validate_request

READER_SECONDS = 180
WORKER_PYTHON = "/opt/venv/bin/python"


def group_alive(group: int) -> bool:
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                if int(fields[2]) == group and fields[0] != "Z":
                    return True
            except (FileNotFoundError, ProcessLookupError):
                pass
    return False


def cleanup_group(process: Any) -> bool:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if not group_alive(process.pid):
            process.wait(timeout=5)
            return True
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 5
        while group_alive(process.pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    return not group_alive(process.pid)


class ReaderProcess:
    def __init__(
        self,
        output: Path,
        credentials: dict[str, str],
        target: dict[str, str],
        *,
        local_fixture: bool = False,
    ) -> None:
        self.output = output
        self._credentials = credentials
        self.target = target
        self.local_fixture = local_fixture

    def __call__(
        self,
        input_path: Path,
        source_run_sha256: str,
        writer_receipt: Path,
        writer_receipt_sha256: str,
        *,
        catalog_name: str,
        namespace: str,
        expected_settings: dict[str, str],
        expected_location: str,
        session_factory: Any = None,
    ) -> dict[str, Any]:
        if os.name != "posix" or not Path("/proc/self/stat").is_file():
            raise ValueError("The isolated reader requires the existing Linux Docker runtime")
        output = self.output.resolve()
        if (
            self.output.exists()
            or self.output.is_symlink()
            or output == ARTIFACTS.resolve()
            or not output.is_relative_to(ARTIFACTS.resolve())
            or output.is_relative_to(input_path.resolve())
            or output.is_relative_to(writer_receipt.parent.resolve())
        ):
            raise ValueError("Use fresh isolated reader output separate from source input/receipt")
        if session_factory is not None:
            raise ValueError("The isolated worker owns its session factory")
        allowed = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
        if self.local_fixture:
            if self._credentials:
                raise ValueError("Local fixture reader must not receive AWS credentials")
        elif (
            not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} <= self._credentials.keys()
            or not self._credentials.keys() <= allowed
            or any(not isinstance(v, str) or not v for v in self._credentials.values())
        ):
            raise ValueError("Cloud reader requires the explicit frozen credential environment")
        request = {
            "contract_version": 1,
            "local_fixture": self.local_fixture,
            "target": self.target,
            "input_path": str(input_path.resolve()),
            "source_run_sha256": source_run_sha256,
            "writer_receipt": str(writer_receipt.resolve()),
            "writer_receipt_sha256": writer_receipt_sha256,
            "catalog_name": catalog_name,
            "namespace": namespace,
            "expected_settings": expected_settings,
            "expected_location": expected_location,
            "output": str(output),
        }
        validate_request(request)
        content = canonical(request)
        if len(content) > MAX_RECORD_BYTES:
            raise ValueError("Reader request exceeds its byte ceiling")
        output.mkdir(parents=True)
        pin = hashlib.sha256(content).hexdigest()
        write_once(output / "request.json", content)
        environment = {
            "PATH": "/opt/venv/bin:/usr/local/bin:/usr/bin:/bin",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
            "PYTHONPATH": str(ROOT / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
            "PYSPARK_PYTHON": WORKER_PYTHON,
            "SPARK_LOCAL_IP": "127.0.0.1",
            "AWS_REGION": "eu-north-1",
            "AWS_DEFAULT_REGION": "eu-north-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CONFIG_FILE": "/dev/null",
            "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        } | self._credentials
        command = [
            WORKER_PYTHON,
            str(ROOT / "scripts/athena_reader_worker.py"),
            "--request",
            str(output / "request.json"),
            "--request-sha256",
            pin,
        ]
        with (
            (output / "stdout.log").open("xb") as stdout,
            (output / "stderr.log").open("xb") as stderr,
        ):
            try:
                process = subprocess.Popen(
                    command,
                    cwd=ROOT,
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                )
            except BaseException:
                environment.clear()
                raise
            confirmed = False
            try:
                returncode = process.wait(timeout=READER_SECONDS)
            finally:
                try:
                    confirmed = cleanup_group(process)
                finally:
                    environment.clear()
                    write_once(
                        output / "process.json",
                        canonical(
                            {
                                "pid": process.pid,
                                "returncode": process.poll(),
                                "owned_group_stopped": confirmed,
                            }
                        ),
                    )
            if not confirmed or returncode != 0:
                raise RuntimeError("Isolated reader failed or process cleanup was not confirmed")
        path = output / "report.json"
        if path.is_symlink():
            raise ValueError("Reader report must be a regular file")
        with path.open("rb") as stream:
            content = stream.read(MAX_RECORD_BYTES + 1)
        if len(content) > MAX_RECORD_BYTES:
            raise ValueError("Reader report exceeds its byte ceiling")
        report = json.loads(content)
        if report.get("request_sha256") != pin:
            raise ValueError("Reader report is for a different request")
        return dict(report["readback"])
