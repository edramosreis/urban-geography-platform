"""Supervise one credential-free dbt worker and broker its bounded SDK requests."""

import hashlib
import select
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from athena_control import canonical, write_once
from athena_read_plan import ARTIFACTS, ROOT
from athena_reader import cleanup_group
from dbt_metadata import DbtBroker
from dbt_model_worker import bounded_json, tooling_ready
from dbt_rpc import MAX_FRAME_BYTES, receive, send
from dbt_view_control import ViewController

WORKER_SCRIPT = ROOT / "scripts/dbt_model_worker.py"


class ModelExecutor:
    def __init__(self, clients: dict[str, Any], *, max_seconds: float = 720) -> None:
        if isinstance(max_seconds, bool) or not 1 <= max_seconds <= 720:
            raise ValueError("Use a worker deadline between 1 and 720 seconds")
        self.clients, self.max_seconds = clients, max_seconds

    def __call__(self, control: ViewController, output: Path) -> dict[str, Any]:
        tooling_ready()
        resolved = output.resolve()
        if (
            output.exists()
            or output.is_symlink()
            or resolved == ARTIFACTS.resolve()
            or not resolved.is_relative_to(ARTIFACTS.resolve())
            or resolved.is_relative_to(control.project.resolve())
            or control.project.resolve().is_relative_to(resolved)
        ):
            raise ValueError("Use fresh dbt worker output separate from the project")
        output = resolved
        broker = DbtBroker(self.clients, control)
        output.mkdir(parents=True)
        work = output / "work"
        work.mkdir()
        request = {
            "version": 1,
            "project": str(control.project.resolve()),
            "output": str(resolved),
            "view_plan": control.plan(),
            "controller_intent_sha256": control.intent_sha256,
        }
        content = canonical(request)
        if len(content) > 65536:
            raise ValueError("dbt worker request exceeds its byte ceiling")
        pin = hashlib.sha256(content).hexdigest()
        write_once(output / "worker-request.json", content)
        environment = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "PYTHONPATH": str(ROOT / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
            "PYTHON_DOTENV_DISABLED": "1",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CONFIG_FILE": "/dev/null",
            "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
            "AWS_DEFAULT_REGION": "eu-north-1",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DO_NOT_TRACK": "1",
        }
        fault: str | None = None
        deadline = time.monotonic() + self.max_seconds
        parent, child = socket.socketpair()
        with parent, child:
            command = [
                sys.executable,
                str(WORKER_SCRIPT),
                "--request",
                str(output / "worker-request.json"),
                "--request-sha256",
                pin,
                "--rpc-fd",
                str(child.fileno()),
            ]
            with (
                (output / "stdout.log").open("xb") as stdout,
                (output / "stderr.log").open("xb") as stderr,
            ):
                process = subprocess.Popen(
                    command,
                    cwd=work,
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                    pass_fds=(child.fileno(),),
                )
                child.close()
                try:
                    while process.poll() is None:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("dbt worker deadline reached")
                        ready, _, _ = select.select(
                            [parent], [], [], min(0.25, max(0, deadline - time.monotonic()))
                        )
                        if not ready:
                            continue
                        try:
                            message = receive(parent, min(deadline, time.monotonic() + 5))
                        except EOFError:
                            process.wait(timeout=5)
                            break
                        prefix = output / f"ipc-{broker.calls + 1:02}"
                        write_once(
                            prefix.with_name(prefix.name + "-request.json"), canonical(message)
                        )
                        try:
                            value = broker.dispatch(message)
                        except BaseException as error:
                            fault = type(error).__name__
                            try:
                                rejected: dict[str, Any] = {
                                    "sequence": message.get("sequence"),
                                    "ok": False,
                                    "value": {},
                                }
                                write_once(
                                    prefix.with_name(prefix.name + "-response.json"),
                                    canonical(rejected),
                                )
                                send(parent, rejected, time.monotonic() + 1)
                            except Exception:
                                pass
                            raise
                        response = {"sequence": message["sequence"], "ok": True, "value": value}
                        response_bytes = canonical(response)
                        if len(response_bytes) > MAX_FRAME_BYTES:
                            raise ValueError("dbt IPC result exceeds its byte ceiling")
                        write_once(prefix.with_name(prefix.name + "-response.json"), response_bytes)
                        send(parent, response, deadline)
                except BaseException as error:
                    fault = fault or type(error).__name__
                    raise
                finally:
                    confirmed = cleanup_group(process)
                    write_once(
                        output / "process.json",
                        canonical(
                            {
                                "pid": process.pid,
                                "returncode": process.poll(),
                                "owned_group_stopped": confirmed,
                                "failure_type": fault,
                                "ipc_requests": broker.calls,
                                "metadata_requests": broker.metadata_calls,
                                "credentials_sent": False,
                            }
                        ),
                    )
                if not confirmed or process.returncode != 0 or broker.failed:
                    raise RuntimeError("dbt worker failed or cleanup was not confirmed")
        report = bounded_json(output / "worker-report.json", 65536)
        if (
            report.get("request_sha256") != pin
            or report.get("binding_restored") is not True
            or report.get("credentials_received") is not False
            or report.get("network_attempts") != 0
            or report.get("ipc_requests") != broker.calls
            or time.monotonic() >= deadline
        ):
            raise ValueError("dbt worker result/identity/deadline differs")
        if not isinstance(report.get("outcome"), dict):
            raise ValueError("Missing dbt worker outcome")
        return dict(report["outcome"])
