"""Local Glue control-plane checks; this helper is not part of the worker wheel."""

import hashlib
import json
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol

from urban_geography.glue_input import MAX_SUMMARY_BYTES
from urban_geography.glue_release import load_release
from urban_geography.glue_s3_input import S3InputClient, _fetch
from urban_geography.glue_sdk import SDK_PINS


class GlueClient(Protocol):
    def get_job(self, **kwargs: Any) -> dict[str, Any]: ...
    def start_job_run(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_job_run(self, **kwargs: Any) -> dict[str, Any]: ...
    def batch_stop_job_run(self, **kwargs: Any) -> dict[str, Any]: ...


def same(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(
        right, sort_keys=True, allow_nan=False
    )


def load_plan(release: Path, pin: str) -> dict[str, Any]:
    manifest = load_release(release, pin)
    target = manifest["identity"]["target"]
    deployment, owner = target["deployment_name"], target["account_id"]
    scratch = f"scratch/glue/{manifest['release_id']}/"
    arguments = manifest["job"]["arguments"] | {
        "--scriptLocation": manifest["job"]["script_location"],
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-disable",
        "--enable-auto-scaling": "false",
        "--custom-logGroup-prefix": f"/urban-geography/{deployment}/glue",
        "--custom-logStream-prefix": deployment + "-pilot",
        "--TempDir": f"s3://{manifest['bucket']}/{scratch}",
    }
    job = {
        "Name": deployment + "-pilot-writer",
        "Role": f"arn:aws:iam::{owner}:role/service-role/{deployment}-glue-writer",
        "Command": {
            "Name": "glueetl",
            "PythonVersion": "3",
            "ScriptLocation": manifest["job"]["script_location"],
        },
        "GlueVersion": "5.1",
        "JobMode": "SCRIPT",
        "ExecutionClass": "STANDARD",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 10,
        "MaxRetries": 0,
        "JobRunQueuingEnabled": False,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "DefaultArguments": {},
        "NonOverridableArguments": arguments,
    }
    return {
        "release_id": manifest["release_id"],
        "release_sha256": pin,
        "bucket": manifest["bucket"],
        "owner": owner,
        "region": target["region"],
        "completion_key": manifest["completion_key"],
        "job": job,
        "start_request": {
            "JobName": job["Name"],
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "Timeout": 10,
            "ExecutionClass": "STANDARD",
            "JobRunQueuingEnabled": False,
            "Arguments": {},
        },
    }


def preflight(glue: GlueClient, s3: S3InputClient, plan: dict[str, Any]) -> dict[str, Any]:
    """Read only: verify the saved job and pinned S3 completion record before submission."""
    job = glue.get_job(JobName=plan["job"]["Name"])["Job"]
    defaults = {
        "JobMode": "SCRIPT",
        "ExecutionClass": "STANDARD",
        "MaxRetries": 0,
        "JobRunQueuingEnabled": False,
        "DefaultArguments": {},
    }
    for key, expected in plan["job"].items():
        actual = job.get(key, defaults.get(key))
        if key == "Command":
            actual = {name: job.get("Command", {}).get(name) for name in expected}
            if job.get("Command", {}).get("Runtime"):
                raise ValueError("Unexpected Glue command runtime")
        if not same(actual, expected):
            raise ValueError("Glue job differs from the reviewed contract: " + key)
    if job.get("SecurityConfiguration") or job.get("Connections", {}).get("Connections"):
        raise ValueError("Unexpected Glue security configuration or connections")
    content, object_version = _fetch(
        s3, plan["bucket"], plan["owner"], plan["completion_key"], MAX_SUMMARY_BYTES
    )
    if hashlib.sha256(content).hexdigest() != plan["release_sha256"]:
        raise ValueError("S3 release completion record differs from the explicit pin")
    return {
        "job_contract_verified": True,
        "release_manifest_verified": True,
        "release_manifest_version": object_version,
    }


def create_glue_client(*, region: str = "eu-north-1", session: Any = None) -> Any:
    """Lazy optional managed SDK, fixed region/TLS and one total request attempt."""
    if region != "eu-north-1":
        raise ValueError("Glue control is scoped to eu-north-1")
    try:
        for name, pin in SDK_PINS.items():
            if version(name) != pin:
                raise ValueError(f"Use {name}=={pin} for this reviewed control client")
        boto3 = import_module("boto3")
        config_class = import_module("botocore.config").Config
    except (PackageNotFoundError, ModuleNotFoundError) as exc:
        raise RuntimeError("Glue control needs the separately provided pinned SDK") from exc
    config = config_class(
        region_name=region,
        signature_version="v4",
        connect_timeout=5,
        read_timeout=10,
        max_pool_connections=1,
        retries={"mode": "standard", "total_max_attempts": 1},
        ignore_configured_endpoint_urls=True,
        use_fips_endpoint=False,
        use_dualstack_endpoint=False,
    )
    selected = boto3.Session(region_name=region) if session is None else session
    return selected.client("glue", region_name=region, use_ssl=True, verify=True, config=config)


POLL_SECONDS = 10
MAX_OBSERVATIONS = 90
WATCH_SECONDS = 900
MAX_STOP_OBSERVATIONS = 12
STOP_SECONDS = 120
TERMINAL = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"}
ACTIVE = {"STARTING", "RUNNING", "STOPPING", "WAITING"}
ROOT = Path(__file__).resolve().parents[1]


class Journal:
    """One fresh private attempt directory; flush intent and run identity before proceeding."""

    def __init__(self, output: Path, release: Path) -> None:
        output, release = output.resolve(), release.resolve()
        if output.exists() or not output.is_relative_to(ROOT / "artifacts"):
            raise ValueError("Use a fresh attempt directory under the project's ignored artifacts")
        if output.is_relative_to(release) or release.is_relative_to(output):
            raise ValueError("The attempt directory must be separate from the frozen release")
        output.mkdir(parents=True)
        self.output = output
        self.record({"event": "attempt_created"})

    def record(self, event: dict[str, Any]) -> None:
        row = event | {"recorded_at_utc": datetime.now(UTC).isoformat()}
        with (self.output / "events.jsonl").open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def receipt(self, value: dict[str, Any]) -> None:
        with (self.output / "receipt.json").open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(value, indent=2, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())


class SubmissionUnknown(RuntimeError):
    """The caller must reconcile job history rather than issue another automatic start."""


def _observation(glue: GlueClient, plan: dict[str, Any], run_id: str) -> dict[str, Any]:
    name = plan["job"]["Name"]
    run = glue.get_job_run(JobName=name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
    if run.get("Id") != run_id or run.get("JobName") != name:
        raise ValueError("Glue observation identity differs from the submitted run")
    state = run.get("JobRunState")
    if state not in TERMINAL | ACTIVE:
        raise ValueError("Unknown Glue run state")
    expected = {
        key: plan["job"][key]
        for key in ("GlueVersion", "WorkerType", "NumberOfWorkers", "Timeout", "ExecutionClass")
    }
    drift = [
        key
        for key, value in expected.items()
        if run.get(key) is not None and not same(run[key], value)
    ]
    missing = [key for key in expected if run.get(key) is None]
    if run.get("JobRunQueuingEnabled", False) is not False:
        drift.append("JobRunQueuingEnabled")
    metrics = {key: run[key] for key in ("ExecutionTime", "DPUSeconds", "Attempt") if key in run}
    return {"state": state, "drift": drift, "missing_runtime_fields": missing, "metrics": metrics}


def execute_trial(
    glue: GlueClient,
    s3: S3InputClient,
    plan: dict[str, Any],
    record: Callable[[dict[str, Any]], None],
    *,
    now: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """One submission, bounded observations and best-effort confirmed stop of only its run."""
    name = plan["job"]["Name"]
    request = {
        "JobName": name,
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 10,
        "ExecutionClass": "STANDARD",
        "JobRunQueuingEnabled": False,
        "Arguments": {},
    }
    if not same(plan["start_request"], request):
        raise ValueError("Per-run overrides are outside the reviewed request")
    record(
        {"event": "preflight_started", "job_name": name, "release_sha256": plan["release_sha256"]}
    )
    verified = preflight(glue, s3, plan)
    record({"event": "preflight_complete", **verified})
    record({"event": "start_intent", "request": request})
    started = now()
    try:
        response = glue.start_job_run(**request)
        run_id = response.get("JobRunId")
        if (
            not isinstance(run_id, str)
            or not 0 < len(run_id) <= 255
            or any(ord(c) < 32 for c in run_id)
        ):
            raise ValueError("Submission did not return a usable run ID")
    except BaseException as exc:
        try:
            record({"event": "submission_unknown", "error_type": type(exc).__name__})
        except Exception:
            pass
        raise SubmissionUnknown(
            "Submission outcome is unknown; reconcile job history before another attempt"
        ) from exc

    stop_attempted = False
    stop_deadline: float | None = None
    stop_observations = 0
    stop_accepted: bool | None = None
    journal_errors: list[str] = []

    def safe_record(value: dict[str, Any]) -> None:
        try:
            record(value)
        except Exception as exc:
            journal_errors.append(type(exc).__name__)

    def finish(
        observed: dict[str, Any] | None, reason: str | None, stop_accepted: bool | None
    ) -> dict[str, Any]:
        terminal = observed is not None and observed["state"] in TERMINAL
        outcome = "stop_unconfirmed"
        if terminal:
            if reason is not None:
                outcome = reason
            elif observed is not None and (
                observed["drift"]
                or (observed["state"] == "SUCCEEDED" and observed["missing_runtime_fields"])
            ):
                outcome = "runtime_unverified"
            else:
                outcome = (
                    "job_succeeded"
                    if observed is not None and observed["state"] == "SUCCEEDED"
                    else "job_failed"
                )
        return {
            "outcome": outcome,
            "job_name": name,
            "run_id": run_id,
            "release_sha256": plan["release_sha256"],
            "release_id": plan["release_id"],
            "terminal_confirmed": terminal,
            "last_observation": observed,
            "stop_attempted": stop_attempted,
            "stop_accepted": stop_accepted,
            "journal_errors": journal_errors,
            "output_readback_verified": False,
        }

    def stop_and_confirm(reason: str, last: dict[str, Any] | None) -> dict[str, Any]:
        nonlocal stop_attempted, stop_deadline, stop_observations, stop_accepted
        if stop_deadline is None:
            stop_deadline = now() + STOP_SECONDS
        if not stop_attempted:
            safe_record({"event": "stop_intent", "run_id": run_id, "reason": reason})
            stop_attempted = True
            try:
                stopped = glue.batch_stop_job_run(JobName=name, JobRunIds=[run_id])
                stop_accepted = not stopped.get("Errors") and stopped.get(
                    "SuccessfulSubmissions"
                ) == [{"JobName": name, "JobRunId": run_id}]
                safe_record({"event": "stop_response", "run_id": run_id, "accepted": stop_accepted})
            except Exception as exc:
                safe_record(
                    {"event": "stop_error", "run_id": run_id, "error_type": type(exc).__name__}
                )
        while stop_observations < MAX_STOP_OBSERVATIONS and now() < stop_deadline:
            stop_observations += 1
            try:
                last = _observation(glue, plan, run_id)
                safe_record({"event": "stop_observation", "run_id": run_id, **last})
                if last["state"] in TERMINAL:
                    return finish(last, reason, stop_accepted)
            except Exception as exc:
                safe_record(
                    {
                        "event": "stop_observation_error",
                        "run_id": run_id,
                        "error_type": type(exc).__name__,
                    }
                )
            pause(min(POLL_SECONDS, max(0, stop_deadline - now())))
        return finish(last, reason, stop_accepted)

    try:
        record({"event": "run_identified", "job_name": name, "run_id": run_id})
        last = None
        for _ in range(MAX_OBSERVATIONS):
            if now() - started >= WATCH_SECONDS:
                return stop_and_confirm("watchdog_limit", last)
            try:
                last = _observation(glue, plan, run_id)
            except Exception as exc:
                record(
                    {
                        "event": "observation_error",
                        "run_id": run_id,
                        "error_type": type(exc).__name__,
                    }
                )
                return stop_and_confirm("observation_failed", last)
            record({"event": "observation", "run_id": run_id, **last})
            if last["state"] in TERMINAL:
                return finish(last, None, None)
            if last["drift"] or (last["state"] == "RUNNING" and last["missing_runtime_fields"]):
                return stop_and_confirm("runtime_unverified", last)
            if last["state"] in {"WAITING", "STOPPING"}:
                return stop_and_confirm("unexpected_wait_or_stop", last)
            pause(min(POLL_SECONDS, max(0, WATCH_SECONDS - (now() - started))))
        return stop_and_confirm("watchdog_limit", last)
    except BaseException as exc:
        # A journal/disk failure or Ctrl+C after submission must still try to stop this run.
        cleanup = stop_and_confirm("controller_interrupted", None)
        exc.add_note(
            f"Owned Glue run {run_id}; terminal confirmed: {cleanup['terminal_confirmed']}"
        )
        raise
