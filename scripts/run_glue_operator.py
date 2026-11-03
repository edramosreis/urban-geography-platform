"""Fixed Docker receiver joining operator-session acquisition to one Glue controller."""

import argparse
import json
import sys
from contextlib import closing
from pathlib import Path
from typing import Any

from aws_login_bridge import MAX_BYTES, TemporaryCredentials, envelope
from glue_control import load_plan, trial_output
from glue_operator_session import create_logs_client, operator_credentials, operator_plan
from glue_writer_receipt import CAPTURE_SECONDS, CaptureError, capture_writer_receipt, load_capture
from run_glue_pilot import run_with_credentials

from urban_geography.ingest import digest

OUTCOMES = {
    "job_succeeded",
    "job_failed",
    "runtime_unverified",
    "watchdog_limit",
    "observation_failed",
    "unexpected_wait_or_stop",
    "controller_interrupted",
    "stop_unconfirmed",
}


WRITER_REQUIRED_SECONDS = CAPTURE_SECONDS + 30


def operation_reply(
    pin: str, result: dict[str, Any] | None = None, writer: dict[str, str] | None = None
) -> dict[str, Any]:
    return {
        "version": 2,
        "release_sha256": pin,
        "status": "operation_failed" if result is None else "controller_completed",
        "outcome": None if result is None else result["outcome"],
        "terminal_confirmed": False if result is None else result["terminal_confirmed"],
        "writer_receipt_captured": writer is not None,
        "writer_receipt_sha256": None if writer is None else writer["writer_receipt_sha256"],
        "capture_receipt_sha256": None if writer is None else writer["capture_receipt_sha256"],
    }


def collect_writer(
    release: Path, pin: str, output: Path, credentials: TemporaryCredentials
) -> dict[str, str] | None:
    """Own one Logs client; retain controller success if capture or cleanup fails."""
    summary = None
    status: dict[str, Any] = {"status": "capture_incomplete"}
    try:
        credentials.require_operation(WRITER_REQUIRED_SECONDS)
        controller = output / "receipt.json"
        controller_pin = digest(controller)
        directory = output.with_name(output.name + "-writer-capture")
        with closing(create_logs_client(credentials)) as client:

            class AdmittedLogs:
                def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
                    try:
                        credentials.require_operation(WRITER_REQUIRED_SECONDS)
                    except ValueError:
                        raise CaptureError("insufficient_capture_credential_lifetime") from None
                    response: dict[str, Any] = client.filter_log_events(**kwargs)
                    return response

            result = capture_writer_receipt(
                AdmittedLogs(), release, pin, controller, controller_pin, directory
            )
        proof_pin = digest(directory / "capture.json")
        load_capture(
            release,
            pin,
            controller,
            controller_pin,
            directory,
            proof_pin,
            result["writer_receipt_sha256"],
        )
        summary = {
            "writer_receipt_sha256": result["writer_receipt_sha256"],
            "capture_receipt_sha256": proof_pin,
        }
        status = {"status": "capture_complete", **summary}
    except Exception:
        pass  # A later log-only capture can use the preserved successful controller receipt.
    try:
        with (output / "writer-capture-status.json").open("x", encoding="utf-8") as destination:
            json.dump(status, destination)
    except OSError:
        return None
    return summary


def run_operation(
    release: Path, pin: str, principal_arn: str, output: Path, content: bytes
) -> dict[str, Any]:
    trial_output(output, release)
    plan = load_plan(release, pin)
    writer = None
    with operator_credentials(release, pin, principal_arn, content) as credentials:
        result = run_with_credentials(plan, release, output, envelope(credentials, plan["owner"]))
        if result["outcome"] == "job_succeeded" and result["terminal_confirmed"] is True:
            writer = collect_writer(release, pin, output, credentials)
    return operation_reply(pin, result, writer)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--credentials-stdin", action="store_true")
    args = parser.parse_args()
    try:
        if not args.execute:
            plan = operator_plan(args.release, args.release_sha256, args.source_role_arn)
            print(
                json.dumps(
                    {
                        "mode": "plan_only",
                        "region": plan["region"],
                        "release_sha256": plan["release_sha256"],
                        "credential_exported": False,
                    }
                )
            )
            return
        if args.output is None or not args.credentials_stdin or sys.stdin.isatty():
            parser.error("Execution requires a fresh output and a private credentials input pipe")
        trial_output(args.output, args.release)
        result = run_operation(
            args.release,
            args.release_sha256,
            args.source_role_arn,
            args.output,
            sys.stdin.buffer.read(MAX_BYTES + 1),
        )
    except Exception:
        result = operation_reply(args.release_sha256)
    # The receiver reports JSON; the host owns the user-facing exit code.
    print(json.dumps(result))


if __name__ == "__main__":
    main()
