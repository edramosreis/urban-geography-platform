"""Fixed Docker receiver joining operator-session acquisition to one Glue controller."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from aws_login_bridge import MAX_BYTES, envelope
from glue_control import load_plan, trial_output
from glue_operator_session import operator_credentials, operator_plan
from run_glue_pilot import run_with_credentials

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


def operation_reply(pin: str, result: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "version": 1,
        "release_sha256": pin,
        "status": "operation_failed" if result is None else "controller_completed",
        "outcome": None if result is None else result["outcome"],
        "terminal_confirmed": False if result is None else result["terminal_confirmed"],
    }


def run_operation(
    release: Path, pin: str, principal_arn: str, output: Path, content: bytes
) -> dict[str, Any]:
    trial_output(output, release)
    plan = load_plan(release, pin)
    with operator_credentials(release, pin, principal_arn, content) as credentials:
        result = run_with_credentials(plan, release, output, envelope(credentials, plan["owner"]))
    return operation_reply(pin, result)


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
