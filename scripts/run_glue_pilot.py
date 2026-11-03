"""Plan by default; an approved run requires an explicit, sufficiently long-lived snapshot."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from aws_login_bridge import MAX_BYTES, BridgeError, receive
from aws_login_probe import owned_clients, verify_identity
from glue_control import Journal, credential_admission, execute_trial, load_plan, trial_output


def run_with_credentials(
    plan: dict[str, Any], release: Path, output: Path, content: bytes
) -> dict[str, Any]:
    """Admit before SDK work, then recheck immediately before the controller submits."""
    trial_output(output, release)
    credentials, owner = receive(content)
    required = credential_admission()["required_lifetime_seconds"]
    try:
        if owner != plan["owner"]:
            raise BridgeError("credential_account_differs")
        credentials.require_operation(required)
        with owned_clients(credentials, "glue") as clients:
            verify_identity(clients["sts"].get_caller_identity(), owner)
            credentials.require_operation(required)
            journal = Journal(output, release)
            journal.record({"event": "credential_admission", **credential_admission()})

            def before_start() -> None:
                try:
                    credentials.require_operation(required)
                except BridgeError:
                    journal.record({"event": "credential_admission_denied_before_start"})
                    raise
                journal.record({"event": "credential_lifetime_rechecked_before_start"})

            result = execute_trial(
                clients["glue"], clients["s3"], plan, journal.record, before_start=before_start
            )
        journal.receipt(result)
        return result
    finally:
        credentials.clear()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--credentials-stdin", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    plan = load_plan(args.release, args.release_sha256)
    if not args.execute:
        print(
            json.dumps(
                {"mode": "plan_only", "credential_admission": credential_admission(), **plan}
            )
        )
        return
    if args.output is None or not args.credentials_stdin:
        parser.error("--execute requires fresh --output and --credentials-stdin")
    if sys.stdin.isatty():
        parser.error("--credentials-stdin requires a private input pipe")
    trial_output(args.output, args.release)
    result = run_with_credentials(
        plan, args.release, args.output, sys.stdin.buffer.read(MAX_BYTES + 1)
    )
    print(json.dumps(result))
    if result["outcome"] != "job_succeeded":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
