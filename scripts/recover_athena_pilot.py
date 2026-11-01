"""Inspect by default; --execute replays an uncertain request once and ends its query."""

import argparse
import json
from pathlib import Path

from athena_clients import query_resources
from athena_preflight import expected_caller
from athena_recovery import inspect_submission, recover_submission, recovery_output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--attempt", type=Path, required=True)
    parser.add_argument("--intent-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--caller-arn")
    parser.add_argument("--caller-user-id")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    saved = inspect_submission(args.attempt, args.intent_sha256)
    if not args.execute:
        print(json.dumps(saved.plan()))
        return
    if args.output is None or args.caller_arn is None or args.caller_user_id is None:
        parser.error("--execute requires an exact caller ARN/UserId and fresh recovery output")
    expected_caller(
        saved.intent()["plan"]["target"]["account_id"], args.caller_arn, args.caller_user_id
    )
    recovery_output(args.output, saved)
    if saved.plan()["replay_already_reserved"]:
        raise ValueError("A recovery replay was already reserved; inspect its evidence manually")
    with query_resources() as resources:
        report = recover_submission(
            resources.clients,
            args.attempt,
            args.intent_sha256,
            args.output,
            caller_arn=args.caller_arn,
            caller_user_id=args.caller_user_id,
        )
    print(json.dumps(report))
    if not report["terminal_confirmed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
