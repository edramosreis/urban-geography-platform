"""Plan by default; --execute runs one separately approved Athena trial with source/result gates."""

import argparse
import json
import os
from pathlib import Path

from athena_clients import query_resources
from athena_control import Attempt
from athena_preflight import expected_caller
from athena_read_plan import ARTIFACTS
from athena_reader import WORKER_PYTHON, ReaderProcess
from athena_reader_worker import verified_jars
from athena_runner import check_inputs, load_plan, run_trial


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--writer-receipt", type=Path)
    parser.add_argument("--writer-receipt-sha256")
    parser.add_argument("--caller-arn")
    parser.add_argument("--caller-user-id")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    plan = load_plan(args.plan, args.plan_sha256)
    if not args.execute:
        print(
            json.dumps(
                {
                    "mode": "plan_only",
                    "plan": plan,
                    "execution_authorized": False,
                    "reader": "/opt/venv/bin/python in an owned process",
                    "requirements": "Approved caller, pinned input/receipt, ready runtimes",
                }
            )
        )
        return
    if any(
        value is None
        for value in (
            args.input,
            args.writer_receipt,
            args.writer_receipt_sha256,
            args.caller_arn,
            args.caller_user_id,
            args.output,
        )
    ):
        parser.error(
            "--execute requires input, writer receipt/pin, exact caller ARN/UserId and fresh output"
        )
    check_inputs(plan, args.input, args.writer_receipt, args.writer_receipt_sha256)
    expected_caller(plan["target"]["account_id"], args.caller_arn, args.caller_user_id)
    output = args.output.resolve()
    if (
        args.output.is_symlink()
        or output.exists()
        or output == ARTIFACTS.resolve()
        or not output.is_relative_to(ARTIFACTS.resolve())
        or output.is_relative_to(args.input.resolve())
        or output.is_relative_to(args.writer_receipt.parent.resolve())
    ):
        raise ValueError(
            "Use fresh trial output under artifacts, separate from source input/receipt"
        )
    if os.name != "posix" or not Path(WORKER_PYTHON).is_file():
        raise ValueError("Execution requires the existing Linux Docker reader runtime")
    verified_jars()
    attempt = Attempt(output, plan)
    with query_resources(include_results=True) as resources:
        reader = ReaderProcess(output / "reader", resources.reader_environment, plan["target"])
        report = run_trial(
            resources.clients,
            attempt,
            input_path=args.input,
            writer_receipt=args.writer_receipt,
            writer_receipt_sha256=args.writer_receipt_sha256,
            caller_arn=args.caller_arn,
            caller_user_id=args.caller_user_id,
            snapshot_reader=reader,
        )
    print(json.dumps(report))
    if report["status"] != "verified_trial":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
