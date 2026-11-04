"""Fixed container receiver; plan by default, execute only with private stdin credentials."""

import argparse
import json
import sys
from pathlib import Path

from athena_reader_operation import prepare_operation, run_operation
from athena_reader_receipt import operation_reply, save_completion
from aws_login_bridge import MAX_BYTES


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--source-role-arn", required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--writer-receipt", type=Path, required=True)
    parser.add_argument("--writer-receipt-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--credentials-stdin", action="store_true")
    args = parser.parse_args()
    options = dict(
        input_path=args.input,
        writer_receipt=args.writer_receipt,
        writer_receipt_sha256=args.writer_receipt_sha256,
        output=args.output,
    )
    try:
        plan, _ = prepare_operation(args.plan, args.plan_sha256, args.source_role_arn, **options)
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "plan_only",
                        "plan_sha256": args.plan_sha256,
                        "region": plan["target"]["region"],
                        "credential_exported": False,
                    }
                )
            )
            return
        if not args.credentials_stdin or sys.stdin.isatty():
            parser.error("Execution requires a private credentials input pipe")
        result = run_operation(
            args.plan,
            args.plan_sha256,
            args.source_role_arn,
            sys.stdin.buffer.read(MAX_BYTES + 1),
            **options,
        )
        proof = save_completion(
            args.output, plan, args.plan_sha256, args.input, args.writer_receipt_sha256, result
        )
        reply = operation_reply(args.plan_sha256, result, proof)
    except Exception:
        reply = operation_reply(args.plan_sha256)
    print(json.dumps(reply))


if __name__ == "__main__":
    main()
