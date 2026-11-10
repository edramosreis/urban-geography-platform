"""Plan-only by default; invoke one fixed cleanup phase using private stdin credentials."""

import argparse
import json
import sys

from aws_login_bridge import MAX_BYTES
from trial_cleanup_command import add_arguments, checked, from_arguments, prepare
from trial_cleanup_operation import run_inventory
from trial_cleanup_receipt import reply, save_completion
from trial_data_cleanup import run_data_cleanup
from trial_quiescence import run_quiescence
from trial_quiescence_plan import plan_sha256


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    add_arguments(parser)
    parser.add_argument("--credentials-stdin", action="store_true")
    args = parser.parse_args()
    pin = args.request_sha256
    try:
        op = from_arguments(args)
        prepared = (
            checked(op, args.output, pin)
            if args.execute or pin is not None
            else prepare(op, args.output)
        )
        pin = prepared["request_sha256"]
        if not args.execute:
            if args.credentials_stdin:
                raise ValueError("Plan mode does not accept credentials")
            print(
                json.dumps(
                    {
                        "mode": "plan_only",
                        "phase": op.phase,
                        "request_sha256": pin,
                        "region": prepared["scope"]["target"]["region"],
                        "credential_exported": False,
                    }
                )
            )
            return
        if not args.credentials_stdin or sys.stdin.isatty():
            raise ValueError("Execution requires a private credential pipe")
        content = sys.stdin.buffer.read(MAX_BYTES + 1)
        if not 0 < len(content) <= MAX_BYTES:
            raise ValueError("Invalid private credential size")
        common = (
            op.release,
            op.release_pin,
            op.inventory.path,
            op.inventory.sha256,
            op.principal,
            content,
        )
        if op.phase == "inventory":
            run_inventory(*common, output=args.output, include_services=True)
        elif op.phase == "quiesce":
            run_quiescence(
                *common,
                job=op.job,
                queries=op.queries,
                expected_plan_sha256=plan_sha256(prepared["recipe"]["payload"]),
                output=args.output,
            )
        else:
            run_data_cleanup(
                op.release,
                op.release_pin,
                op.evidence(),
                op.principal,
                content,
                output=args.output,
                coordinator_paused=op.paused,
                approved=op.approved,
                resume=op.resume,
            )
        content = b""
        proof = save_completion(args.output, op, prepared)
        value = reply(pin, op.phase, proof)
    except Exception:
        value = reply(pin, args.phase)
    print(json.dumps(value))


if __name__ == "__main__":
    main()
