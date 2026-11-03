"""Exercise Glue control with the real pinned SDK, Stubber, dummy credentials and fake time."""

import argparse
import copy
import io
import json
import os
from pathlib import Path
from typing import Any

from glue_control import (
    Journal,
    SubmissionUnknown,
    create_glue_client,
    execute_trial,
    load_plan,
    same,
)

from urban_geography.glue_input import MAX_SUMMARY_BYTES
from urban_geography.glue_sdk import create_input_client


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def now(self) -> float:
        return self.value

    def pause(self, seconds: float) -> None:
        self.value += seconds


def compare_terraform(plan: dict[str, Any], path: Path) -> None:
    saved = json.loads(path.read_text(encoding="utf-8"))
    names = {
        "Name": "name",
        "Role": "role_arn",
        "GlueVersion": "glue_version",
        "JobMode": "job_mode",
        "ExecutionClass": "execution_class",
        "WorkerType": "worker_type",
        "NumberOfWorkers": "number_of_workers",
        "Timeout": "timeout",
        "MaxRetries": "max_retries",
        "JobRunQueuingEnabled": "job_run_queuing_enabled",
        "DefaultArguments": "default_arguments",
        "NonOverridableArguments": "non_overridable_arguments",
    }
    for key, field in names.items():
        if not same(plan["job"][key], saved[field]):
            raise ValueError("Control plan differs from retained Terraform job: " + key)
    command = saved["command"][0]
    assert plan["job"]["Command"] == {
        "Name": command["name"],
        "PythonVersion": command["python_version"],
        "ScriptLocation": command["script_location"],
    }
    assert saved["execution_property"][0]["max_concurrent_runs"] == 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--terraform-job", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Use fresh SDK-check output")
    plan = load_plan(args.release, args.release_sha256)
    compare_terraform(plan, args.terraform_job)
    import boto3
    from botocore.response import StreamingBody
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="testing", aws_secret_access_key="testing", region_name="eu-north-1"
    )
    previous = os.environ.get("AWS_ENDPOINT_URL_GLUE")
    os.environ["AWS_ENDPOINT_URL_GLUE"] = "https://example.invalid"
    try:
        glue = create_glue_client(session=session)
    finally:
        if previous is None:
            os.environ.pop("AWS_ENDPOINT_URL_GLUE", None)
        else:
            os.environ["AWS_ENDPOINT_URL_GLUE"] = previous
    s3 = create_input_client(session=session)
    results = {}
    bodies = []
    try:
        assert glue.meta.endpoint_url == "https://glue.eu-north-1.amazonaws.com"
        assert glue.meta.config.retries["total_max_attempts"] == 1
        assert glue.meta.config.signature_version == "v4"
        assert glue.meta.config.connect_timeout == 5 and glue.meta.config.read_timeout == 10
        name = plan["job"]["Name"]
        for mode in ("success", "watchdog", "submission_unknown", "stop_unconfirmed"):
            journal = Journal(args.output / mode, args.release)
            clock = Clock()
            run_id = "jr-stubbed-" + mode

            def observed(state: str, owned_id: str = run_id) -> dict[str, Any]:
                fields = {
                    key: plan["job"][key]
                    for key in (
                        "GlueVersion",
                        "WorkerType",
                        "NumberOfWorkers",
                        "Timeout",
                        "ExecutionClass",
                    )
                }
                return {
                    "JobRun": fields
                    | {"Id": owned_id, "JobName": name, "JobRunState": state, "ExecutionTime": 12}
                }

            expected_get = {"JobName": name, "RunId": run_id, "PredecessorsIncluded": False}
            with Stubber(glue) as service, Stubber(s3) as storage:
                service.add_response(
                    "get_job", {"Job": copy.deepcopy(plan["job"])}, {"JobName": name}
                )
                content = (args.release / "release.json").read_bytes()
                raw = io.BytesIO(content)
                bodies.append(raw)
                storage.add_response(
                    "get_object",
                    {
                        "Body": StreamingBody(raw, len(content)),
                        "ContentLength": len(content),
                        "VersionId": "stubbed-control-version",
                    },
                    {
                        "Bucket": plan["bucket"],
                        "Key": plan["completion_key"],
                        "ExpectedBucketOwner": plan["owner"],
                        "Range": f"bytes=0-{MAX_SUMMARY_BYTES}",
                    },
                )
                if mode == "submission_unknown":
                    service.add_client_error(
                        "start_job_run",
                        service_error_code="OperationTimeoutException",
                        http_status_code=400,
                        expected_params=plan["start_request"],
                    )
                else:
                    service.add_response(
                        "start_job_run", {"JobRunId": run_id}, plan["start_request"]
                    )
                    if mode == "success":
                        for state in ("RUNNING", "SUCCEEDED"):
                            service.add_response("get_job_run", observed(state), expected_get)
                    elif mode == "watchdog":
                        for _ in range(90):
                            service.add_response("get_job_run", observed("RUNNING"), expected_get)
                        service.add_response(
                            "batch_stop_job_run",
                            {
                                "SuccessfulSubmissions": [{"JobName": name, "JobRunId": run_id}],
                                "Errors": [],
                            },
                            {"JobName": name, "JobRunIds": [run_id]},
                        )
                        for state in ("STOPPING", "STOPPED"):
                            service.add_response("get_job_run", observed(state), expected_get)
                    else:
                        service.add_client_error(
                            "get_job_run",
                            service_error_code="AccessDeniedException",
                            http_status_code=400,
                            expected_params=expected_get,
                        )
                        service.add_response(
                            "batch_stop_job_run",
                            {
                                "SuccessfulSubmissions": [],
                                "Errors": [
                                    {
                                        "JobName": name,
                                        "JobRunId": run_id,
                                        "ErrorDetail": {
                                            "ErrorCode": "AccessDeniedException",
                                            "ErrorMessage": "stubbed",
                                        },
                                    }
                                ],
                            },
                            {"JobName": name, "JobRunIds": [run_id]},
                        )
                        for _ in range(12):
                            service.add_client_error(
                                "get_job_run",
                                service_error_code="AccessDeniedException",
                                http_status_code=400,
                                expected_params=expected_get,
                            )
                try:
                    result = execute_trial(
                        glue, s3, plan, journal.record, now=clock.now, pause=clock.pause
                    )
                except SubmissionUnknown:
                    assert mode == "submission_unknown"
                    result = {"outcome": "submission_unknown", "terminal_confirmed": False}
                service.assert_no_pending_responses()
                storage.assert_no_pending_responses()
            expected = {
                "success": "job_succeeded",
                "watchdog": "watchdog_limit",
                "submission_unknown": "submission_unknown",
                "stop_unconfirmed": "stop_unconfirmed",
            }[mode]
            assert result["outcome"] == expected
            assert result["terminal_confirmed"] == (mode in {"success", "watchdog"})
            assert raw.closed
            journal.receipt(result)
            results[mode] = result
    finally:
        s3.close()
        glue.close()
    receipt = {
        "status": "complete",
        "transport": "botocore.Stubber",
        "terraform_contract_matches": True,
        "configured_endpoint_override_ignored": True,
        "responses_closed": all(body.closed for body in bodies),
        "cases": results,
        "aws_api_calls_or_paid_runs": False,
    }
    (args.output / "receipt.json").write_text(
        json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "status": "complete",
                "simulated_cases": list(results),
                "aws_api_calls_or_paid_runs": False,
            }
        )
    )


if __name__ == "__main__":
    main()
