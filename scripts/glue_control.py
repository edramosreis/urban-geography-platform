"""Local Glue control-plane checks; this helper is not part of the worker wheel."""

import hashlib
import json
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


def create_glue_client(*, region: str = "eu-west-1", session: Any = None) -> Any:
    """Lazy optional managed SDK, fixed region/TLS and one total request attempt."""
    if region != "eu-west-1":
        raise ValueError("Glue control is scoped to eu-west-1")
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
