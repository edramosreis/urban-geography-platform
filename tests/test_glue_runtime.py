"""Configuration and CLI checks for a proposed Glue runtime; no Java or AWS needed."""

import importlib.util
import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from urban_geography.glue_runtime import JARS, runtime_plan

ROOT = Path(__file__).resolve().parents[1]


def test_startup_recipe_agrees_with_foundation_and_local_runtime_pins():
    plan = runtime_plan(deployment_name="urban-geo-test", account_id="123456789012")
    assert plan["warehouse"] == "s3://urban-geo-test-lake-123456789012-eu-west-1/warehouse/"
    assert plan["database"] == "urban_geo_test_m4"
    assert plan["glue_version"] == "5.1"
    spec = importlib.util.spec_from_file_location("fetch_jars", ROOT / "scripts/fetch_jars.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert JARS[:3] == module.JARS
    assert len(JARS) == 4
    assert plan["python_requirement"] == "apache-sedona==1.8.0"

    arguments = plan["jar_job_arguments"]
    assert arguments["--user-jars-first"] == "true"
    assert "--datalake-formats" not in arguments
    assert len(arguments["--extra-jars"].split(",")) == 4
    tokens = shlex.split(arguments["--conf"])
    assert all(tokens[index] == "--conf" for index in range(1, len(tokens), 2))
    reconstructed = dict(token.split("=", 1) for token in tokens[::2])
    assert reconstructed == plan["spark_settings"]
    prefix = "spark.sql.catalog.glue_catalog"
    assert reconstructed[prefix + ".glue.id"] == "123456789012"
    assert reconstructed[prefix + ".client.region"] == "eu-west-1"
    assert reconstructed[prefix + ".s3.sse.type"] == "s3"
    assert reconstructed[prefix + ".table-override.format-version"] == "2"
    assert "spark.master" not in reconstructed
    assert "spark.sql.defaultCatalog" not in reconstructed


@pytest.mark.parametrize(
    "changes",
    [
        {"deployment_name": "Bad_Name"},
        {"deployment_name": "safe --conf spark.master=local[20]"},
        {"deployment_name": "ab"},
        {"account_id": "not-an-account"},
        {"account_id": "123456789012 --conf x=y"},
        {"region": "us-east-1"},
    ],
)
def test_bad_targets_fail_before_configuration_is_rendered(changes):
    values = {"deployment_name": "urban-geo-test", "account_id": "123456789012"}
    with pytest.raises(ValueError):
        runtime_plan(**(values | changes))


def test_cli_needs_no_spark_or_aws_libraries_and_never_overwrites(tmp_path):
    output = tmp_path / "runtime.json"
    command = [
        sys.executable,
        "-c",
        "import sys,runpy; sys.modules['pyspark']=None; sys.modules['boto3']=None; "
        "sys.argv=['prepare_glue_runtime.py',*sys.argv[1:]]; "
        "runpy.run_path('scripts/prepare_glue_runtime.py',run_name='__main__')",
        "--deployment-name",
        "urban-geo-test",
        "--account-id",
        "123456789012",
        "--output",
        str(output),
    ]
    subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    initial = output.read_bytes()
    assert json.loads(initial)["status"] == "prepared_configuration_only"
    failed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    assert failed.returncode != 0 and "already exists" in failed.stderr
    assert output.read_bytes() == initial


@pytest.mark.parametrize("defect", ["invalid_target", "data_output"])
def test_cli_rejections_create_no_output(tmp_path, defect):
    output = tmp_path / "new" / "runtime.json"
    if defect == "data_output":
        output = ROOT / "data" / "unapproved-runtime-plan.json"
    command = [
        sys.executable,
        str(ROOT / "scripts/prepare_glue_runtime.py"),
        "--deployment-name",
        "Bad_Name" if defect == "invalid_target" else "urban-geo-test",
        "--account-id",
        "123456789012",
        "--output",
        str(output),
    ]
    result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode != 0
    assert not output.exists()
