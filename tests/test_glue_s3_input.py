"""Explicit-key S3 transfers against an in-memory client; no SDK, credentials or network."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from s3_input_fixtures import MemoryS3

import urban_geography.glue_s3_input as transfer
from urban_geography.glue_input import load_prepared_input, stage_input
from urban_geography.ingest import digest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def case(history_run_factory, tmp_path):
    run = history_run_factory()
    source = tmp_path / "prepared"
    stage_input(run.path, run.summary_sha256, source)
    client = MemoryS3(source, run.summary_sha256)
    options = {
        "deployment_name": "urban-geo-test",
        "account_id": "123456789012",
        "expected_summary_sha256": run.summary_sha256,
        "expected_manifest_sha256": digest(source / "input-manifest.json"),
        "output": tmp_path / "downloaded",
    }
    return SimpleNamespace(source=source, client=client, options=options)


def execute(case, **changes):
    return transfer.download_prepared_input(case.client, **(case.options | changes))


def test_exact_transfer_is_readable_and_closes_short_streams(case):
    case.client.objects["unrelated/ignored.parquet"] = b"never read"
    result = execute(case)
    assert result["status"] == "complete" and result["get_requests"] == 4
    assert (
        result["payload_bytes"]
        == load_prepared_input(
            case.options["output"], case.options["expected_summary_sha256"]
        ).payload_bytes
    )
    assert result["downloaded_body_bytes"] == sum(
        path.stat().st_size for path in case.source.rglob("*") if path.is_file()
    )
    assert set(result["object_versions"]) == {
        path.relative_to(case.source).as_posix()
        for path in case.source.rglob("*")
        if path.is_file()
    }
    assert all(body.closed for body in case.client.bodies)
    for request in case.client.requests:
        assert request["Bucket"] == "urban-geo-test-lake-123456789012-eu-west-1"
        assert request["ExpectedBucketOwner"] == "123456789012"
        assert request["Key"].startswith(case.client.prefix)
        assert request["Range"].startswith("bytes=0-")
    for path in case.source.rglob("*"):
        if path.is_file():
            assert digest(case.options["output"] / path.relative_to(case.source)) == digest(path)


@pytest.mark.parametrize("field", ["expected_summary_sha256", "expected_manifest_sha256"])
def test_invalid_pins_make_no_requests(case, field):
    with pytest.raises(ValueError, match="SHA256"):
        execute(case, **{field: "not-a-pin"})
    assert not case.client.requests and not case.options["output"].exists()


def test_wrong_manifest_pin_reads_only_the_bounded_control_object(case):
    with pytest.raises(ValueError, match="manifest checksum"):
        execute(case, expected_manifest_sha256="0" * 64)
    assert len(case.client.requests) == 1 and all(body.closed for body in case.client.bodies)
    assert not case.options["output"].exists()


@pytest.mark.parametrize(
    "defect",
    [
        "identity",
        "path",
        "count",
        "size",
        "total",
        "ceiling",
        "rows",
        "summary_pin",
        "status",
        "version_type",
    ],
)
def test_bad_manifest_stops_before_payload_requests(case, defect):
    key = case.client.prefix + "input-manifest.json"
    manifest = json.loads(case.client.objects[key])
    if defect == "identity":
        manifest["source_run_sha256"] = "0" * 64
    elif defect == "path":
        manifest["files"]["../escape.parquet"] = {"bytes": 1, "sha256": "0" * 64}
    elif defect == "count":
        manifest["files"] = {
            f"building_proximity/p{i}.parquet": {"bytes": 1, "sha256": "0" * 64} for i in range(14)
        }
    elif defect == "size":
        manifest["files"]["summary.json"]["bytes"] = transfer.MAX_SUMMARY_BYTES + 1
    elif defect == "total":
        manifest["payload_bytes"] += 1
    elif defect == "ceiling":
        manifest["limits"]["payload_bytes_including_summary"] = -1
    elif defect == "rows":
        manifest["mapped_buildings"] = transfer.MAX_ROWS + 1
    elif defect == "summary_pin":
        manifest["files"]["summary.json"]["sha256"] = "0" * 64
    elif defect == "status":
        manifest["status"] = "failed"
    elif defect == "version_type":
        manifest["contract_version"] = True
    content = json.dumps(manifest).encode()
    case.client.objects[key] = content
    with pytest.raises(ValueError):
        execute(case, expected_manifest_sha256=hashlib.sha256(content).hexdigest())
    assert len(case.client.requests) == 1
    assert not case.options["output"].exists()
    assert not list(case.options["output"].parent.glob(".download-*"))


@pytest.mark.parametrize("defect", ["hash", "grow", "shrink", "missing"])
def test_payload_errors_never_publish_a_completed_folder(case, defect):
    key = next(key for key in case.client.objects if key.endswith(".parquet"))
    content = case.client.objects[key]
    if defect == "hash":
        case.client.objects[key] = bytes([content[0] ^ 1]) + content[1:]
    elif defect == "grow":
        case.client.objects[key] += b"x"
    elif defect == "shrink":
        case.client.objects[key] = content[:-1]
    else:
        del case.client.objects[key]
    with pytest.raises((ValueError, FileNotFoundError)):
        execute(case)
    assert not case.options["output"].exists()
    assert all(body.closed for body in case.client.bodies)


@pytest.mark.parametrize("defect", ["version", "length", "interrupted"])
def test_response_errors_close_the_body_and_fail(case, defect):
    def change(response):
        if defect == "version":
            response["VersionId"] = "null"
        elif defect == "length":
            response["ContentLength"] -= 1
        else:
            response["Body"].fail_reads = True

    case.client.response_change = change
    with pytest.raises((ValueError, OSError)):
        execute(case)
    assert all(body.closed for body in case.client.bodies)
    assert not case.options["output"].exists()


@pytest.mark.parametrize("kind", ["existing", "data", "source", "git"])
def test_invalid_destinations_make_no_requests_or_overwrites(case, kind, tmp_path):
    output = {
        "existing": case.options["output"],
        "data": ROOT / "data/unapproved-s3-input",
        "source": ROOT / "src/unapproved-s3-input",
        "git": ROOT / ".git/unapproved-s3-input",
    }[kind]
    if kind == "existing":
        output.mkdir()
        (output / "keep.txt").write_text("keep")
    with pytest.raises(ValueError):
        execute(case, output=output)
    assert not case.client.requests
    if kind == "existing":
        assert (output / "keep.txt").read_text() == "keep"
    else:
        assert not output.exists()


@pytest.mark.parametrize(
    "changes",
    [{"deployment_name": "Bad_Name"}, {"account_id": "not-an-account"}, {"region": "us-east-1"}],
)
def test_invalid_cloud_target_makes_no_requests(case, changes):
    with pytest.raises(ValueError):
        execute(case, **changes)
    assert not case.client.requests and not case.options["output"].exists()


def test_oversized_manifest_response_is_bounded_and_closed(case):
    key = case.client.prefix + "input-manifest.json"
    content = b" " * (transfer.MAX_SUMMARY_BYTES + 100)
    case.client.objects[key] = content
    with pytest.raises(ValueError, match="bounded object length"):
        execute(case, expected_manifest_sha256=hashlib.sha256(content).hexdigest())
    assert case.client.requests[0]["Range"] == f"bytes=0-{transfer.MAX_SUMMARY_BYTES}"
    assert case.client.bodies[0].closed and not case.options["output"].exists()


def test_final_prepared_reader_refuses_inconsistent_control_fields(case):
    key = case.client.prefix + "input-manifest.json"
    manifest = json.loads(case.client.objects[key])
    manifest["limits"]["grid_rows"] = 999
    content = json.dumps(manifest).encode()
    case.client.objects[key] = content
    with pytest.raises(ValueError, match="manifest differs"):
        execute(case, expected_manifest_sha256=hashlib.sha256(content).hexdigest())
    assert len(case.client.requests) == 4 and not case.options["output"].exists()
    assert all(body.closed for body in case.client.bodies)
