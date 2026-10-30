"""Conditional delivery, exact readback and resumability with an in-memory S3 fake."""

import base64
import hashlib
import io
import runpy
import sys
from pathlib import Path

import pytest

from urban_geography.glue_delivery import delivery_plan, upload_release
from urban_geography.glue_s3_input import download_prepared_input


class ServiceError(Exception):
    def __init__(self, code="PreconditionFailed", status=412):
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class MemoryDelivery:
    def __init__(self):
        self.objects = {}
        self.requests = []
        self.bodies = []
        self.fail_key = None
        self.corrupt_key = None
        self.change_version = False
        self.version = "version-1"
        self.closed = False

    def put_object(self, **kwargs):
        self.requests.append(("put", kwargs))
        assert kwargs["IfNoneMatch"] == "*" and kwargs["ServerSideEncryption"] == "AES256"
        assert kwargs["ContentLength"] == len(kwargs["Body"])
        assert (
            kwargs["ChecksumSHA256"]
            == base64.b64encode(hashlib.sha256(kwargs["Body"]).digest()).decode()
        )
        key = kwargs["Key"]
        if key == self.fail_key:
            raise ServiceError("AccessDenied", 403)
        if key in self.objects:
            raise ServiceError()
        self.objects[key] = kwargs["Body"]
        return {"VersionId": self.version}

    def get_object(self, **kwargs):
        self.requests.append(("get", kwargs))
        key = kwargs["Key"]
        body = io.BytesIO(b"corrupt" if key == self.corrupt_key else self.objects[key])
        self.bodies.append(body)
        return {
            "Body": body,
            "ContentLength": len(body.getvalue()),
            "VersionId": "changed-version" if self.change_version else self.version,
        }

    def close(self):
        self.closed = True


def test_upload_replay_and_download_preserve_the_prepared_input(frozen):
    client = MemoryDelivery()
    plan = delivery_plan(frozen.path, frozen.pin)
    first = upload_release(client, frozen.path, frozen.pin)
    assert first["created"] == 11 and first["reused"] == 0
    assert [item["key"] for item in first["objects"]] == [item["key"] for item in plan["objects"]]
    assert first["objects"][-2]["key"].endswith("/input-manifest.json")
    assert first["objects"][-1]["key"].endswith("/release.json")
    second = upload_release(client, frozen.path, frozen.pin)
    assert second["created"] == 0 and second["reused"] == 11
    assert len(client.objects) == 11 and len(client.requests) == 44
    target = frozen.manifest["identity"]["target"]
    restored = frozen.path.parent / "restored"
    transfer = download_prepared_input(
        client,
        **target,
        expected_summary_sha256=frozen.options["expected_summary_sha256"],
        expected_manifest_sha256=frozen.manifest["identity"]["input_manifest_sha256"],
        output=restored,
    )
    assert transfer["mapped_buildings"] == 2
    assert all(body.closed for body in client.bodies)
    assert all(request["ExpectedBucketOwner"] == "123456789012" for _, request in client.requests)
    assert not client.closed  # The library caller owns this client.


def test_partial_failure_never_publishes_completion_and_can_resume(frozen):
    client = MemoryDelivery()
    plan = delivery_plan(frozen.path, frozen.pin)
    client.fail_key = plan["objects"][3]["key"]
    with pytest.raises(ServiceError):
        upload_release(client, frozen.path, frozen.pin)
    assert plan["objects"][-1]["key"] not in client.objects
    assert plan["objects"][-2]["key"] not in client.objects
    assert len(client.objects) == 3
    before = dict(client.objects)
    client.fail_key = None
    result = upload_release(client, frozen.path, frozen.pin)
    assert result["reused"] == 3 and result["created"] == 8
    assert all(client.objects[key] == value for key, value in before.items())


@pytest.mark.parametrize(
    "defect", ["existing_mismatch", "corrupt_readback", "version_changed", "unversioned"]
)
def test_bad_remote_bytes_or_versions_stop_before_completion(frozen, defect):
    client = MemoryDelivery()
    plan = delivery_plan(frozen.path, frozen.pin)
    key = plan["objects"][0]["key"]
    if defect == "existing_mismatch":
        client.objects[key] = b"different content"
    elif defect == "corrupt_readback":
        client.corrupt_key = key
    elif defect == "version_changed":
        client.change_version = True
    else:
        client.version = "null"
    with pytest.raises(ValueError):
        upload_release(client, frozen.path, frozen.pin)
    assert plan["objects"][-1]["key"] not in client.objects
    assert all(body.closed for body in client.bodies)
    assert len([request for action, request in client.requests if action == "put"]) == 1


@pytest.mark.parametrize(
    "code,status",
    [
        ("AccessDenied", 403),
        ("ConditionalRequestConflict", 409),
        ("PreconditionFailed", 403),
        ("Other", 412),
    ],
)
def test_only_precise_412_conflicts_are_reused(frozen, code, status):
    client = MemoryDelivery()

    def fail(**kwargs):
        raise ServiceError(code, status)

    client.put_object = fail
    with pytest.raises(ServiceError):
        upload_release(client, frozen.path, frozen.pin)
    assert not client.requests


def test_preflight_stops_before_any_request(frozen):
    client = MemoryDelivery()
    (frozen.path / "private-note.txt").write_text("not part of the frozen release")
    with pytest.raises(ValueError):
        upload_release(client, frozen.path, frozen.pin)
    assert not client.requests


def test_local_change_after_earlier_put_is_detected_before_completion(frozen):
    client = MemoryDelivery()
    plan = delivery_plan(frozen.path, frozen.pin)
    original = client.put_object

    def mutate(**kwargs):
        result = original(**kwargs)
        if kwargs["Key"] == plan["objects"][0]["key"]:
            (frozen.path / plan["objects"][0]["path"]).write_bytes(b"local drift after put")
        return result

    client.put_object = mutate
    with pytest.raises(ValueError):
        upload_release(client, frozen.path, frozen.pin)
    assert plan["objects"][-1]["key"] not in client.objects


@pytest.mark.parametrize("mode", ["plan", "success", "failure", "invalid"])
def test_cli_defaults_to_plan_and_closes_owned_clients(frozen, monkeypatch, capsys, mode):
    script = Path(__file__).resolve().parents[1] / "scripts/upload_glue_release.py"
    main = runpy.run_path(str(script))["main"]
    client = MemoryDelivery()
    factories = []

    def factory():
        factories.append(True)
        return client

    def upload(*args):
        if mode == "failure":
            raise RuntimeError("upload failure")
        return {"status": "complete"}

    monkeypatch.setitem(main.__globals__, "create_input_client", factory)
    monkeypatch.setitem(main.__globals__, "upload_release", upload)
    args = [str(script), "--release", str(frozen.path), "--release-sha256", frozen.pin]
    if mode != "plan":
        args.append("--execute")
    if mode == "invalid":
        (frozen.path / "release.json").write_bytes(b"changed")
    monkeypatch.setattr(sys, "argv", args)
    if mode in {"failure", "invalid"}:
        with pytest.raises((RuntimeError, ValueError)):
            main()
    else:
        main()
        assert "plan_only" in capsys.readouterr().out if mode == "plan" else client.closed
    assert len(factories) == (1 if mode in {"success", "failure"} else 0)
    assert client.closed == (mode in {"success", "failure"})
