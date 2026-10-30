"""Deterministic bundle archives and refusal guards; no downloads or Java."""

import hashlib
import zipfile

import pytest

import urban_geography.glue_bundle as bundle


def test_identical_wheels_produce_identical_verified_archive(tmp_path):
    wheel = tmp_path / "example-1.0-py3-none-any.whl"
    wheel.write_bytes(b"synthetic wheel bytes")
    first, second = tmp_path / "first.zip", tmp_path / "second.zip"
    members = bundle.write_archive([wheel], "example==1.0\n", first)
    assert bundle.write_archive([wheel], "example==1.0\n", second) == members
    assert first.read_bytes() == second.read_bytes()
    output = tmp_path / "unpacked"
    bundle.unpack_verified(first, members, output)
    assert (output / "wheels" / wheel.name).read_bytes() == wheel.read_bytes()
    assert (output / "wheels/requirements.txt").read_text() == "example==1.0\n"


def test_duplicate_wheel_names_fail_before_archive_creation(tmp_path):
    paths = []
    for name in ("a", "b"):
        directory = tmp_path / name
        directory.mkdir()
        path = directory / "same.whl"
        path.write_bytes(b"wheel")
        paths.append(path)
    archive = tmp_path / "bad.zip"
    with pytest.raises(ValueError, match="Duplicate wheel"):
        bundle.write_archive(paths, "", archive)
    assert not archive.exists()


@pytest.mark.parametrize("defect", ["changed", "extra", "escape"])
def test_archive_drift_or_unsafe_names_fail_before_extraction(tmp_path, defect):
    name, content = "wheels/example.whl", b"wheel"
    members = {name: hashlib.sha256(content).hexdigest()}
    entries = {name: b"changed"} if defect == "changed" else {name: content}
    if defect == "extra":
        entries["extra.txt"] = b"unexpected"
    elif defect == "escape":
        name = "wheels/../escape.whl"
        entries = {name: content}
        members = {name: hashlib.sha256(content).hexdigest()}
    archive = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive, "w") as target:
        for path, value in entries.items():
            target.writestr(path, value)
    output = tmp_path / "unpacked"
    with pytest.raises(ValueError):
        bundle.unpack_verified(archive, members, output)
    assert not output.exists()
    assert not (tmp_path / "escape.whl").exists()


@pytest.mark.parametrize("kind", ["existing", "data", "source", "cache", "project"])
def test_bad_output_fails_before_export_or_install(tmp_path, monkeypatch, kind):
    cache, project = tmp_path / "cache", tmp_path / "project-package"
    output = {
        "existing": tmp_path / "exists",
        "data": tmp_path / "data/out",
        "source": tmp_path / "src/out",
        "cache": cache / "out",
        "project": project / "out",
    }[kind]
    if kind == "existing":
        output.mkdir()

    def forbidden(*args):
        pytest.fail("Invalid output must not export or install packages")

    monkeypatch.setattr(bundle, "export_dependencies", forbidden)
    with pytest.raises(ValueError):
        bundle.build_bundle(tmp_path, cache, project, output)


def test_publication_retries_only_permission_errors_with_a_fixed_limit(tmp_path, monkeypatch):
    stage, destination = tmp_path / "stage", tmp_path / "published"
    stage.mkdir()
    original = type(stage).rename
    calls = []
    waits = []

    def transient(path, target):
        calls.append(path)
        if len(calls) < 3:
            raise PermissionError("temporary Windows sharing violation")
        return original(path, target)

    monkeypatch.setattr(type(stage), "rename", transient)
    monkeypatch.setattr(bundle.time, "sleep", waits.append)
    bundle.publish_directory(stage, destination)
    assert len(calls) == 3 and waits == [0.5, 1.0]
    assert destination.exists() and not stage.exists()

    other = tmp_path / "other"
    other.mkdir()
    calls.clear()
    waits.clear()

    def persistent(path, target):
        calls.append(path)
        raise PermissionError("persistent error")

    monkeypatch.setattr(type(stage), "rename", persistent)
    with pytest.raises(PermissionError):
        bundle.publish_directory(other, tmp_path / "not-published")
    assert len(calls) == 4 and waits == [0.5, 1.0, 1.5]
    assert other.exists()


def test_publication_refuses_existing_destination_without_rename(tmp_path, monkeypatch):
    stage, destination = tmp_path / "stage", tmp_path / "published"
    stage.mkdir()
    destination.mkdir()
    keep = destination / "keep.txt"
    keep.write_text("keep")

    def forbidden(*args):
        pytest.fail("An existing destination must not be renamed over")

    monkeypatch.setattr(type(stage), "rename", forbidden)
    with pytest.raises(FileExistsError):
        bundle.publish_directory(stage, destination)
    assert keep.read_text() == "keep" and stage.exists()


def test_bundle_requirements_explicitly_select_the_project_with_its_hash():
    actual = bundle.project_requirements("example==1.0 --hash=sha256:abc\n", "0.1.0", "f" * 64)
    assert (
        actual
        == "example==1.0 --hash=sha256:abc\nurban-geography==0.1.0 --hash=sha256:" + "f" * 64 + "\n"
    )
