"""Release identity, exact file inventory and local publication without AWS."""

import copy
import hashlib
import json

import pytest

import urban_geography.glue_release as release


def repin(case, manifest):
    content = release.encode(manifest)
    (case.path / "release.json").write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def test_freeze_has_only_required_files_and_derived_job_locations(frozen):
    manifest = frozen.manifest
    assert (
        manifest["job"]["arguments"]["--summary-sha256"]
        == frozen.options["expected_summary_sha256"]
    )
    assert manifest["job"]["arguments"]["--python-modules-installer-option"] == "--no-index"
    assert manifest["job"]["script_location"].endswith("/run_glue_s3_writer.py")
    assert len(manifest["identity"]["files"]) == 10
    assert all(not key.startswith("warehouse/") for key in manifest["keys"].values())
    assert not list(frozen.path.parent.glob(".release-*"))
    # Whitespace in the pinned control file is permitted; contract values are exact.
    content = json.dumps(manifest, indent=2).encode()
    (frozen.path / "release.json").write_bytes(content)
    assert release.load_release(frozen.path, hashlib.sha256(content).hexdigest()) == manifest


@pytest.mark.parametrize("defect", ["checksum", "missing", "extra", "symlink", "root_symlink"])
def test_local_inventory_drift_is_rejected(frozen, defect):
    target = frozen.path / release.SCRIPT
    path = frozen.path
    if defect == "checksum":
        target.write_bytes(b"changed")
    elif defect == "missing":
        target.unlink()
    elif defect == "extra":
        (frozen.path / "private-note.txt").write_bytes(b"private")
    elif defect == "root_symlink":
        path = frozen.path.parent / "linked-release"
        path.symlink_to(frozen.path, target_is_directory=True)
    else:
        target.unlink()
        target.symlink_to(frozen.options["root"] / "scripts/run_glue_s3_writer.py")
    with pytest.raises(ValueError):
        release.load_release(path, frozen.pin)


@pytest.mark.parametrize("defect", ["destination", "job", "release_id", "version_bool", "unknown"])
def test_repinning_does_not_allow_arbitrary_contract_changes(frozen, defect):
    changed = copy.deepcopy(frozen.manifest)
    if defect == "destination":
        changed["keys"][release.SCRIPT] = "warehouse/unapproved.py"
    elif defect == "job":
        changed["job"]["arguments"]["--account-id"] = "999999999999"
    elif defect == "release_id":
        changed["release_id"] = "0" * 64
    elif defect == "version_bool":
        changed["contract_version"] = True
    else:
        changed["unexpected"] = True
    with pytest.raises(ValueError, match="derived contract"):
        release.load_release(frozen.path, repin(frozen, changed))


@pytest.mark.parametrize("defect", ["path", "size", "total", "bool_size", "jar", "pin", "count"])
def test_identity_rejects_unsafe_paths_wrong_pins_and_limits(frozen, defect):
    identity = copy.deepcopy(frozen.manifest["identity"])
    files = identity["files"]
    if defect == "path":
        files["input/../private.txt"] = {"bytes": 1, "sha256": "0" * 64}
    elif defect == "size":
        files[release.SCRIPT]["bytes"] = release.MAX_RELEASE_BYTES + 1
    elif defect == "total":
        files[release.SCRIPT]["bytes"] = release.MAX_RELEASE_BYTES
    elif defect == "bool_size":
        files[release.SCRIPT]["bytes"] = True
    elif defect == "jar":
        files["jars/runtime-0.jar"]["sha256"] = "0" * 64
    elif defect == "pin":
        identity["summary_sha256"] = "0" * 64
    else:
        for index in range(33):
            files[f"input/building_proximity/p{index}.parquet"] = {"bytes": 1, "sha256": "0" * 64}
    with pytest.raises(ValueError):
        release.render_release(identity)


def test_explicit_manifest_pin_is_required(frozen):
    for pin in ("", "A" * 64, "0" * 64):
        with pytest.raises(ValueError):
            release.load_release(frozen.path, pin)


@pytest.mark.parametrize("kind", ["existing", "data", "source", "git", "overlap"])
def test_bad_output_refused_before_bundle_check(frozen, monkeypatch, kind):
    output = {
        "existing": frozen.path,
        "data": frozen.options["root"] / "data/new",
        "source": frozen.options["root"] / "src/new",
        "git": frozen.options["root"] / ".git/new",
        "overlap": frozen.options["bundle"] / "new",
    }[kind]

    def forbidden(*args):
        pytest.fail("Invalid output must not inspect bundle")

    monkeypatch.setattr(release, "verify_bundle", forbidden)
    with pytest.raises(ValueError):
        release.freeze_release(**(frozen.options | {"output": output}))


def test_copy_failure_leaves_no_completed_or_staging_release(frozen, monkeypatch):
    original = release._copy_verified

    def changed(source, target, expected):
        if source.name.endswith(".zip"):
            source.write_bytes(b"changed after inspection")
        original(source, target, expected)

    monkeypatch.setattr(release, "_copy_verified", changed)
    output = frozen.path.parent / "failed"
    with pytest.raises(ValueError):
        release.freeze_release(**(frozen.options | {"output": output}))
    assert not output.exists() and not list(output.parent.glob(".release-*"))


def test_previous_region_manifest_is_rejected_even_with_matching_checksum(frozen):
    manifest = copy.deepcopy(frozen.manifest)
    manifest["identity"]["target"]["region"] = "eu-west-1"
    pin = repin(frozen, manifest)
    with pytest.raises(ValueError, match="scoped to eu-north-1"):
        release.load_release(frozen.path, pin)
