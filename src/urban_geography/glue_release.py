"""Freeze verified Glue software and pilot input into one bounded local release."""

import hashlib
import json
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from urban_geography.glue_bundle import (
    INSTALLER_OPTIONS,
    project_requirements,
    publish_directory,
    unpack_verified,
)
from urban_geography.glue_dependencies import digest, export_dependencies, verify_cache
from urban_geography.glue_input import MAX_SUMMARY_BYTES, _copy_verified, load_prepared_input
from urban_geography.glue_package import package_inputs, verify_wheel
from urban_geography.glue_runtime import JARS, runtime_plan
from urban_geography.glue_s3_input import _sha256

MAX_OBJECTS = 32
MAX_RELEASE_BYTES = 512 * 1024 * 1024
SCRIPT = "software/run_glue_s3_writer.py"
BUNDLE = "software/urban-geography.gluewheels.zip"


def encode(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def file_record(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Release requires regular files")
    size = path.stat().st_size
    if not 0 < size <= MAX_RELEASE_BYTES:
        raise ValueError("Release file exceeds the byte ceiling")
    checksum = hashlib.sha256()
    received = 0
    with path.open("rb") as stream:
        while block := stream.read(min(1024 * 1024, size + 1 - received)):
            received += len(block)
            if received > size:
                raise ValueError("Release file grew during inspection")
            checksum.update(block)
    if received != size:
        raise ValueError("Release file size changed during inspection")
    return {"bytes": size, "sha256": checksum.hexdigest()}


def render_release(identity: dict[str, Any], *, contract_version: int = 2) -> dict[str, Any]:
    """Derive all S3 keys and job arguments; arbitrary destinations are not accepted."""
    if type(contract_version) is not int or contract_version not in (1, 2):
        raise ValueError("Release manifest differs from its derived contract: unsupported version")
    if set(identity) != {"target", "summary_sha256", "input_manifest_sha256", "files"}:
        raise ValueError("Unexpected release identity fields")
    target = identity["target"]
    if not isinstance(target, dict) or set(target) != {"deployment_name", "account_id", "region"}:
        raise ValueError("Release target is incomplete")
    if not all(isinstance(value, str) for value in target.values()):
        raise ValueError("Release target values must be strings")
    recipe = runtime_plan(**target)
    if not _sha256(identity["summary_sha256"]) or not _sha256(identity["input_manifest_sha256"]):
        raise ValueError("Release requires explicit SHA256 pins")
    files = identity["files"]
    if not isinstance(files, dict) or not 9 <= len(files) <= MAX_OBJECTS:
        raise ValueError("Release object count is outside its ceiling")
    required = {SCRIPT, BUNDLE, "input/summary.json", "input/input-manifest.json"}
    jars = {"jars/" + name.rsplit("/", 1)[1]: sha for name, sha in JARS}
    required.update(jars)
    if not required.issubset(files):
        raise ValueError("Release is missing required software or input")
    total = 0
    for name, item in files.items():
        if name not in required and not re.fullmatch(
            r"input/(building_proximity|grid_proximity)/[A-Za-z0-9][A-Za-z0-9._-]*\.parquet", name
        ):
            raise ValueError("Release contains an unexpected or unsafe path")
        if not isinstance(item, dict) or set(item) != {"bytes", "sha256"}:
            raise ValueError("Release file descriptor differs")
        if not _sha256(item["sha256"]) or type(item["bytes"]) is not int:
            raise ValueError("Release checksum or size type differs")
        if not 0 < item["bytes"] <= MAX_RELEASE_BYTES:
            raise ValueError("Release file exceeds the byte ceiling")
        if name in jars and item["sha256"] != jars[name]:
            raise ValueError("Release JAR differs from the reviewed runtime pin")
        total += item["bytes"]
    if total > MAX_RELEASE_BYTES:
        raise ValueError("Release total exceeds the byte ceiling")
    for name, pin in [
        ("summary.json", "summary_sha256"),
        ("input-manifest.json", "input_manifest_sha256"),
    ]:
        if files["input/" + name]["sha256"] != identity[pin]:
            raise ValueError("Release input checksum differs from its pin")
    # Preserve historical v1 IDs; bind the changed installer contract into every new v2 ID.
    release_identity = (
        identity
        if contract_version == 1
        else {"contract_version": contract_version, "identity": identity}
    )
    release_id = hashlib.sha256(encode(release_identity)).hexdigest()
    prefix = "runtime/releases/" + release_id + "/"
    keys = {}
    for name in sorted(files):
        if name.startswith("input/"):
            key = "inputs/" + identity["summary_sha256"] + "/" + name[len("input/") :]
        elif name.startswith("jars/"):
            key = "runtime/" + name
        else:
            key = prefix + name[len("software/") :]
        keys[name] = key
    bucket = urlsplit(recipe["warehouse"]).netloc
    return {
        "contract_version": contract_version,
        "status": "frozen_local_release",
        "identity": identity,
        "release_id": release_id,
        "bucket": bucket,
        "keys": keys,
        "completion_key": prefix + "release.json",
        "payload_bytes": total,
        "job": {
            "glue_version": recipe["glue_version"],
            "script_location": f"s3://{bucket}/{keys[SCRIPT]}",
            "arguments": recipe["jar_job_arguments"]
            | {
                "--additional-python-modules": f"s3://{bucket}/{keys[BUNDLE]}",
                "--python-modules-installer-option": (
                    "--no-index" if contract_version == 1 else " ".join(INSTALLER_OPTIONS)
                ),
                "--deployment-name": target["deployment_name"],
                "--account-id": target["account_id"],
                "--region": target["region"],
                "--summary-sha256": identity["summary_sha256"],
                "--manifest-sha256": identity["input_manifest_sha256"],
                "--table-name": "building_proximity",
            },
        },
        "managed_glue_validated": False,
    }


def load_release(path: Path, expected_sha256: str) -> dict[str, Any]:
    """Check the pinned control file, exact local inventory and original pilot contract."""
    if not _sha256(expected_sha256):
        raise ValueError("An explicit release SHA256 pin is required")
    if path.is_symlink():
        raise ValueError("Release symlinks are forbidden")
    control = path / "release.json"
    if control.is_symlink() or not control.is_file():
        raise ValueError("Release requires a regular completion manifest")
    with control.open("rb") as stream:
        content = stream.read(MAX_SUMMARY_BYTES + 1)
    if len(content) > MAX_SUMMARY_BYTES or hashlib.sha256(content).hexdigest() != expected_sha256:
        raise ValueError("Release manifest size or checksum differs")
    manifest = json.loads(content)
    expected = render_release(
        manifest["identity"], contract_version=manifest.get("contract_version")
    )
    if encode(expected) != encode(manifest):
        raise ValueError("Release manifest differs from its derived contract")
    files = manifest["identity"]["files"]
    found = set()
    for item in path.rglob("*"):
        if item.is_symlink():
            raise ValueError("Release symlinks are forbidden")
        if item.is_file():
            found.add(item.relative_to(path).as_posix())
            if len(found) > MAX_OBJECTS + 1:
                raise ValueError("Release has too many files")
    if found != set(files) | {"release.json"}:
        raise ValueError("Release local inventory differs")
    for name, item in files.items():
        if file_record(path / name) != item:
            raise ValueError("Release file checksum or size differs: " + name)
    identity = manifest["identity"]
    plan = load_prepared_input(path / "input", identity["summary_sha256"])
    if {"input/" + name for name in plan.files} | {"input/input-manifest.json"} != {
        name for name in files if name.startswith("input/")
    }:
        raise ValueError("Release input inventory differs from the prepared manifest")
    return manifest


def verify_bundle(root: Path, bundle: Path) -> None:
    """Recheck archived wheels against current source and the frozen dependency lock."""
    receipt = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    if receipt.get("installer_options") != list(INSTALLER_OPTIONS):
        raise ValueError("Bundle installer options differ from the verified offline contract")
    archive = bundle / "urban-geography.gluewheels.zip"
    if receipt["status"] != "complete" or file_record(archive) != {
        key: receipt["bundle"][key] for key in ("bytes", "sha256")
    }:
        raise ValueError("Bundle completion or checksum differs")
    with zipfile.ZipFile(archive) as zipped:
        if (
            len(zipped.infolist()) > 32
            or sum(i.file_size for i in zipped.infolist()) > MAX_RELEASE_BYTES
        ):
            raise ValueError("Unpacked bundle exceeds release ceilings")
    inputs = package_inputs(root)
    plan = export_dependencies(root)
    with tempfile.TemporaryDirectory(prefix="urban-release-check-") as temporary:
        unpacked = Path(temporary)
        unpack_verified(archive, receipt["bundle"]["members_sha256"], unpacked)
        wheels = unpacked / "wheels"
        project = wheels / f"urban_geography-{inputs['version']}-py3-none-any.whl"
        project_info = verify_wheel(project, inputs)
        requirements = (wheels / "requirements.txt").read_text(encoding="utf-8")
        if requirements != project_requirements(
            plan["requirements"], inputs["version"], project_info["sha256"]
        ):
            raise ValueError("Bundle requirements differ from the current frozen selection")
        project.rename(unpacked / project.name)
        (wheels / "requirements.txt").rename(unpacked / "requirements.txt")
        verify_cache(root, wheels, plan)
    checked = receipt["installed_bundle_check"]
    if (
        not all(
            checked.get(name) is True
            for name in (
                "all_package_versions_match",
                "provided_spark_and_py4j_preserved",
                "sql_resources_match",
                "native_library_operations_pass",
                "fresh_venv_without_system_site_packages",
            )
        )
        or checked.get("spark_started") is not False
    ):
        raise ValueError("Bundle lacks its completed offline installation check")


def freeze_release(
    root: Path,
    *,
    bundle: Path,
    prepared_input: Path,
    expected_summary_sha256: str,
    jars: list[Path],
    target: dict[str, str],
    output: Path,
) -> dict[str, Any]:
    """Copy only verified release payloads; no SDK, credentials or AWS operations."""
    root, output = root.resolve(), output.resolve()
    sources = [bundle.resolve(), prepared_input.resolve(), *(jar.resolve() for jar in jars)]
    if output.exists() or any(output.is_relative_to(root / n) for n in ("data", "src", ".git")):
        raise ValueError("Release output must be fresh and outside data/source/Git")
    if any(output.is_relative_to(p) or p.is_relative_to(output) for p in sources):
        raise ValueError("Release output must be disjoint from its inputs")
    runtime_plan(**target)
    before = package_inputs(root)
    verify_bundle(root, bundle)
    plan = load_prepared_input(prepared_input, expected_summary_sha256)
    if len(jars) != len(JARS) or {jar.name for jar in jars} != {
        n.rsplit("/", 1)[1] for n, _ in JARS
    }:
        raise ValueError("Release needs exactly the four reviewed JAR filenames")
    paths = {
        SCRIPT: root / "scripts/run_glue_s3_writer.py",
        BUNDLE: bundle / "urban-geography.gluewheels.zip",
        **{"jars/" + jar.name: jar for jar in jars},
        **{"input/" + name: prepared_input / name for name in plan.files},
        "input/input-manifest.json": prepared_input / "input-manifest.json",
    }
    identity: dict[str, Any] = {
        "target": target,
        "summary_sha256": expected_summary_sha256,
        "input_manifest_sha256": digest(prepared_input / "input-manifest.json"),
        "files": {name: file_record(path) for name, path in paths.items()},
    }
    manifest = render_release(identity)
    content = encode(manifest)
    pin = hashlib.sha256(content).hexdigest()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".release-", dir=output.parent) as temporary:
        stage = Path(temporary)
        for name, source in paths.items():
            destination = stage / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            _copy_verified(source, destination, identity["files"][name])
        (stage / "release.json").write_bytes(content)
        load_release(stage, pin)
        if package_inputs(root) != before:
            raise ValueError("Project source changed while freezing the release")
        publish_directory(stage, output)
    return {
        "release_id": manifest["release_id"],
        "manifest_sha256": pin,
        "objects": len(paths),
        "payload_bytes": manifest["payload_bytes"],
    }
