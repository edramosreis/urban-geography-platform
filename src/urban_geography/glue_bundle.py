"""Assemble a verified project wheel and frozen dependency cache without network."""

import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import venv
import zipfile
from pathlib import Path
from typing import Any

from urban_geography.glue_dependencies import (
    digest,
    export_dependencies,
    fresh_output,
    verify_cache,
)
from urban_geography.glue_package import verify_wheel

INSTALLER_OPTIONS = ("--no-index", "--no-deps")
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def write_archive(paths: list[Path], requirements: str, target: Path) -> dict[str, str]:
    """Fixed ZIP metadata makes identical wheel bytes produce an identical bundle."""
    members = {f"wheels/{path.name}": path.read_bytes() for path in paths}
    if len(members) != len(paths):
        raise ValueError("Duplicate wheel filenames cannot enter a bundle")
    members["wheels/requirements.txt"] = requirements.encode("utf-8")
    with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_STORED) as archive:
        for name, content in sorted(members.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o644 << 16
            archive.writestr(info, content)

    return {name: hashlib.sha256(content).hexdigest() for name, content in members.items()}


def unpack_verified(archive_path: Path, members: dict[str, str], output: Path) -> None:
    """Check the whole archive before extracting only the exact known member names."""

    with zipfile.ZipFile(archive_path) as archive:
        if len(archive.namelist()) != len(members) or set(archive.namelist()) != set(members):
            raise ValueError("Bundle archive member inventory differs")
        for name, expected in members.items():
            if not name.startswith("wheels/") or Path(name).name != name[len("wheels/") :]:
                raise ValueError("Unsafe bundle member")
            if hashlib.sha256(archive.read(name)).hexdigest() != expected:
                raise ValueError("Bundle member checksum differs")
        for name in members:
            target = output / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(name))


def publish_directory(stage: Path, destination: Path) -> None:
    """Bound transient Windows bind-mount permission errors; never replace a destination."""
    for attempt in range(6):
        if destination.exists():
            raise FileExistsError("Bundle publication destination already exists")
        try:
            stage.rename(destination)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.5 * (attempt + 1))


def build_bundle(root: Path, cache: Path, project_package: Path, output: Path) -> dict[str, Any]:
    root, cache, project_package = root.resolve(), cache.resolve(), project_package.resolve()
    output = fresh_output(root, output)
    if any(
        output.is_relative_to(path) or path.is_relative_to(output)
        for path in (cache, project_package)
    ):
        raise ValueError("Bundle output must be separate from its inputs")
    plan = export_dependencies(root)
    cached = json.loads((cache / "manifest.json").read_text(encoding="utf-8"))
    if cached["status"] != "complete" or cached["plan"] != plan:
        raise ValueError("Dependency cache does not match current frozen export")
    artifacts = verify_cache(root, cache / "wheels", plan)
    project = json.loads((project_package / "manifest.json").read_text(encoding="utf-8"))
    if project["status"] != "complete":
        raise ValueError("Project package is incomplete")
    filename = project["wheel"]["filename"]
    if Path(filename).name != filename or filename.startswith("."):
        raise ValueError("Unsafe project wheel filename")
    wheel = project_package / "wheels" / filename
    if digest(wheel) != project["wheel"]["sha256"]:
        raise ValueError("Project wheel checksum differs")
    project_info = verify_wheel(wheel, project["source_inputs"])
    if project["source_inputs"]["pyproject_sha256"] != plan["input_sha256"]["pyproject.toml"]:
        raise ValueError("Project dependency declarations differ from the frozen export")
    destination = output
    destination.parent.mkdir(parents=True, exist_ok=True)
    output = destination.parent / (".bundle-" + uuid.uuid4().hex)
    output.mkdir()
    wheels = output / "wheels"
    wheels.mkdir()
    paths = []
    for source, expected in [
        *((cache / "wheels" / item["filename"], item["sha256"]) for item in artifacts),
        (wheel, project_info["sha256"]),
    ]:
        target = wheels / source.name
        shutil.copyfile(source, target)
        if digest(target) != expected:
            raise ValueError("Wheel changed while copying into bundle")
        paths.append(target)
    archive = output / "urban-geography.gluewheels.zip"
    requirements = project_requirements(
        plan["requirements"], project["project"]["version"], project_info["sha256"]
    )
    members = write_archive(paths, requirements, archive)
    inputs = {
        "pins": plan["pins"] | {"urban-geography": project["project"]["version"]},
        "provided": plan["provided"],
        "sql_sha256": project["source_inputs"]["sql_sha256"],
    }
    with tempfile.TemporaryDirectory(prefix="urban-glue-bundle-") as temporary:
        temporary_root = Path(temporary)
        unpack_verified(archive, members, temporary_root)
        site = temporary_root / "site-packages"
        install = subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--offline",
                *INSTALLER_OPTIONS,
                "--python",
                sys.executable,
                "--target",
                str(site),
                *(str(path) for path in sorted((temporary_root / "wheels").glob("*.whl"))),
            ],
            cwd=root,
            text=True,
            capture_output=True,
            timeout=120,
        )
        (output / "install.log").write_text(install.stdout + install.stderr, encoding="utf-8")
        install.check_returncode()
        validation_env = temporary_root / "validation-venv"
        venv.EnvBuilder(with_pip=False, system_site_packages=False, symlinks=True).create(
            validation_env
        )
        provided_root = temporary_root / "provided-runtime"
        provided_root.mkdir()
        for name in plan["provided"]:
            specification = importlib.util.find_spec(name)
            if specification is None or specification.submodule_search_locations is None:
                raise ValueError("Managed Spark/Py4J package is unavailable for validation")
            package_path = Path(next(iter(specification.submodule_search_locations)))
            (provided_root / name).symlink_to(package_path, target_is_directory=True)
        inputs_path = output / "install-inputs.json"
        inputs_path.write_text(json.dumps(inputs, indent=2) + "\n", encoding="utf-8")
        checked = subprocess.run(
            [
                str(validation_env / "bin/python"),
                "-I",
                str(PROJECT_ROOT / "scripts/check_glue_python_install.py"),
                "--site",
                str(site),
                "--project-root",
                str(root),
                "--inputs",
                str(inputs_path),
                "--provided-root",
                str(provided_root),
            ],
            cwd=temporary_root,
            text=True,
            capture_output=True,
            timeout=60,
        )
        (output / "import.log").write_text(checked.stdout + checked.stderr, encoding="utf-8")
        checked.check_returncode()
        installed = json.loads(checked.stdout)
    if (
        verify_cache(root, cache / "wheels", plan) != artifacts
        or digest(wheel) != project_info["sha256"]
    ):
        raise ValueError("Bundle inputs changed during assembly")
    receipt = {
        "status": "complete",
        "contract_version": 1,
        "target": {"glue": "5.1", "python": "3.11", "platforms": plan["platforms"]},
        "bundle": {
            "filename": archive.name,
            "sha256": digest(archive),
            "bytes": archive.stat().st_size,
            "members_sha256": members,
        },
        "project_wheel": project_info,
        "project_manifest_sha256": digest(project_package / "manifest.json"),
        "dependency_wheels": artifacts,
        "input_sha256": plan["input_sha256"],
        "installed_bundle_check": installed,
        "installer_options": list(INSTALLER_OPTIONS),
        "aws_api_or_upload": False,
        "managed_glue_validated": False,
    }
    (output / "manifest.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    publish_directory(output, destination)
    return receipt


def project_requirements(dependencies: str, version: str, wheel_sha256: str) -> str:
    """Select the project itself as well as its frozen runtime dependencies."""
    return dependencies.rstrip() + f"\nurban-geography=={version} --hash=sha256:{wheel_sha256}\n"
