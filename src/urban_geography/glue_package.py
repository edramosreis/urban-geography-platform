"""Build and verify the project's own wheel offline; never bundle managed Spark."""

import hashlib
import json
import subprocess
import sys
import tomllib
import uuid
import zipfile
from configparser import ConfigParser
from email.parser import Parser
from pathlib import Path, PurePosixPath
from typing import Any

METADATA_FILES = {"METADATA", "WHEEL", "RECORD", "entry_points.txt"}
PRIVATE_PARTS = {"private", "secrets", "credentials", "data", "artifacts", "docs", ".aws", ".codex"}


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def package_inputs(root: Path) -> dict[str, Any]:
    config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    if config["build-system"] != {
        "requires": ["hatchling==1.27.0"],
        "build-backend": "hatchling.build",
    }:
        raise ValueError("Package builder requires the reviewed Hatchling pin")
    project = config["project"]
    if project["name"] != "urban-geography":
        raise ValueError("Unexpected project package name")
    package = root / "src/urban_geography"
    payload = {}
    for path in sorted(package.rglob("*")):
        if not path.is_file() or path.suffix not in {".py", ".sql"}:
            continue
        relative = path.relative_to(root / "src").as_posix()
        parts = PurePosixPath(relative).parts
        if any(part.casefold() in PRIVATE_PARTS for part in parts):
            raise ValueError("Private source paths cannot enter the project wheel")
        payload[relative] = _sha256(path.read_bytes())
    required = {
        "urban_geography/__init__.py",
        "urban_geography/sql/grid_proximity.sql",
        "urban_geography/sql/release_proximity.sql",
    }
    if not required.issubset(payload):
        raise ValueError("Package source or required SQL resources are missing")
    return {
        "name": project["name"],
        "version": project["version"],
        "requires_python": project["requires-python"],
        "dependencies": sorted(project["dependencies"]),
        "scripts": project.get("scripts", {}),
        "pyproject_sha256": _sha256((root / "pyproject.toml").read_bytes()),
        "payload_sha256": payload,
        "sql_sha256": {
            name: _sha256((package / "sql" / (name + ".sql")).read_text(encoding="utf-8").encode())
            for name in ("grid_proximity", "release_proximity")
        },
    }


def verify_wheel(wheel: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    """Reject missing, altered or extra payload, including private/dependency files."""
    info_prefix = f"urban_geography-{inputs['version']}.dist-info/"
    expected = inputs["payload_sha256"]
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Wheel contains duplicate ZIP entries")
        actual = set()
        for entry in archive.infolist():
            name = entry.filename
            parts = PurePosixPath(name).parts
            if name.startswith("/") or "\\" in name or ".." in parts:
                raise ValueError("Unsafe wheel entry path")
            if name.startswith(info_prefix) and name[len(info_prefix) :] in METADATA_FILES:
                if entry.file_size > 128 * 1024:
                    raise ValueError("Wheel metadata is unexpectedly large")
                continue
            if name not in expected:
                raise ValueError(f"Unexpected wheel payload: {name}")
            if _sha256(archive.read(name)) != expected[name]:
                raise ValueError(f"Wheel payload differs from source: {name}")
            actual.add(name)
        if actual != set(expected):
            raise ValueError("Wheel is missing source or SQL payload")
        if not {info_prefix + name for name in METADATA_FILES}.issubset(names):
            raise ValueError("Wheel metadata is incomplete")
        metadata = Parser().parsestr(archive.read(info_prefix + "METADATA").decode("utf-8"))
        if (
            metadata["Name"] != inputs["name"]
            or metadata["Version"] != inputs["version"]
            or sorted(part.strip() for part in metadata.get("Requires-Python", "").split(","))
            != sorted(part.strip() for part in inputs["requires_python"].split(","))
            or sorted(metadata.get_all("Requires-Dist", [])) != inputs["dependencies"]
        ):
            raise ValueError("Wheel package metadata differs from pyproject.toml")
        entry_points = ConfigParser()
        entry_points.read_string(archive.read(info_prefix + "entry_points.txt").decode("utf-8"))
        actual_scripts = (
            dict(entry_points["console_scripts"])
            if entry_points.has_section("console_scripts")
            else {}
        )
        if actual_scripts != inputs["scripts"]:
            raise ValueError("Wheel entry points differ from pyproject.toml")
        wheel_metadata = Parser().parsestr(archive.read(info_prefix + "WHEEL").decode("utf-8"))
        if wheel_metadata["Root-Is-Purelib"] != "true" or wheel_metadata.get_all("Tag") != [
            "py3-none-any"
        ]:
            raise ValueError("Expected a pure Python project wheel")
    return {
        "filename": wheel.name,
        "sha256": _sha256(wheel.read_bytes()),
        "bytes": wheel.stat().st_size,
        "payload_files": len(expected),
        "source_and_sql_match": True,
        "bundled_dependencies": [],
    }


SMOKE = """
import hashlib, importlib, importlib.metadata, json, pathlib, sys
site, root, inputs_path = map(pathlib.Path, sys.argv[1:])
sys.path = [p for p in sys.path if p and not pathlib.Path(p).resolve().is_relative_to(root)]
sys.path.insert(0, str(site))
inputs = json.loads(inputs_path.read_text(encoding="utf-8"))
modules = {}
for name in ("urban_geography", "urban_geography.cli", "urban_geography.lakehouse",
             "urban_geography.glue_runtime", "urban_geography.gold"):
    module = importlib.import_module(name)
    path = pathlib.Path(module.__file__).resolve()
    assert path.is_relative_to(site), (name, str(path))
    modules[name] = path.relative_to(site).as_posix()
distribution = importlib.metadata.distribution("urban-geography")
assert pathlib.Path(distribution.locate_file("")).resolve() == site
entry = next(
    ep for ep in distribution.entry_points
    if ep.group == "console_scripts" and ep.name == "urban-geo"
)
assert callable(entry.load())
from urban_geography.gold import load_sql_models, sql_model_hashes
assert sql_model_hashes(load_sql_models()) == inputs["sql_sha256"]
import pyspark
assert importlib.metadata.version("pyspark") == "3.5.6"
assert not pathlib.Path(pyspark.__file__).resolve().is_relative_to(site)
assert pyspark.SparkContext._active_spark_context is None
print(json.dumps({"installed_modules": modules, "sql_resources_match": True,
                  "pyspark_provided_by_environment": True, "spark_started": False}))
"""


def build_package(root: Path, output: Path) -> dict[str, Any]:
    """Publish a fresh verified wheel/manifest after isolated offline installation."""
    root, output = root.resolve(), output.resolve()
    if output.exists():
        raise ValueError("Package output already exists; use a fresh directory")
    if output.is_relative_to(root / "data") or output.is_relative_to(root / "src"):
        raise ValueError("Package output must be outside retained data and package source")
    inputs = package_inputs(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.parent / (".package-" + uuid.uuid4().hex)
    stage.mkdir()
    (stage / "inputs.json").write_text(json.dumps(inputs, indent=2) + "\n", encoding="utf-8")
    tool = subprocess.run(
        ["uv", "--version"], text=True, capture_output=True, timeout=10, check=True
    ).stdout.strip()
    if tool.split()[:2] != ["uv", "0.9.5"]:
        raise ValueError("Use the supported Docker runtime with uv 0.9.5")
    built = subprocess.run(
        ["uv", "build", "--offline", "--wheel", "--out-dir", str(stage / "wheels")],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=120,
    )
    (stage / "build.log").write_text(built.stdout + built.stderr, encoding="utf-8")
    built.check_returncode()
    wheels = [path for path in (stage / "wheels").iterdir() if path.name != ".gitignore"]
    if len(wheels) != 1 or wheels[0].suffix != ".whl":
        raise ValueError("Expected exactly one project wheel")
    wheel = wheels[0]
    inventory = verify_wheel(wheel, inputs)
    site = stage / "site-packages"
    install = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--offline",
            "--no-index",
            "--no-deps",
            "--python",
            sys.executable,
            "--target",
            str(site),
            str(wheel),
        ],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=120,
    )
    (stage / "install.log").write_text(install.stdout + install.stderr, encoding="utf-8")
    install.check_returncode()
    smoke = subprocess.run(
        [sys.executable, "-I", "-c", SMOKE, str(site), str(root), str(stage / "inputs.json")],
        cwd=stage,
        text=True,
        capture_output=True,
        timeout=60,
    )
    (stage / "import.log").write_text(smoke.stdout + smoke.stderr, encoding="utf-8")
    smoke.check_returncode()
    if package_inputs(root) != inputs:
        raise ValueError("Project source changed while packaging")
    receipt = {
        "status": "complete",
        "contract_version": 1,
        "build_tools": {"uv": tool, "backend": "hatchling==1.27.0"},
        "project": {key: inputs[key] for key in ("name", "version", "requires_python")},
        "wheel": inventory,
        "source_inputs": inputs,
        "installed_wheel_check": json.loads(smoke.stdout),
        "dependencies_installed": False,
        "source_stable_during_build": True,
        "aws_api_or_upload": False,
        "scope": "Project wheel only; dependency delivery and cloud writer are separate work.",
    }
    (stage / "manifest.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    stage.rename(output)
    return receipt
