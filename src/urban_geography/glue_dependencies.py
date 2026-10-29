"""Frozen Python wheel cache for the proposed Glue 5.1 / CPython 3.11 target."""

import hashlib
import re
import subprocess
import tomllib
import uuid
import zipfile
from email.parser import Parser
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

PROVIDED = {"pyspark": "3.5.6", "py4j": "0.10.9.7"}
PLATFORMS = ("manylinux_2_34_x86_64", "manylinux_2_28_x86_64", "manylinux2014_x86_64")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fresh_output(root: Path, output: Path) -> Path:
    output = output.resolve()
    if output.exists():
        raise ValueError("Dependency output already exists; use a fresh directory")
    if any(output.is_relative_to(root.resolve() / name) for name in ("data", "src")):
        raise ValueError("Dependency output must be outside retained data and package source")
    return output


def export_dependencies(root: Path) -> dict[str, Any]:
    """Let pinned uv select the runtime closure, without resolving or updating versions."""
    before = {name: digest(root / name) for name in ("pyproject.toml", "uv.lock")}
    tool = subprocess.run(
        ["uv", "--version"], text=True, capture_output=True, check=True, timeout=10
    )
    if tool.stdout.split()[:2] != ["uv", "0.9.5"]:
        raise ValueError("Use the supported Docker runtime with uv 0.9.5")
    command = [
        "uv",
        "export",
        "--locked",
        "--offline",
        "--no-dev",
        "--no-emit-project",
        "--no-emit-package",
        "pyspark",
        "--no-emit-package",
        "py4j",
        "--format",
        "requirements.txt",
        "--no-header",
        "--no-annotate",
    ]
    exported = subprocess.run(
        command, cwd=root, text=True, capture_output=True, timeout=60, check=True
    )
    pins = {}
    for line in exported.stdout.splitlines():
        value = line.strip().removesuffix("\\").rstrip()
        if not value or value.startswith("--hash=sha256:"):
            continue
        match = re.fullmatch(r"([a-z0-9][a-z0-9_.-]*)==([A-Za-z0-9_.+!-]+)", value)
        if match is None or match[1] in pins or match[1] in PROVIDED:
            raise ValueError("Unsupported or non-pinned runtime requirement export")
        pins[match[1]] = match[2]
    if not pins:
        raise ValueError("Dependency export is empty")
    locked = {
        item["name"]: item for item in tomllib.loads((root / "uv.lock").read_text())["package"]
    }
    if any(locked[name]["version"] != value for name, value in PROVIDED.items()):
        raise ValueError("Managed Spark pins differ from the proposed runtime")
    for name, version in pins.items():
        if locked[name]["version"] != version or not locked[name].get("wheels"):
            raise ValueError("Export is not backed by locked binary artifacts")
    if {name: digest(root / name) for name in before} != before:
        raise ValueError("Dependency inputs changed during export")
    return {
        "requirements": exported.stdout,
        "pins": pins,
        "provided": PROVIDED,
        "platforms": list(PLATFORMS),
        "input_sha256": before,
    }


def compatible_wheel(filename: str) -> bool:
    """Narrow policy: CPython 3.11 native wheels or universal Python 3 wheels."""
    if not filename.endswith(".whl"):
        return False
    parts = filename[:-4].rsplit("-", 3)
    if len(parts) != 4:
        return False
    _, python, abi, platform = parts
    if abi == "none" and platform == "any" and "py3" in python.split("."):
        return True
    return python == "cp311" and abi == "cp311" and bool(set(platform.split(".")) & set(PLATFORMS))


def verify_cache(root: Path, wheels: Path, plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Require exactly the exported closure, official lock hashes and target wheel tags."""
    if {name: digest(root / name) for name in plan["input_sha256"]} != plan["input_sha256"]:
        raise ValueError("Dependency lock inputs differ")
    locked = {
        item["name"]: item for item in tomllib.loads((root / "uv.lock").read_text())["package"]
    }
    found = {}
    for path in sorted(wheels.iterdir()):
        if not path.is_file() or not compatible_wheel(path.name):
            raise ValueError("Unexpected file or unsupported target wheel")
        with zipfile.ZipFile(path) as archive:
            entries = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
            if len(entries) != 1:
                raise ValueError("Wheel must have one package metadata record")
            metadata = Parser().parsestr(archive.read(entries[0]).decode("utf-8"))
        name = re.sub(r"[-_.]+", "-", str(metadata["Name"])).lower()
        if name in found or name not in plan["pins"] or metadata["Version"] != plan["pins"][name]:
            raise ValueError("Unexpected, duplicate or incorrect dependency version")
        actual = digest(path)
        matches = [
            item
            for item in locked[name]["wheels"]
            if unquote(urlparse(item["url"]).path).rsplit("/", 1)[-1] == path.name
            and item["hash"] == "sha256:" + actual
        ]
        if len(matches) != 1 or path.stat().st_size != matches[0]["size"]:
            raise ValueError("Dependency wheel differs from its locked artifact")
        found[name] = {
            "name": name,
            "version": metadata["Version"],
            "filename": path.name,
            "sha256": actual,
            "bytes": path.stat().st_size,
        }
    if set(found) != set(plan["pins"]):
        raise ValueError("Dependency cache is missing required wheels")
    return list(found.values())


def fetch_dependencies(root: Path, output: Path) -> dict[str, Any]:
    """Explicit software download step; no cloud APIs, source distributions or version changes."""
    import json

    output = fresh_output(root, output)
    plan = export_dependencies(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.parent / (".dependencies-" + uuid.uuid4().hex)
    stage.mkdir()
    requirements = stage / "requirements.txt"
    requirements.write_text(plan["requirements"], encoding="utf-8")
    command = [
        "/usr/local/bin/python",
        "-m",
        "pip",
        "--isolated",
        "--disable-pip-version-check",
        "download",
        "--index-url",
        "https://pypi.org/simple",
        "--require-hashes",
        "--only-binary=:all:",
        "--no-deps",
        "--retries",
        "0",
        "--timeout",
        "30",
        "--python-version",
        "3.11",
        "--implementation",
        "cp",
        "--abi",
        "cp311",
        "--dest",
        str(stage / "wheels"),
        "-r",
        str(requirements),
    ]
    for platform in PLATFORMS:
        command.extend(["--platform", platform])
    result = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=300)
    (stage / "download.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    result.check_returncode()
    artifacts = verify_cache(root, stage / "wheels", plan)
    receipt = {
        "status": "complete",
        "contract_version": 1,
        "plan": plan,
        "wheels": artifacts,
        "source_distributions": False,
        "aws_api_or_upload": False,
    }
    (stage / "manifest.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    stage.rename(output)
    return receipt
