"""Wheel contents, provenance and offline installed-artifact acceptance."""

import json
import zipfile
from pathlib import Path

import pytest

import urban_geography.glue_package as package

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def tiny_project(tmp_path):
    root = tmp_path / "project"
    source = root / "src/urban_geography"
    (source / "sql").mkdir(parents=True)
    (source / "__init__.py").write_text('"""Synthetic package."""\n', encoding="utf-8")
    for name in ("grid_proximity", "release_proximity"):
        (source / "sql" / (name + ".sql")).write_text("SELECT 1\n", encoding="utf-8")
    (root / "pyproject.toml").write_text(
        '[build-system]\nrequires=["hatchling==1.27.0"]\nbuild-backend="hatchling.build"\n'
        '[project]\nname="urban-geography"\nversion="0.1.0"\n'
        'requires-python=">=3.11,<3.12"\ndependencies=["pyspark==3.5.6"]\n'
        '[project.scripts]\nurban-geo="urban_geography.cli:main"\n',
        encoding="utf-8",
    )
    return root


def wheel_entries(root):
    inputs = package.package_inputs(root)
    entries = {name: (root / "src" / name).read_bytes() for name in inputs["payload_sha256"]}
    prefix = "urban_geography-0.1.0.dist-info/"
    entries[prefix + "METADATA"] = (
        b"Metadata-Version: 2.4\nName: urban-geography\nVersion: 0.1.0\n"
        b"Requires-Python: <3.12,>=3.11\nRequires-Dist: pyspark==3.5.6\n\n"
    )
    entries[prefix + "WHEEL"] = b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    entries[prefix + "entry_points.txt"] = (
        b"[console_scripts]\nurban-geo = urban_geography.cli:main\n"
    )
    entries[prefix + "RECORD"] = b""
    return inputs, entries


def write_wheel(path, entries):
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return path


def test_payload_and_equivalent_python_constraint_order(tiny_project, tmp_path):
    inputs, entries = wheel_entries(tiny_project)
    wheel = write_wheel(tmp_path / "app.whl", entries)
    actual = package.verify_wheel(wheel, inputs)
    assert actual["source_and_sql_match"] is True
    assert actual["payload_files"] == 3
    assert actual["bundled_dependencies"] == []
    assert len(actual["sha256"]) == 64


@pytest.mark.parametrize(
    "extra",
    [
        "urban_geography/AGENTS.md",
        "data/raw.parquet",
        "urban_geography/credentials.json",
        "pyspark/__init__.py",
        "../escape.py",
    ],
)
def test_extra_private_dependency_and_unsafe_payload_is_rejected(tiny_project, tmp_path, extra):
    inputs, entries = wheel_entries(tiny_project)
    entries[extra] = b"must not be packaged"
    with pytest.raises(ValueError, match="Unexpected wheel payload|Unsafe wheel entry"):
        package.verify_wheel(write_wheel(tmp_path / "bad.whl", entries), inputs)


@pytest.mark.parametrize("defect", ["changed_code", "missing_sql", "metadata", "entry_point"])
def test_source_resource_and_metadata_drift_is_rejected(tiny_project, tmp_path, defect):
    inputs, entries = wheel_entries(tiny_project)
    if defect == "changed_code":
        entries["urban_geography/__init__.py"] += b"# changed\n"
    elif defect == "missing_sql":
        del entries["urban_geography/sql/grid_proximity.sql"]
    elif defect == "metadata":
        name = "urban_geography-0.1.0.dist-info/METADATA"
        entries[name] = entries[name].replace(b"Version: 0.1.0", b"Version: 9.0.0")
    else:
        name = "urban_geography-0.1.0.dist-info/entry_points.txt"
        entries[name] = entries[name].replace(b":main", b":wrong")
    with pytest.raises(ValueError):
        package.verify_wheel(write_wheel(tmp_path / "bad.whl", entries), inputs)


@pytest.mark.parametrize("kind", ["existing", "data", "source"])
def test_output_refusal_precedes_build_process(tiny_project, tmp_path, monkeypatch, kind):
    output = {
        "existing": tmp_path / "exists",
        "data": tiny_project / "data" / "bad",
        "source": tiny_project / "src" / "bad",
    }[kind]
    if kind == "existing":
        output.mkdir()

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid outputs must not start a build process")

    monkeypatch.setattr(package.subprocess, "run", forbidden)
    with pytest.raises(ValueError):
        package.build_package(tiny_project, output)


def test_real_project_wheel_build_and_isolated_offline_install(tmp_path):
    output = tmp_path / "package"
    receipt = package.build_package(ROOT, output)
    assert receipt["status"] == "complete"
    assert receipt["source_stable_during_build"] is True
    assert receipt["dependencies_installed"] is False
    assert receipt["installed_wheel_check"]["spark_started"] is False
    assert receipt["installed_wheel_check"]["sql_resources_match"] is True
    assert receipt["installed_wheel_check"]["pyspark_provided_by_environment"] is True
    assert json.loads((output / "manifest.json").read_text()) == receipt
    assert (output / "wheels" / receipt["wheel"]["filename"]).is_file()
    assert not list(tmp_path.glob(".package-*"))
