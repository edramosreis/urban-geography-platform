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
    if "license_sha256" in inputs:
        entries[prefix + "licenses/LICENSE"] = (root / "LICENSE").read_bytes()
        entries[prefix + "METADATA"] = entries[prefix + "METADATA"].replace(
            b"\n\n", b"\nLicense-Expression: MIT\nLicense-File: LICENSE\n\n"
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


def test_release_rechecks_archived_project_against_current_source(
    tiny_project, tmp_path, monkeypatch
):
    import urban_geography.glue_release as release
    from urban_geography.glue_bundle import project_requirements, write_archive
    from urban_geography.glue_dependencies import digest

    inputs, entries = wheel_entries(tiny_project)
    wheel = write_wheel(tmp_path / "urban_geography-0.1.0-py3-none-any.whl", entries)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    archive = bundle / "urban-geography.gluewheels.zip"
    members = write_archive([wheel], project_requirements("", "0.1.0", digest(wheel)), archive)
    (tiny_project / "uv.lock").write_text("package=[]")
    plan = {"requirements": "", "input_sha256": {}, "pins": {}}
    monkeypatch.setattr(release, "export_dependencies", lambda root: plan)
    checked = dict.fromkeys(
        (
            "all_package_versions_match",
            "provided_spark_and_py4j_preserved",
            "sql_resources_match",
            "native_library_operations_pass",
            "fresh_venv_without_system_site_packages",
        ),
        True,
    ) | {"spark_started": False}
    manifest = {
        "status": "complete",
        "bundle": {
            "sha256": digest(archive),
            "bytes": archive.stat().st_size,
            "members_sha256": members,
        },
        "installer_options": ["--no-index", "--no-deps"],
        "installed_bundle_check": checked,
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    release.verify_bundle(tiny_project, bundle)
    (tiny_project / "src/urban_geography/__init__.py").write_text("# changed since build")
    with pytest.raises(ValueError, match="differs from source"):
        release.verify_bundle(tiny_project, bundle)


@pytest.fixture
def licensed_project(tiny_project):
    project = tiny_project / "pyproject.toml"
    project.write_text(
        project.read_text().replace(
            "[project.scripts]", 'license="MIT"\nlicense-files=["LICENSE"]\n[project.scripts]'
        )
    )
    (tiny_project / "LICENSE").write_text("MIT License\nSynthetic copyright fixture\n")
    return tiny_project


def test_declared_license_is_included_and_pinned(licensed_project, tmp_path):
    inputs, entries = wheel_entries(licensed_project)
    result = package.verify_wheel(write_wheel(tmp_path / "licensed.whl", entries), inputs)
    assert result["source_and_sql_match"] is True
    (licensed_project / "LICENSE").write_text("Changed after the package input was frozen")
    assert package.package_inputs(licensed_project) != inputs


@pytest.mark.parametrize("defect", ["missing", "changed", "extra", "expression", "file_header"])
def test_license_drift_and_extra_license_payload_are_rejected(licensed_project, tmp_path, defect):
    inputs, entries = wheel_entries(licensed_project)
    prefix = "urban_geography-0.1.0.dist-info/"
    if defect == "missing":
        del entries[prefix + "licenses/LICENSE"]
    elif defect == "changed":
        entries[prefix + "licenses/LICENSE"] = b"Different license bytes"
    elif defect == "extra":
        entries[prefix + "licenses/credentials.txt"] = b"Must not enter the wheel"
    elif defect == "expression":
        entries[prefix + "METADATA"] = entries[prefix + "METADATA"].replace(b"MIT", b"Apache-2.0")
    else:
        entries[prefix + "METADATA"] = entries[prefix + "METADATA"].replace(
            b"License-File: LICENSE", b"License-File: private.txt"
        )
    with pytest.raises(ValueError, match="license|LICENSE|Unexpected wheel payload"):
        package.verify_wheel(write_wheel(tmp_path / "bad-license.whl", entries), inputs)
