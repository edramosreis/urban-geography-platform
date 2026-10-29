"""Frozen dependency selection, platform policy and wheel-cache integrity."""

import json
import zipfile
from pathlib import Path

import pytest

import urban_geography.glue_dependencies as deps

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("filename", "accepted"),
    [
        ("numpy-1.26.4-cp311-cp311-manylinux_2_28_x86_64.whl", True),
        ("numpy-1.26.4-cp311-cp311-manylinux2014_x86_64.whl", True),
        ("attrs-26.1.0-py3-none-any.whl", True),
        ("six-1.17.0-py2.py3-none-any.whl", True),
        ("numpy-1.26.4-cp312-cp312-manylinux_2_28_x86_64.whl", False),
        ("numpy-1.26.4-cp311-cp311-win_amd64.whl", False),
        ("numpy-1.26.4-cp311-cp311-manylinux_2_28_aarch64.whl", False),
        ("numpy-1.26.4-cp311-cp311-manylinux_2_36_x86_64.whl", False),
        ("numpy-1.26.4-cp311-cp311-musllinux_1_2_x86_64.whl", False),
        ("numpy-1.26.4.tar.gz", False),
    ],
)
def test_explicit_python_and_linux_target(filename, accepted):
    assert deps.compatible_wheel(filename) is accepted


def test_real_locked_export_excludes_managed_and_development_packages():
    before = deps.digest(ROOT / "uv.lock")
    plan = deps.export_dependencies(ROOT)
    assert {"apache-sedona", "numpy", "pandas", "pyarrow", "pyproj", "shapely", "requests"} <= plan[
        "pins"
    ].keys()
    assert not {"pyspark", "py4j", "pytest", "ruff", "mypy"} & plan["pins"].keys()
    assert "--hash=sha256:" in plan["requirements"]
    assert plan["provided"] == {"pyspark": "3.5.6", "py4j": "0.10.9.7"}
    assert deps.digest(ROOT / "uv.lock") == before


@pytest.fixture
def wheel_cache(tmp_path):
    wheels = tmp_path / "wheels"
    wheels.mkdir()
    wheel = wheels / "numpy-1.26.4-cp311-cp311-manylinux_2_28_x86_64.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("numpy-1.26.4.dist-info/METADATA", "Name: numpy\nVersion: 1.26.4\n")
    locked = {
        "package": [
            {
                "name": "numpy",
                "version": "1.26.4",
                "wheels": [
                    {
                        "url": "https://files.pythonhosted.org/test/" + wheel.name,
                        "hash": "sha256:" + deps.digest(wheel),
                        "size": wheel.stat().st_size,
                    }
                ],
            }
        ]
    }
    # JSON values are also valid TOML strings; only this narrow fixture shape is emitted.
    item = locked["package"][0]
    (tmp_path / "uv.lock").write_text(
        '[[package]]\nname="numpy"\nversion="1.26.4"\nwheels=[{'
        + ",".join(key + "=" + json.dumps(value) for key, value in item["wheels"][0].items())
        + "}]\n",
        encoding="utf-8",
    )
    plan = {
        "pins": {"numpy": "1.26.4"},
        "input_sha256": {"uv.lock": deps.digest(tmp_path / "uv.lock")},
    }
    return tmp_path, wheels, wheel, plan


def test_complete_wheel_cache_matches_exact_lock_artifacts(wheel_cache):
    root, wheels, wheel, plan = wheel_cache
    result = deps.verify_cache(root, wheels, plan)
    assert len(result) == 1 and result[0]["sha256"] == deps.digest(wheel)


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "changed_bytes",
        "wrong_version",
        "managed",
        "wrong_platform",
        "extra",
        "lock_drift",
    ],
)
def test_cache_drift_is_rejected(wheel_cache, defect):
    root, wheels, wheel, plan = wheel_cache
    if defect == "missing":
        wheel.unlink()
    elif defect == "changed_bytes":
        wheel.write_bytes(wheel.read_bytes() + b"changed")
    elif defect in {"wrong_version", "managed"}:
        with zipfile.ZipFile(wheel, "w") as archive:
            name = "pyspark" if defect == "managed" else "numpy"
            archive.writestr("test.dist-info/METADATA", f"Name: {name}\nVersion: 9.0.0\n")
    elif defect == "wrong_platform":
        wheel.rename(wheels / wheel.name.replace("manylinux_2_28_x86_64", "win_amd64"))
    elif defect == "extra":
        (wheels / "credentials.json").write_text("{}")
    else:
        path = root / "uv.lock"
        path.write_text(path.read_text() + "\n")
    with pytest.raises(ValueError):
        deps.verify_cache(root, wheels, plan)


@pytest.mark.parametrize("kind", ["existing", "data", "source"])
def test_bad_download_output_fails_before_export_or_network(tmp_path, monkeypatch, kind):
    output = tmp_path / {"existing": "exists", "data": "data/cache", "source": "src/cache"}[kind]
    if kind == "existing":
        output.mkdir()

    def forbidden(*args):
        pytest.fail("Invalid output must not export or start a download")

    monkeypatch.setattr(deps, "export_dependencies", forbidden)
    with pytest.raises(ValueError):
        deps.fetch_dependencies(tmp_path, output)
