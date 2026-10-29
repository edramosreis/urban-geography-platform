"""Runtime-check refusal guards run before any JVM or subprocess can start."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("glue_check", ROOT / "scripts/check_glue_runtime.py")
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


@pytest.mark.parametrize("defect", ["existing_output", "data_output", "corrupt_bundle"])
def test_check_refuses_invalid_input_before_runtime_start(tmp_path, monkeypatch, defect):
    def forbidden(*args, **kwargs):
        pytest.fail("A refusal must happen before Spark or a child process starts")

    monkeypatch.setattr(CHECKER.SedonaContext, "builder", forbidden)
    monkeypatch.setattr(CHECKER.subprocess, "run", forbidden)
    bundle = tmp_path / "iceberg-aws-bundle-1.10.2.jar"
    bundle.write_bytes(b"corrupt")
    output = tmp_path / "check"
    if defect == "existing_output":
        output.mkdir()
        sentinel = output / "keep.txt"
        sentinel.write_text("keep", encoding="utf-8")
        message = "already exists"
    elif defect == "data_output":
        output = ROOT / "data" / "unapproved-runtime-check"
        monkeypatch.chdir(tmp_path)
        message = "outside retained data"
    else:
        # Isolate the checksum gate from dependencies on installed base JARs.
        monkeypatch.setattr(CHECKER, "JARS", CHECKER.JARS[-1:])
        message = "checksum differs"
    with pytest.raises(ValueError, match=message):
        CHECKER.check_runtime(bundle, output)
    if defect == "existing_output":
        assert sentinel.read_text(encoding="utf-8") == "keep"
        assert list(output.iterdir()) == [sentinel]
    else:
        assert not output.exists()
    assert bundle.read_bytes() == b"corrupt"
