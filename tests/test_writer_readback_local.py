"""The local demonstration cannot overwrite results or escape the private artifact tree."""

import importlib.util
from unittest.mock import Mock

import pytest
from writer_readback_fixtures import ROOT

SPEC = importlib.util.spec_from_file_location(
    "check_writer_readback", ROOT / "scripts/check_writer_readback.py"
)
assert SPEC is not None and SPEC.loader is not None
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


@pytest.mark.parametrize(
    "kind", ["existing", "outside", "contains_input", "inside_input", "symlink"]
)
def test_local_check_rejects_unsafe_output_before_writer_start(tmp_path, monkeypatch, kind):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    source = artifacts / "prepared"
    source.mkdir()
    (source / "summary.json").write_text("retained", encoding="utf-8")
    output = artifacts / "fresh"
    if kind == "existing":
        output.mkdir()
        (output / "sentinel").write_text("retained", encoding="utf-8")
    elif kind == "outside":
        output = tmp_path / "outside"
    elif kind == "contains_input":
        output = artifacts
    elif kind == "inside_input":
        output = source / "fresh"
    else:
        output.symlink_to(artifacts / "unused", target_is_directory=True)
    monkeypatch.setattr(CHECK, "ARTIFACTS", artifacts)
    writer = Mock()
    monkeypatch.setattr(CHECK, "run_writer", writer)
    with pytest.raises(ValueError):
        CHECK.check_local(source, "a" * 64, output)
    writer.assert_not_called()
    assert (source / "summary.json").read_text() == "retained"
    assert not (artifacts / "unused").exists()
    assert not (tmp_path / "outside").exists()
    if kind == "existing":
        assert (output / "sentinel").read_text() == "retained"
