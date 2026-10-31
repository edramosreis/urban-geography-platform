"""Load the local administration helper without adding it to the worker package."""

import copy
import importlib.util
import io
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("glue_control", ROOT / "scripts/glue_control.py")
assert SPEC is not None and SPEC.loader is not None
CONTROL = importlib.util.module_from_spec(SPEC)
sys.modules["glue_control"] = CONTROL
SPEC.loader.exec_module(CONTROL)


def make_control_case(frozen):
    plan = CONTROL.load_plan(frozen.path, frozen.pin)
    body = io.BytesIO((frozen.path / "release.json").read_bytes())
    s3 = SimpleNamespace(
        get_object=Mock(
            return_value={
                "Body": body,
                "ContentLength": len(body.getvalue()),
                "VersionId": "version-1",
            }
        )
    )
    glue = SimpleNamespace(get_job=Mock(return_value={"Job": copy.deepcopy(plan["job"])}))
    return SimpleNamespace(plan=plan, body=body, s3=s3, glue=glue, frozen=frozen)
