"""Load the local administration helper without adding it to the worker package."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("glue_control", ROOT / "scripts/glue_control.py")
assert SPEC is not None and SPEC.loader is not None
CONTROL = importlib.util.module_from_spec(SPEC)
sys.modules["glue_control"] = CONTROL
SPEC.loader.exec_module(CONTROL)
