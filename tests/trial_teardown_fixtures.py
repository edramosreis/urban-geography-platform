"""Load the rehearsal scripts under the same isolated names as other cleanup tests."""

import importlib.util
import sys

from trial_cleanup_command_fixtures import ROOT
from trial_cleanup_command_fixtures import command as command

for name in (
    "trial_teardown_simulation",
    "trial_data_cleanup",
    "trial_cleanup_receipt",
    "run_trial_cleanup",
    "check_trial_teardown",
):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
model = sys.modules["trial_teardown_simulation"]
rehearsal = sys.modules["check_trial_teardown"]
