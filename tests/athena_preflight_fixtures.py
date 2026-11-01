"""Wholly synthetic identity/catalog observations, with the existing reader patched in tests."""

import importlib.util
import sys
from unittest.mock import Mock

from athena_control_fixtures import CONTROL  # noqa: F401 - registers script imports
from athena_plan_fixtures import ATHENA, OPTIONS, ROOT

spec = importlib.util.spec_from_file_location(
    "athena_preflight", ROOT / "scripts/athena_preflight.py"
)
assert spec is not None and spec.loader is not None
PREFLIGHT = importlib.util.module_from_spec(spec)
sys.modules["athena_preflight"] = PREFLIGHT
spec.loader.exec_module(PREFLIGHT)

CALLER = {
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/urban-geo-test-reader/offline-session",
    "UserId": "AROA" + "A" * 17 + ":offline-session",
}


def synthetic_table(plan):
    location = "s3://urban-geo-test-lake-123456789012-eu-west-1/warehouse/building_proximity"
    return {
        "Table": {
            "CatalogId": "123456789012",
            "DatabaseName": plan["binding"]["source_schema"],
            "Name": "building_proximity",
            "TableType": "EXTERNAL_TABLE",
            "VersionId": "1",
            "Parameters": {
                "table_type": "ICEBERG",
                "metadata_location": location + "/metadata/00000-offline.metadata.json",
            },
            "StorageDescriptor": {"Location": location},
        }
    }


def synthetic_readback(plan):
    recipe = PREFLIGHT.runtime_plan(**plan["target"])
    return {
        "status": "verified_snapshot_readback",
        "binding": plan["binding"],
        "table": "glue_catalog." + recipe["database"] + ".building_proximity",
        "location": recipe["warehouse"] + "building_proximity",
        "writer_receipt_sha256": "b" * 64,
        "verified_spark_settings": recipe["spark_settings"],
        "rows": 2,
        "table_uuid": "b6309a11-cc67-422d-8789-374651d36e67",
        "readback_matches": True,
        "metadata_stable": True,
        "verification_engine": "spark_iceberg",
    }


def make_case(tmp_path, monkeypatch):
    plan = ATHENA.read_plan(**OPTIONS)
    sts = Mock()
    sts.get_caller_identity.return_value = dict(CALLER)
    glue = Mock()
    glue.get_table.return_value = synthetic_table(plan)
    reader = Mock(return_value=synthetic_readback(plan))
    monkeypatch.setattr(PREFLIGHT, "read_snapshot", reader)
    options = {
        "caller_arn": CALLER["Arn"],
        "caller_user_id": CALLER["UserId"],
        "input_path": tmp_path / "input",
        "writer_receipt": tmp_path / "writer.json",
        "writer_receipt_sha256": "b" * 64,
        "session_factory": Mock(),
    }
    return plan, sts, glue, reader, options


spec = importlib.util.spec_from_file_location("athena_clients", ROOT / "scripts/athena_clients.py")
assert spec is not None and spec.loader is not None
CLIENTS = importlib.util.module_from_spec(spec)
sys.modules["athena_clients"] = CLIENTS
spec.loader.exec_module(CLIENTS)
