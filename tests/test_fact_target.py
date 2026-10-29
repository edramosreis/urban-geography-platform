"""Invalid explicit publication targets must fail before any Spark or input access."""

import pytest

from urban_geography.lakehouse import publish_facts


@pytest.mark.parametrize("field", ["catalog_name", "namespace", "table_name"])
@pytest.mark.parametrize(
    "value", ["", "other.table", "wrong-name", "1name", "NAME", "x; DROP TABLE y"]
)
def test_unsafe_target_is_rejected_before_spark_or_input_access(field, value):
    # None sentinels make any attempted session/input access fail this guard test.
    with pytest.raises(ValueError, match=f"Iceberg {field} must be a safe lowercase identifier"):
        publish_facts(None, None, **{field: value})
