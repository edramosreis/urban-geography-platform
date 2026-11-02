"""Own seven read-only dbt tests and validate their bounded failure summaries."""

import re
from pathlib import Path
from typing import Any

from athena_control import canonical, inspect_query, write_once
from dbt_test_plan import TEST_NAMES, test_plan
from dbt_view_control import ViewController
from dbt_view_plan import normalized_sql

COLUMNS = ("failures", "should_warn", "should_error")


def failure_summary(response: dict[str, Any]) -> int:
    if len(canonical(response)) > 65536 or response.get("NextToken"):
        raise ValueError("Data-test result is oversized or paginated")
    result = response.get("ResultSet", {})
    columns = result.get("ResultSetMetadata", {}).get("ColumnInfo", [])
    if [(c.get("Name"), c.get("Type")) for c in columns] != list(
        zip(COLUMNS, ("bigint", "boolean", "boolean"), strict=True)
    ):
        raise ValueError("Data-test result columns differ")
    rows = result.get("Rows", [])
    if len(rows) != 2:
        raise ValueError("Expected exactly a header and one test summary")
    cells = []
    for row in rows:
        values = row.get("Data", [])
        if len(values) != 3 or any(
            set(v) != {"VarCharValue"} or not isinstance(v["VarCharValue"], str) for v in values
        ):
            raise ValueError("Malformed data-test summary cells")
        cells.append([v["VarCharValue"] for v in values])
    if cells[0] != list(COLUMNS) or re.fullmatch(r"0|[1-9][0-9]{0,18}", cells[1][0]) is None:
        raise ValueError("Unexpected test summary header/count")
    failures = int(cells[1][0])
    if failures > 2**63 - 1 or cells[1][1:] != ["true" if failures else "false"] * 2:
        raise ValueError("Test failure count and decision flags differ")
    return failures


class TestController(ViewController):
    def make_plan(self, project: Path, **target: str) -> dict[str, Any]:
        return test_plan(project, **target)

    def select_recipe(self, plan: dict[str, Any], sql: str, index: int) -> dict[str, Any]:
        matches = [q for q in plan["queries"] if normalized_sql(q["sql"]) == normalized_sql(sql)]
        if len(matches) != 1 or matches[0]["name"] in {s["name"] for s in self._steps}:
            raise ValueError("Unexpected or repeated data-test SQL")
        return dict(matches[0])

    def inspect_execution(
        self, raw: dict[str, Any], request: dict[str, Any], identity: str
    ) -> dict[str, Any]:
        return inspect_query(raw["QueryExecution"], request, identity)

    def report(self) -> dict[str, Any]:
        complete = len(self._steps) == len(TEST_NAMES) and all(
            s.get("outcome") == "query_succeeded" and "test_failures" in s for s in self._steps
        )
        passed = (
            complete and not self._blocked and all(s["test_failures"] == 0 for s in self._steps)
        )
        status = (
            "data_tests_passed"
            if passed
            else (
                "data_tests_failed" if complete and not self._blocked else "data_tests_not_verified"
            )
        )
        return {
            "status": status,
            "intent_sha256": self.intent_sha256,
            "queries_reserved": len(self._steps),
            "blocked": self._blocked,
            "data_scanned_bytes": self._scanned,
            "queries": self._steps,
            "data_tests_executed": sum("test_failures" in s for s in self._steps),
            "data_tests_passed": passed,
            "result_verified": False,
        }

    def get_query_results(self, **params: Any) -> dict[str, Any]:
        identity = self._owned("results", params)
        try:
            response = self.client.get_query_results(QueryExecutionId=identity, MaxResults=3)
            failures = failure_summary(response)
            self._gate()
            index, step = next(
                (i, s) for i, s in enumerate(self._steps) if s.get("query_id") == identity
            )
            write_once(
                self.output / f"query-{index + 1:02}" / "test-summary.json",
                canonical({"query_id": identity, "test_name": step["name"], "failures": failures}),
            )
            step["test_failures"] = failures
            return response
        except BaseException:
            self._blocked = True
            raise
