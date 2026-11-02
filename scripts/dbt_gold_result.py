"""Own one release-view SELECT and compare its bounded result with the pinned pilot summary."""

import csv
import io
from pathlib import Path
from typing import Any

from athena_control import canonical, inspect_query, write_once
from athena_read_plan import same
from athena_results import (
    COLUMNS,
    MAX_RESULT_BYTES,
    compare_gold_csv,
    expected_gold,
    successful_observation,
)
from athena_runner import pinned_record
from dbt_view_control import ViewController
from dbt_view_plan import view_build_plan
from prepare_dbt_project import sha256

TYPES = ("varchar", "varchar", "varchar", "bigint", "bigint", "double")


def gold_result_plan(project: Path, **target: str) -> dict[str, Any]:
    base = view_build_plan(project, **target)
    schema = base["source_plan"]["binding"]["source_schema"]
    sql = (
        "SELECT "
        + ", ".join(COLUMNS)
        + f' FROM "AwsDataCatalog"."{schema}"."release_proximity"'
        + " ORDER BY source_release, area_id LIMIT 2"
    )
    return base | {
        "kind": "gold_result",
        "queries": [{"name": "release_result", "sql": sql, "sql_sha256": sha256(sql.encode())}],
        "limits": base["limits"] | {"max_queries": 1},
    }


def compare_result(response: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    if len(canonical(response)) > MAX_RESULT_BYTES or response.get("NextToken"):
        raise ValueError("Gold result is oversized or paginated")
    if "UpdateCount" in response and (
        type(response["UpdateCount"]) is not int or response["UpdateCount"] != 0
    ):
        raise ValueError("Gold SELECT returned an unexpected update count")
    result = response.get("ResultSet", {})
    columns = result.get("ResultSetMetadata", {}).get("ColumnInfo", [])
    if [(c.get("Name"), c.get("Type")) for c in columns] != list(zip(COLUMNS, TYPES, strict=True)):
        raise ValueError("Gold result columns/types differ")
    rows = result.get("Rows", [])
    if len(rows) != 2:
        raise ValueError("Expected a header and exactly one release-summary row")
    values = []
    for row in rows:
        cells = row.get("Data", [])
        if len(cells) != len(COLUMNS) or any(
            set(c) != {"VarCharValue"}
            or not isinstance(c["VarCharValue"], str)
            or len(c["VarCharValue"]) > 256
            for c in cells
        ):
            raise ValueError("Malformed Gold result cells")
        values.append([c["VarCharValue"] for c in cells])
    stream = io.StringIO(newline="")
    csv.writer(stream).writerows(values)
    return compare_gold_csv(stream.getvalue().encode(), expected)


class GoldController(ViewController):
    def make_plan(self, project: Path, **target: str) -> dict[str, Any]:
        return gold_result_plan(project, **target)

    def inspect_execution(
        self, raw: dict[str, Any], request: dict[str, Any], identity: str
    ) -> dict[str, Any]:
        return inspect_query(raw["QueryExecution"], request, identity)

    def verify_result(self, query_id: str, *, summary_path: Path) -> dict[str, Any]:
        identity = self._owned("results", {"QueryExecutionId": query_id})
        try:
            plan = self.plan()
            pin = plan["source_plan"]["binding"]["source_run_sha256"]
            expected = expected_gold(summary_path, pin)
            attempt_path = self.output / "query-01/intent.json"
            attempt_pin = self._steps[0]["intent_sha256"]
            request = pinned_record(attempt_path, attempt_pin)["request"]
            cap = plan["source_plan"]["workgroup"]["Configuration"]["BytesScannedCutoffPerQuery"]
            before = successful_observation(self.client, request, identity, cap)
            if not same(before, self._steps[0]["last_observation"]):
                raise ValueError("Gold query metadata changed before result verification")
            response = self.client.get_query_results(QueryExecutionId=identity, MaxResults=3)
            row = compare_result(response, expected)
            after = successful_observation(self.client, request, identity, cap)
            if not same(before, after):
                raise ValueError("Gold query metadata changed during result verification")
            self._gate()
            pinned_record(attempt_path, attempt_pin)
            if expected_gold(summary_path, pin) != expected:
                raise ValueError("Reference summary changed during Gold result verification")
            raw = canonical(response)
            write_once(self.output / "query-01/results.json", raw)
            proof = {
                "status": "gold_result_matches",
                "query_id": identity,
                "controller_intent_sha256": self.intent_sha256,
                "query_sha256": plan["queries"][0]["sql_sha256"],
                "source_summary_sha256": pin,
                "row": row,
                "expected": expected,
                "response_sha256": sha256(raw),
                "data_scanned_bytes": after["data_scanned_bytes"],
            }
            write_once(self.output / "query-01/gold-result.json", canonical(proof))
            self._steps[0]["gold_result"] = proof
            return proof
        except BaseException:
            self._blocked = True
            raise

    def report(self) -> dict[str, Any]:
        complete = (
            len(self._steps) == 1
            and self._steps[0].get("outcome") == "query_succeeded"
            and "gold_result" in self._steps[0]
            and not self._blocked
        )
        return {
            "status": "gold_result_matches" if complete else "gold_result_not_verified",
            "intent_sha256": self.intent_sha256,
            "queries_reserved": len(self._steps),
            "blocked": self._blocked,
            "data_scanned_bytes": self._scanned,
            "queries": self._steps,
            "result_verified": complete,
            "scope": (
                "One release-view row versus pinned pilot summary; "
                "no grid-level or live-service proof"
            ),
        }
