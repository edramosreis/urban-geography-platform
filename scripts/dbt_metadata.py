"""Allow only bounded catalog work for the prepared two-view dbt graph."""

import re
from typing import Any

from athena_read_plan import same
from dbt_rpc import METHODS
from dbt_view_control import ViewController
from dbt_view_plan import VIEWS


class DbtBroker:
    def __init__(self, clients: dict[str, Any], control: ViewController) -> None:
        if clients["athena"] is not control.client:
            raise ValueError("dbt and query controller must share their owned Athena client")
        self.clients, self.control = clients, control
        source = control.plan()["source_plan"]
        self.account = source["target"]["account_id"]
        self.database = source["binding"]["source_schema"]
        self.calls = self.metadata_calls = 0
        self.failed = False
        self.versions_seen: set[str] = set()
        self.deletable: dict[str, set[str]] = {}

    def dispatch(self, message: dict[str, Any]) -> dict[str, Any]:
        try:
            self.calls += 1
            if (
                self.failed
                or self.calls > 64
                or set(message) != {"version", "sequence", "service", "operation", "parameters"}
                or type(message["version"]) is not int
                or message["version"] != 1
                or type(message["sequence"]) is not int
                or message["sequence"] != self.calls
            ):
                raise ValueError("Invalid or excessive dbt IPC request")
            service, operation, params = (
                message["service"],
                message["operation"],
                message["parameters"],
            )
            if (
                service not in METHODS
                or operation not in METHODS[service]
                or not isinstance(params, dict)
            ):
                raise ValueError("Unsupported dbt service operation")
            if service == "athena":
                return getattr(self.control, operation)(**params)
            self.metadata_calls += 1
            if self.metadata_calls > 32:
                raise ValueError("dbt metadata request ceiling reached")
            return self._metadata(service, operation, params)
        except BaseException:
            self.failed = True
            raise

    def _metadata(self, service: str, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        if service == "sts":
            if params:
                raise ValueError("Unexpected caller arguments")
            response = self.clients[service].get_caller_identity()
            if response.get("Account") != self.account:
                raise ValueError("dbt caller account differs")
            return {key: response[key] for key in ("Account", "Arn", "UserId")}
        glue = self.clients["glue"]
        ancestors = {"CatalogId": self.account, "DatabaseName": self.database}
        if operation == "get_databases" and not params:
            response = glue.get_databases(CatalogId=self.account, MaxResults=10)
            rows = response.get("DatabaseList", [])
            if response.get("NextToken") or len(rows) != 1 or rows[0].get("Name") != self.database:
                raise ValueError("Unexpected database discovery result/page")
            return {"DatabaseList": [{"Name": self.database}]}
        if operation == "get_tables" and same(params, ancestors):
            response = glue.get_tables(**ancestors, MaxResults=10)
            rows = response.get("TableList", [])
            names = [row.get("Name") for row in rows]
            if (
                response.get("NextToken")
                or len(rows) > 3
                or len(set(names)) != len(names)
                or not set(names) <= {"building_proximity", *VIEWS}
                or "building_proximity" not in names
            ):
                raise ValueError("Unexpected relation discovery result/page")
            return {
                "TableList": [
                    {key: row[key] for key in ("Name", "TableType", "Parameters") if key in row}
                    for row in rows
                ]
            }
        view = params.get("TableName")
        if view not in VIEWS:
            raise ValueError("Metadata mutation/read is not for a Gold view")
        base = {"DatabaseName": self.database, "TableName": view}
        if (
            operation == "get_table_versions"
            and same(params, base)
            and view not in self.versions_seen
        ):
            self.versions_seen.add(view)
            response = glue.get_table_versions(**base, CatalogId=self.account, MaxResults=20)
            rows = response.get("TableVersions", [])
            if response.get("NextToken") or len(rows) > 12:
                raise ValueError("View history exceeds metadata limit")
            versions: list[dict[str, Any]] = []
            for row in rows:
                identity = row.get("VersionId")
                table = row.get("Table", {})
                if (
                    not isinstance(identity, str)
                    or re.fullmatch(r"[0-9]{1,12}", identity) is None
                    or table.get("Name") != view
                    or table.get("VersionId") != identity
                ):
                    raise ValueError("Unexpected view version identity")
                versions.append(
                    {
                        "VersionId": identity,
                        "Table": {
                            "Name": view,
                            "VersionId": identity,
                            "StorageDescriptor": table.get("StorageDescriptor", {}),
                        },
                    }
                )
            ids: list[str] = [row["VersionId"] for row in versions]
            if len(set(ids)) != len(ids):
                raise ValueError("Duplicate view versions")
            self.deletable[view] = set(sorted(ids, key=int, reverse=True)[4:])
            return {"TableVersions": versions}
        if (
            operation == "delete_table_version"
            and set(params) == {*base, "CatalogId", "VersionId"}
            and same({k: params[k] for k in ancestors}, ancestors)
            and params["VersionId"] in self.deletable.get(view, set())
        ):
            self.deletable[view].remove(params["VersionId"])
            glue.delete_table_version(**params)
            return {}
        raise ValueError("Unexpected or repeated view metadata operation")
