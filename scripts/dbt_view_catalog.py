"""Read two ordinary Athena views from Glue and require the pinned definitions."""

import base64
import binascii
import hashlib
import json
import re
from typing import Any, Protocol

from athena_control import canonical
from dbt_view_plan import VIEWS, compiled_models

MAX_VIEW_BYTES = 64 * 1024
KEYWORDS = set(
    (
        "with select from as for version of group by order count sum "
        "case when then else end cast double"
    ).split()
)
TOKEN = re.compile(
    r"--[^\r\n]*|/\*.*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|"
    r"[A-Za-z_][A-Za-z_0-9$]*|[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|"
    r"<=|>=|<>|!=|\|\||[(),.*+/<>=;%-]",
    re.S,
)


class CatalogClient(Protocol):
    def get_table(self, **kwargs: Any) -> dict[str, Any]: ...


def sql_tokens(sql: str) -> list[str]:
    """Conservative lexical equality, not arbitrary SQL semantic equivalence."""
    if not isinstance(sql, str) or len(sql.encode()) > 16384:
        raise ValueError("View SQL exceeds the fixed query ceiling")
    tokens = []
    end = 0
    for match in TOKEN.finditer(sql):
        if sql[end : match.start()].strip():
            raise ValueError("Unsupported view SQL token")
        token = match[0]
        end = match.end()
        if token.startswith(("--", "/*")):
            continue
        if token.startswith("'"):
            tokens.append(token)  # Preserve the source-run literal exactly.
        elif token.startswith('"'):
            tokens.append("identifier:" + token[1:-1].replace('""', '"').lower())
        elif token[0].isalpha() or token[0] == "_":
            tokens.append(
                ("keyword:" if token.lower() in KEYWORDS else "identifier:") + token.lower()
            )
        else:
            tokens.append(token)
    if sql[end:].strip() or not tokens:
        raise ValueError("Unsupported or empty view SQL")
    return tokens


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate view-definition JSON field")
        value[key] = item
    return value


def invalid_constant(value: str) -> Any:
    raise ValueError("Non-finite view-definition JSON value")


def column_pairs(value: Any, name_key: str, type_key: str) -> list[tuple[str, str]]:
    if (
        not isinstance(value, list)
        or len(value) > 8
        or any(
            not isinstance(c, dict)
            or not isinstance(c.get(name_key), str)
            or not isinstance(c.get(type_key), str)
            for c in value
        )
    ):
        raise ValueError("Malformed view columns")
    return [(c[name_key], c[type_key]) for c in value]


def columns(name: str, *, glue: bool = False) -> list[tuple[str, str]]:
    names = ["source_release", "area_id", "source_run_sha256"]
    if name == "grid_proximity":
        names.append("grid_id")
    return [(n, "string" if glue else "varchar") for n in names] + [
        ("mapped_buildings", "bigint"),
        ("buildings_within", "bigint"),
        ("share_within", "double"),
    ]


def view_state(table: dict[str, Any], plan: dict[str, Any], name: str) -> dict[str, Any]:
    if not isinstance(table, dict):
        raise ValueError("Malformed view table")
    source = plan["source_plan"]
    account, schema = source["target"]["account_id"], source["binding"]["source_schema"]
    if name not in VIEWS:
        raise ValueError("Unexpected view name")
    parameters = table.get("Parameters", {})
    storage = table.get("StorageDescriptor", {})
    if (
        not isinstance(parameters, dict)
        or not isinstance(storage, dict)
        or len(canonical(parameters)) > 4096
    ):
        raise ValueError("Malformed or oversized view parameters/storage")
    if (
        (table.get("CatalogId"), table.get("DatabaseName"), table.get("Name"))
        != (account, schema, name)
        or table.get("TableType") != "VIRTUAL_VIEW"
        or parameters.get("presto_view") != "true"
        or any(k in parameters for k in ("table_type", "metadata_location"))
        or any(k in table for k in ("TargetTable", "FederatedTable", "ViewDefinition"))
        or table.get("IsMultiDialectView", False) is not False
        or table.get("IsRegisteredWithLakeFormation", False) is not False
        or table.get("PartitionKeys")
        or storage.get("Location") not in (None, "")
        or any(
            storage.get(k)
            for k in ("AdditionalLocations", "SchemaReference", "InputFormat", "OutputFormat")
        )
        or column_pairs(storage.get("Columns"), "Name", "Type") != columns(name, glue=True)
    ):
        raise ValueError("Unexpected view catalog identity, kind or columns")
    version = table.get("VersionId")
    if not isinstance(version, str) or re.fullmatch(r"[0-9]{1,20}", version) is None:
        raise ValueError("Missing or unsupported view catalog version")
    original = table.get("ViewOriginalText")
    expanded = table.get("ViewExpandedText")
    if (
        not isinstance(original, str)
        or len(original.encode()) > MAX_VIEW_BYTES
        or not original.startswith("/* Presto View: ")
        or not original.endswith(" */")
        or expanded not in (None, "", "/* Presto View */")
    ):
        raise ValueError("Unsupported Athena view encoding")
    try:
        content = base64.b64decode(original[len("/* Presto View: ") : -len(" */")], validate=True)
        definition = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=invalid_constant,
        )
    except (ValueError, UnicodeError, binascii.Error) as error:
        raise ValueError("Malformed Athena view definition") from error
    if (
        not isinstance(definition, dict)
        or len(canonical(definition)) > 24 * 1024
        or not {"originalSql", "catalog", "schema", "columns"} <= set(definition)
        or set(definition)
        - {"originalSql", "catalog", "schema", "columns", "owner", "runAsInvoker"}
    ):
        raise ValueError("Unsupported Athena view definition fields")
    if (
        not isinstance(definition["catalog"], str)
        or definition["catalog"].lower() != "awsdatacatalog"
        or definition["schema"] != schema
        or column_pairs(definition["columns"], "name", "type") != columns(name)
        or ("runAsInvoker" in definition and type(definition["runAsInvoker"]) is not bool)
        or (definition.get("owner") is not None and not isinstance(definition["owner"], str))
        or sql_tokens(definition["originalSql"])
        != sql_tokens(compiled_models(source["binding"])[name])
    ):
        raise ValueError("View SQL, binding or output columns differ from the prepared model")
    projection = {
        "catalog_id": account,
        "database": schema,
        "name": name,
        "version_id": version,
        "original_text_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "expanded_text": expanded,
        "definition_sha256": hashlib.sha256(canonical(definition)).hexdigest(),
        "parameters_sha256": hashlib.sha256(canonical(parameters)).hexdigest(),
        "definition": definition,
        "parameters": parameters,
        "columns": columns(name),
    }
    # SDK timestamps/HTTP fields are not needed and never enter durable JSON.
    return projection


def observe_views(client: CatalogClient, plan: dict[str, Any]) -> dict[str, Any]:
    source = plan["source_plan"]
    return {
        name: view_state(
            client.get_table(
                CatalogId=source["target"]["account_id"],
                DatabaseName=source["binding"]["source_schema"],
                Name=name,
            )["Table"],
            plan,
            name,
        )
        for name in VIEWS
    }
