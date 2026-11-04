"""Pinned synthetic controller receipts and original SELECT/view intent records."""

import hashlib
import importlib.util
import json
import sys

from trial_service_fixtures import ROOT, make_services

for name in ("dbt_view_plan", "trial_quiescence_plan"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
plans = sys.modules["trial_quiescence_plan"]


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return plans.Record(path, hashlib.sha256(path.read_bytes()).hexdigest())


def make_evidence(frozen, folder):
    value = make_services(frozen)
    value.frozen = frozen
    target = frozen.manifest["identity"]["target"]
    summary = frozen.manifest["identity"]["summary_sha256"]
    source = sys.modules["athena_read_plan"].read_plan(
        **target, snapshot_id=123, source_run_sha256=summary
    )
    models = sys.modules["dbt_view_plan"].compiled_models(source["binding"])
    schema = source["binding"]["source_schema"]
    queries = [
        {
            "name": name,
            "sql": f'create or replace view "AwsDataCatalog"."{schema}"."{name}" as\n'
            + models[name],
        }
        for name in sys.modules["dbt_view_plan"].VIEWS
    ]
    for row in queries:
        row["sql_sha256"] = hashlib.sha256(row["sql"].encode()).hexdigest()
    parent = save(
        folder / "parent.json", {"contract_version": 1, "source_plan": source, "queries": queries}
    )
    value.job_receipt = save(
        folder / "job.json",
        {
            "job_name": value.job["Name"],
            "run_id": "jr_owned",
            "release_id": frozen.manifest["release_id"],
            "release_sha256": frozen.pin,
            "outcome": "stop_unconfirmed",
            "terminal_confirmed": False,
        },
    )
    evidence = []
    value.requests = {}
    for index, sql in enumerate(
        [source["request_template"]["QueryString"], *[r["sql"] for r in queries]]
    ):
        request = source["request_template"] | {
            "QueryString": sql,
            "ClientRequestToken": str(index + 1) * 64,
        }
        sql_pin = hashlib.sha256(sql.encode()).hexdigest()
        intent = {
            "contract_version": 1,
            "request": request,
            "plan": source
            if index == 0
            else {"query_sha256": sql_pin, "workgroup": source["workgroup"]},
        }
        if index:
            intent["parent_intent_sha256"] = parent.sha256
        intent_record = save(folder / f"intent-{index}.json", intent)
        query_id = "query-" + str(index)
        receipt = save(
            folder / f"receipt-{index}.json",
            {
                "query_id": query_id,
                "intent_sha256": intent_record.sha256,
                "query_sha256": sql_pin,
                "terminal_confirmed": False,
                "outcome": "stop_unconfirmed",
            },
        )
        evidence.append(plans.QueryEvidence(intent_record, receipt, parent if index else None))
        value.requests[query_id] = request
    value.evidence = tuple(evidence)
    return value
