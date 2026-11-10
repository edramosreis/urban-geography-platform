"""Pure release-bound infrastructure targets and surviving observer identity."""

import json
import sys
from collections import Counter
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from trial_infrastructure_fixtures import make_case, plans


def test_plan_has_only_release_derived_primary_names_and_a_surviving_caller(
    frozen, tmp_path, monkeypatch
):
    case = make_case(frozen, tmp_path, monkeypatch)
    assert Counter(t["kind"] for t in case.plan["targets"]) == {
        "bucket": 2,
        "database": 1,
        "job": 1,
        "workgroup": 1,
        "role": 5,
        "policy": 4,
        "log_group": 2,
    }
    assert case.plan["limits"]["max_calls"] == 18 and case.plan["read_only"]
    assert not case.plan["deletion_authorized"] and len(case.pin) == 64
    writer = next(
        t
        for t in case.plan["targets"]
        if t["kind"] == "role" and t["name"].endswith("-glue-writer")
    )
    assert ":role/service-role/" in writer["arn"]
    assert all(not c.method_calls for c in case.clients.values())
    assert not case.output.exists()


@pytest.mark.parametrize(
    "suffix", ["glue-writer", "glue-operator", "athena-reader", "dbt-view-builder", "trial-cleanup"]
)
def test_roles_being_removed_cannot_be_the_post_teardown_observer(frozen, suffix):
    owner = frozen.manifest["identity"]["target"]["account_id"]
    deployment = frozen.manifest["identity"]["target"]["deployment_name"]
    with pytest.raises(ValueError):
        plans.absence_plan(
            frozen.path, frozen.pin, f"arn:aws:iam::{owner}:role/{deployment}-{suffix}"
        )


@pytest.mark.parametrize(
    "principal",
    [
        "arn:aws:iam::000000000000:role/login",
        "arn:aws:iam::123456789012:root",
        "arn:aws:iam::123456789012:user/login",
    ],
)
def test_foreign_account_root_or_user_principals_are_refused(frozen, principal):
    with pytest.raises(ValueError):
        plans.absence_plan(frozen.path, frozen.pin, principal)


def test_changed_release_pin_is_refused(frozen):
    with pytest.raises(ValueError):
        plans.absence_plan(frozen.path, "0" * 64, "arn:aws:iam::123456789012:role/login")


def test_cli_only_prints_a_local_plan_summary(frozen, monkeypatch, capsys):
    source = Mock()
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=source))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "trial_infrastructure_plan.py",
            "--release",
            str(frozen.path),
            "--release-sha256",
            frozen.pin,
            "--source-role-arn",
            "arn:aws:iam::123456789012:role/login",
        ],
    )
    plans.main()
    value = json.loads(capsys.readouterr().out)
    assert value["mode"] == "plan_only" and value["targets"] == 16
    assert (
        not value["credentials_read"] and not value["output_created"] and value["aws_requests"] == 0
    )
    assert "123456789012" not in json.dumps(value)
    source.read.assert_not_called()
