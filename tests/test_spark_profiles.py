"""Bounded Spark profile selection and executor-registration gates without Java."""

from types import SimpleNamespace

import pytest

import urban_geography.spark as runtime


def test_local_default_and_bounded_two_executor_settings():
    assert runtime.execution_settings("local") == {"spark.master": "local[2]"}
    cluster = runtime.execution_settings("two_executor")
    assert cluster["spark.master"] == "spark://master:7077"
    assert cluster["spark.executor.cores"] == "1" and cluster["spark.cores.max"] == "2"
    assert cluster["spark.executor.memory"] == "768m"
    assert cluster["spark.dynamicAllocation.enabled"] == "false"
    assert cluster["spark.driver.host"] == "driver"


@pytest.mark.parametrize("profile", ["", "spark://unapproved:7077", "local[20]"])
def test_unknown_profile_rejected_before_files_or_jvm(tmp_path, profile):
    output = tmp_path / "events"
    with pytest.raises(ValueError, match="Unknown Spark"):
        runtime.create_session(output, execution_profile=profile)
    assert not output.exists()


def test_executor_registration_wait_is_bounded_and_requires_two(monkeypatch):
    ids = iter([[], ["0"], ["0", "1"]])
    monkeypatch.setattr(runtime, "executor_ids", lambda spark: next(ids))
    monkeypatch.setattr(runtime.time, "sleep", lambda seconds: None)
    assert runtime.wait_for_executors(None, 1) == ["0", "1"]
    monkeypatch.setattr(runtime, "executor_ids", lambda spark: ["0"])
    with pytest.raises(RuntimeError, match="exactly two"):
        runtime.wait_for_executors(None, 0)
    monkeypatch.setattr(runtime, "executor_ids", lambda spark: ["0", "1", "2"])
    with pytest.raises(RuntimeError, match="exactly two"):
        runtime.wait_for_executors(None, 1)


def test_executor_ids_use_the_jvm_executor_registry():
    values = SimpleNamespace(size=lambda: 2, apply=lambda index: ["1", "0"][index])
    context = SimpleNamespace(
        _jsc=SimpleNamespace(sc=lambda: SimpleNamespace(getExecutorIds=lambda: values))
    )
    assert runtime.executor_ids(SimpleNamespace(sparkContext=context)) == ["0", "1"]


def test_zero_locality_wait_is_an_explicit_diagnostic_not_the_default():
    standard = runtime.execution_settings("two_executor")
    diagnostic = runtime.execution_settings("two_executor_no_locality_wait")
    assert "spark.locality.wait" not in standard
    assert diagnostic == {**standard, "spark.locality.wait": "0s"}
    assert runtime.execution_settings("local") == {"spark.master": "local[2]"}
