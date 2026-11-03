"""Small synthetic release factory; JAR and bundle verification are isolated in tests."""

import hashlib
from types import SimpleNamespace

import urban_geography.glue_release as release
from urban_geography.glue_input import stage_input


def make_frozen(history_run_factory, tmp_path, monkeypatch):
    run = history_run_factory()
    prepared = tmp_path / "prepared"
    stage_input(run.path, run.summary_sha256, prepared)
    root = tmp_path / "project"
    (root / "scripts").mkdir(parents=True)
    (root / "scripts/run_glue_s3_writer.py").write_bytes(b"print('writer')")
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "urban-geography.gluewheels.zip").write_bytes(b"verified bundle fixture")
    jars = []
    pins = []
    for index in range(4):
        jar = tmp_path / f"runtime-{index}.jar"
        jar.write_bytes(f"jar-{index}".encode())
        jars.append(jar)
        pins.append(("fixture/" + jar.name, hashlib.sha256(jar.read_bytes()).hexdigest()))
    monkeypatch.setattr(release, "JARS", pins)
    monkeypatch.setattr(release, "package_inputs", lambda root: {"source": "unchanged"})
    monkeypatch.setattr(release, "verify_bundle", lambda root, bundle: None)
    options = dict(
        root=root,
        bundle=bundle,
        prepared_input=prepared,
        expected_summary_sha256=run.summary_sha256,
        jars=jars,
        target={
            "deployment_name": "urban-geo-test",
            "account_id": "123456789012",
            "region": "eu-north-1",
        },
        output=tmp_path / "release",
    )
    result = release.freeze_release(**options)
    return SimpleNamespace(
        path=options["output"],
        pin=result["manifest_sha256"],
        options=options,
        manifest=release.load_release(options["output"], result["manifest_sha256"]),
    )
