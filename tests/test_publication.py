import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "publication", Path(__file__).resolve().parents[1] / "scripts" / "check_publication.py"
)
assert SPEC is not None and SPEC.loader is not None
PUBLICATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLICATION)


@pytest.mark.parametrize(
    "path",
    [
        "AGENTS.md",
        "docs/STATUS.md",
        "docs/ARCHITECTURE.md",
        "docs/evidence/run.json",
        ".private/backups/history.bundle",
        ".env.production",
        ".aws/credentials",
        "configs/credentials.json",
        "configs/prod.tfvars",
        "configs/client.pem",
        "src/urban_geography/private/note.txt",
        "scripts/secrets.json",
        "tests/key.KEY",
        "data/raw/part.parquet",
        "artifacts/summary.json",
        "personal.txt",
    ],
)
def test_private_paths_cannot_enter_published_history(path):
    assert not PUBLICATION.is_publishable(path)


@pytest.mark.parametrize(
    "path",
    [
        "README.md",
        "Dockerfile",
        ".gitignore",
        "configs/coimbra_pilot.json",
        ".github/workflows/ci.yml",
        "src/urban_geography/cli.py",
        "tests/test_publication.py",
        "scripts/check_publication.py",
    ],
)
def test_runtime_and_verification_sources_can_be_published(path):
    assert PUBLICATION.is_publishable(path)
