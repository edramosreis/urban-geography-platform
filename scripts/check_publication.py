"""Reject private files in tracked content/history; optionally run an offline secret scan."""

import argparse
import subprocess
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
GITLEAKS_IMAGE = (
    "ghcr.io/gitleaks/gitleaks@"
    "sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f"
)
ROOT_FILES = {
    ".dockerignore",
    ".gitattributes",
    ".gitignore",
    ".python-version",
    "Dockerfile",
    "README.md",
    "compose.yaml",
    "pyproject.toml",
    "uv.lock",
}
# Deliberate, exact allowance for reviewed infrastructure source; never local state/plans.
INFRA_FILES = {
    "infra/aws/versions.tf",
    "infra/aws/variables.tf",
    "infra/aws/main.tf",
    "infra/aws/outputs.tf",
    "infra/aws/.terraform.lock.hcl",
    "infra/aws/tests/foundation.tftest.hcl",
}
PUBLIC_PREFIXES = ("src/", "tests/", "scripts/", "configs/", ".github/workflows/")
PRIVATE_COMPONENTS = {
    "docs",
    "notes",
    "planning",
    "private",
    "data",
    "artifacts",
    "credentials",
    "secrets",
    "__pycache__",
    ".private",
    ".aws",
    ".ssh",
    ".agents",
    ".codex",
    ".claude",
    ".cursor",
    ".idea",
    ".vscode",
    ".cache",
    ".venv",
    ".terraform",
}
PRIVATE_SUFFIXES = {
    ".md",
    ".markdown",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".jks",
    ".bundle",
    ".tfvars",
    ".tfstate",
    ".tfplan",
    ".kubeconfig",
    ".pyc",
    ".pyo",
}


def is_publishable(path: str) -> bool:
    if path in ROOT_FILES or path in INFRA_FILES:
        return True
    parts = PurePosixPath(path).parts
    if not parts or ".." in parts or path.startswith("/"):
        return False
    if not path.startswith(PUBLIC_PREFIXES):
        return False
    lowered = [part.casefold() for part in parts]
    if any(part in PRIVATE_COMPONENTS or part.startswith(".env") for part in lowered):
        return False
    if any(
        part.startswith(("credentials.", "secrets.", "id_rsa", "id_ed25519"))
        or any(marker in part for marker in (".tfstate.", ".tfvars.", ".tfplan."))
        for part in lowered
    ):
        return False
    return PurePosixPath(path).suffix.casefold() not in PRIVATE_SUFFIXES


def git(*arguments: str) -> str:
    return subprocess.check_output(
        ["git", *arguments], cwd=ROOT, text=True, encoding="utf-8"
    ).strip()


def check(revision: str = "HEAD", history: bool = False, staged: bool = False) -> list[str]:
    if staged:
        return [path for path in git("ls-files").splitlines() if not is_publishable(path)]
    commit = git("rev-parse", "--verify", revision + "^{commit}")
    commits = git("rev-list", commit).splitlines() if history else [commit]
    violations = []
    for current in commits:
        for path in git("ls-tree", "-r", "--name-only", current).splitlines():
            if not is_publishable(path):
                violations.append(f"{current[:12]}:{path}")
    return violations


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--staged", action="store_true")
    parser.add_argument("--secrets", action="store_true")
    args = parser.parse_args()
    violations = check(args.revision, args.history, args.staged)
    if violations:
        raise SystemExit("Publication blocked: private/unapproved paths:\n" + "\n".join(violations))
    if args.secrets:
        if args.staged:
            raise SystemExit("Commit the audited index before running the history secret scan.")
        commit = git("rev-parse", "--verify", args.revision + "^{commit}")
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--mount",
                f"type=bind,source={ROOT},target=/repo,readonly",
                GITLEAKS_IMAGE,
                "git",
                "/repo",
                "--redact",
                "--no-banner",
                f"--log-opts={commit}",
            ],
            check=True,
            cwd=ROOT,
        )
    print("Publication policy passed.")


if __name__ == "__main__":
    main()
