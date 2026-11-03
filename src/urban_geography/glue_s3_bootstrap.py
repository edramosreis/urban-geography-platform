"""Own the S3 client and temporary input around one writer invocation."""

from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from urban_geography.glue_input import PROJECT_ROOT
from urban_geography.glue_s3_input import download_prepared_input


class _DeferredClient:
    """Let the downloader validate arguments before any SDK client is constructed."""

    def __init__(self, factory: Callable[[], Any]) -> None:
        self.factory = factory
        self.client: Any = None

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.client is None:
            self.client = self.factory()
        return self.client.get_object(**kwargs)

    def close(self) -> None:
        if self.client is not None:
            self.client.close()


def run_s3_writer(
    *,
    deployment_name: str,
    account_id: str,
    expected_summary_sha256: str,
    expected_manifest_sha256: str,
    scratch_root: Path,
    client_factory: Callable[[], Any],
    write: Callable[[Path], dict[str, Any]],
    region: str = "eu-north-1",
) -> dict[str, Any]:
    """Download/verify, close the client, invoke the writer and remove owned scratch."""
    scratch_root = scratch_root.resolve()
    if not scratch_root.is_dir() or any(
        scratch_root.is_relative_to(PROJECT_ROOT / name) for name in ("data", "src", ".git")
    ):
        raise ValueError("Use an existing scratch directory outside retained data/source/Git")
    with TemporaryDirectory(prefix="urban-glue-input-", dir=scratch_root) as temporary:
        workspace = Path(temporary).resolve()
        if not workspace.is_relative_to(scratch_root):
            raise ValueError("Temporary input escaped the selected scratch directory")
        client = _DeferredClient(client_factory)
        prepared = workspace / "prepared"
        try:
            transfer = download_prepared_input(
                client,
                deployment_name=deployment_name,
                account_id=account_id,
                expected_summary_sha256=expected_summary_sha256,
                expected_manifest_sha256=expected_manifest_sha256,
                output=prepared,
                region=region,
            )
        finally:
            client.close()
        publication = write(prepared)
        result = {
            "status": "complete",
            "transfer": transfer,
            "publication": publication,
            "sdk_client_closed_before_writer": True,
        }
    return result | {"scratch_cleaned": True}
