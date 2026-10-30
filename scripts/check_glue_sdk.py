"""Check the pinned real SDK offline with Stubber, dummy credentials and retained input."""

import argparse
import io
import json
import os
from importlib.metadata import version
from pathlib import Path

from urban_geography.glue_input import MAX_SUMMARY_BYTES, PROJECT_ROOT, load_prepared_input
from urban_geography.glue_s3_input import download_prepared_input
from urban_geography.glue_sdk import SDK_PINS, create_input_client
from urban_geography.ingest import digest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--summary-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(PROJECT_ROOT / "data"):
        raise ValueError("SDK check needs a fresh output outside retained data")
    prepared = load_prepared_input(args.input, args.summary_sha256)
    import boto3
    from botocore.exceptions import ClientError
    from botocore.response import StreamingBody
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="testing", aws_secret_access_key="testing", region_name="eu-west-1"
    )
    old_endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")
    os.environ["AWS_ENDPOINT_URL_S3"] = "https://example.invalid"
    try:
        client = create_input_client(session=session)
    finally:
        if old_endpoint is None:
            os.environ.pop("AWS_ENDPOINT_URL_S3", None)
        else:
            os.environ["AWS_ENDPOINT_URL_S3"] = old_endpoint
    streams = []
    try:
        assert client.meta.endpoint_url == "https://s3.eu-west-1.amazonaws.com"
        config = client.meta.config
        assert config.connect_timeout == 5 and config.read_timeout == 10
        assert config.retries["total_max_attempts"] == 1
        assert config.signature_version == "s3v4" and config.max_pool_connections == 1
        # The endpoint option is consumed during construction; verify its resolved effect above.
        assert config.s3["addressing_style"] == "virtual"
        prefix = "inputs/" + args.summary_sha256 + "/"
        bucket = "urban-geo-test-lake-123456789012-eu-west-1"
        manifest = args.input / "input-manifest.json"
        files = [("input-manifest.json", MAX_SUMMARY_BYTES)] + [
            (name, item["bytes"]) for name, item in sorted(prepared.files.items())
        ]
        with Stubber(client) as stub:
            for name, cap in files:
                data = (args.input / name).read_bytes()
                raw = io.BytesIO(data)
                streams.append(raw)
                stub.add_response(
                    "get_object",
                    {
                        "Body": StreamingBody(raw, len(data)),
                        "ContentLength": len(data),
                        "VersionId": "stubbed-version-1",
                    },
                    {
                        "Bucket": bucket,
                        "Key": prefix + name,
                        "ExpectedBucketOwner": "123456789012",
                        "Range": f"bytes=0-{cap}",
                    },
                )
            transfer = download_prepared_input(
                client,
                deployment_name="urban-geo-test",
                account_id="123456789012",
                expected_summary_sha256=args.summary_sha256,
                expected_manifest_sha256=digest(manifest),
                output=output / "restored",
            )
            stub.assert_no_pending_responses()
        assert all(stream.closed for stream in streams)
        with Stubber(client) as stub:
            stub.add_client_error(
                "get_object",
                service_error_code="AccessDenied",
                http_status_code=403,
                expected_params={
                    "Bucket": bucket,
                    "Key": prefix + "input-manifest.json",
                    "ExpectedBucketOwner": "123456789012",
                    "Range": f"bytes=0-{MAX_SUMMARY_BYTES}",
                },
            )
            try:
                download_prepared_input(
                    client,
                    deployment_name="urban-geo-test",
                    account_id="123456789012",
                    expected_summary_sha256=args.summary_sha256,
                    expected_manifest_sha256=digest(manifest),
                    output=output / "denied",
                )
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "AccessDenied"
            else:
                raise AssertionError("SDK errors must propagate")
            stub.assert_no_pending_responses()
        assert not (output / "denied").exists()
    finally:
        client.close()
    result = {
        "status": "complete",
        "sdk_versions": {name: version(name) for name in SDK_PINS},
        "sdk_configuration_verified": True,
        "configured_endpoint_override_ignored": True,
        "real_sdk_request_and_response_shapes_verified": True,
        "responses_closed": True,
        "access_denied_propagates": True,
        "aws_api_or_upload": False,
        "transport": "botocore.Stubber",
        "transfer": transfer,
    }
    (output / "receipt.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "complete",
                "sdk_versions": result["sdk_versions"],
                "get_responses": transfer["get_requests"],
                "aws_api_or_upload": False,
            }
        )
    )


if __name__ == "__main__":
    main()
