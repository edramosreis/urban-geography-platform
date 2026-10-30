"""Check real pinned SDK delivery requests offline with Stubber and dummy credentials."""

import argparse
import base64
import io
import json
from pathlib import Path

from urban_geography.glue_delivery import delivery_plan, upload_release
from urban_geography.glue_input import PROJECT_ROOT
from urban_geography.glue_sdk import create_input_client


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if (
        output.exists()
        or output.is_relative_to(args.release.resolve())
        or any(output.is_relative_to(PROJECT_ROOT / name) for name in ("data", "src", ".git"))
    ):
        raise ValueError("SDK delivery check needs fresh output outside release/data/source/Git")
    plan = delivery_plan(args.release, args.release_sha256)
    import boto3
    from botocore.exceptions import ClientError
    from botocore.response import StreamingBody
    from botocore.stub import Stubber

    session = boto3.Session(
        aws_access_key_id="testing", aws_secret_access_key="testing", region_name="eu-west-1"
    )
    client = create_input_client(session=session)
    bodies = []
    results = []
    try:
        for reuse in (False, True):
            with Stubber(client) as stub:
                for item in plan["objects"]:
                    content = (args.release / item["path"]).read_bytes()
                    expected = {
                        "Bucket": plan["bucket"],
                        "Key": item["key"],
                        "ExpectedBucketOwner": plan["expected_bucket_owner"],
                        "Body": content,
                        "ContentLength": len(content),
                        "ChecksumSHA256": base64.b64encode(bytes.fromhex(item["sha256"])).decode(),
                        "ServerSideEncryption": "AES256",
                        "IfNoneMatch": "*",
                    }
                    if reuse:
                        stub.add_client_error(
                            "put_object",
                            service_error_code="PreconditionFailed",
                            http_status_code=412,
                            expected_params=expected,
                        )
                    else:
                        stub.add_response(
                            "put_object", {"VersionId": "stubbed-version-1"}, expected
                        )
                    body = io.BytesIO(content)
                    bodies.append(body)
                    stub.add_response(
                        "get_object",
                        {
                            "Body": StreamingBody(body, len(content)),
                            "ContentLength": len(content),
                            "VersionId": "stubbed-version-1",
                        },
                        {
                            "Bucket": plan["bucket"],
                            "Key": item["key"],
                            "ExpectedBucketOwner": plan["expected_bucket_owner"],
                            "Range": f"bytes=0-{item['bytes']}",
                        },
                    )
                result = upload_release(client, args.release, args.release_sha256)
                assert result["reused" if reuse else "created"] == len(plan["objects"])
                stub.assert_no_pending_responses()
                results.append(result)
            assert all(body.closed for body in bodies)
        item = plan["objects"][0]
        with Stubber(client) as stub:
            stub.add_client_error(
                "put_object", service_error_code="AccessDenied", http_status_code=403
            )
            try:
                upload_release(client, args.release, args.release_sha256)
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "AccessDenied"
            else:
                raise AssertionError("SDK access denial must propagate")
            stub.assert_no_pending_responses()
    finally:
        client.close()
    output.mkdir(parents=True)
    receipt = {
        "status": "complete",
        "transport": "botocore.Stubber",
        "aws_api_or_upload": False,
        "real_sdk_request_shapes_verified": True,
        "responses_closed": True,
        "access_denied_propagates": True,
        "first_delivery": results[0],
        "replay": results[1],
    }
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "complete",
                "objects_per_attempt": len(plan["objects"]),
                "created": results[0]["created"],
                "reused": results[1]["reused"],
                "aws_api_or_upload": False,
            }
        )
    )


if __name__ == "__main__":
    main()
