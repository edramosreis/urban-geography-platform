# Provider mocking and plan-only runs: no AWS calls or resource creation.
mock_provider "aws" {}

variables {
  aws_region      = "eu-north-1"
  aws_account_id  = "123456789012"
  deployment_name = "urban-geo-test"
  glue_release = {
    release_id      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    manifest_sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    summary_sha256  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    object_keys     = ["runtime/jars/iceberg-spark-runtime-3.5_2.12-1.10.2.jar", "runtime/jars/sedona-spark-shaded-3.5_2.12-1.8.0.jar", "runtime/jars/geotools-wrapper-1.8.0-33.1.jar", "runtime/jars/iceberg-aws-bundle-1.10.2.jar", "runtime/releases/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/run_glue_s3_writer.py", "runtime/releases/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/urban-geography.gluewheels.zip", "runtime/releases/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/release.json", "inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/summary.json", "inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/input-manifest.json", "inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/building_proximity/part-0.parquet", "inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/grid_proximity/part-0.parquet"]
  }
}

run "delivery_is_exact_and_create_only" {
  command = plan

  assert {
    condition = (
      toset(jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[0].Action) == toset(["s3:GetObject", "s3:PutObject"]) &&
      toset(jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[0].Resource) == toset([
        for key in var.glue_release.object_keys : "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-north-1/${key}"
      ]) &&
      jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[0].Condition.StringEquals["s3:ResourceAccount"] == var.aws_account_id
    )
    error_message = "Delivery must grant only PUT/GET on every exact release object in the intended account."
  }
  assert {
    condition = (
      jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[1].Effect == "Deny" &&
      jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[1].Action == "s3:PutObject" &&
      jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[1].Condition.StringNotEquals["s3:if-none-match"] == "*" &&
      jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[2].Effect == "Deny" &&
      jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[2].Condition.StringNotEquals["s3:x-amz-server-side-encryption"] == "AES256" &&
      toset(jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[3].Action) == toset(["s3:DeleteObject", "s3:DeleteObjectVersion"]) &&
      jsondecode(aws_iam_policy.release_delivery[0].policy).Statement[3].Effect == "Deny" &&
      alltrue([for statement in slice(jsondecode(aws_iam_policy.release_delivery[0].policy).Statement, 1, 4) :
        toset(statement.Resource) == toset([
          "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-north-1/runtime/*",
          "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-north-1/inputs/*"
        ])
      ]) && length(aws_iam_policy.release_delivery[0].policy) <= 6144
    )
    error_message = "Delivered software/input must require create-only encrypted PUTs and preserve objects/versions."
  }
}

run "writer_reads_release_and_writes_one_table" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[0].Action == "s3:GetObject" &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[0].Resource) == toset([
        for key in var.glue_release.object_keys : "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-north-1/${key}"
      ]) &&
      jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[2].Action == "s3:ListBucket" &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[2].Condition.StringLike["s3:prefix"]) == toset([
        "warehouse/building_proximity", "warehouse/building_proximity/*"
      ]) &&
      jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[3].Resource == "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-north-1/warehouse/building_proximity/*" &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[3].Action) == toset([
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"
      ])
    )
    error_message = "Execution must read exact inputs and confine table writes, cleanup and listing to building_proximity."
  }
  assert {
    condition = (
      jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[4].Action == "glue:GetDatabase" &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[5].Action) == toset([
        "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[5].Resource) == toset([
        "arn:aws:glue:eu-north-1:123456789012:catalog",
        "arn:aws:glue:eu-north-1:123456789012:database/urban_geo_test_m4",
        "arn:aws:glue:eu-north-1:123456789012:table/urban_geo_test_m4/building_proximity"
      ]) &&
      alltrue([for statement in jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement : statement.Effect == "Allow"]) &&
      length(aws_iam_role_policy.glue_writer[0].policy) <= 10240
    )
    error_message = "Catalog permissions must cover only the existing database and the pilot table commits."
  }
}

run "service_trust_and_dedicated_logs" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_iam_role.glue_writer[0].assume_role_policy).Statement == [{
        Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "glue.amazonaws.com" }
      }] && aws_iam_role.glue_writer[0].max_session_duration == 3600 &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[6].Action) == toset(["logs:CreateLogStream", "logs:PutLogEvents"]) &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[6].Resource) == toset([
        "arn:aws:logs:eu-north-1:123456789012:log-group:/urban-geography/urban-geo-test/glue/error:log-stream:*",
        "arn:aws:logs:eu-north-1:123456789012:log-group:/urban-geography/urban-geo-test/glue/output:log-stream:*"
      ]) &&
      alltrue([for kind, group in aws_cloudwatch_log_group.glue :
        group.retention_in_days == 7 && group.name == "/urban-geography/urban-geo-test/glue/${kind}"
      ]) && output.glue_log_group_prefix == "/urban-geography/urban-geo-test/glue"
    )
    error_message = "Only Glue may assume the role; logs must use precreated dedicated groups with finite retention."
  }
}

run "omitted_release_creates_no_permissions" {
  command = plan
  variables { glue_release = null }
  assert {
    condition = (
      length(aws_iam_policy.release_delivery) == 0 && length(aws_iam_role.glue_writer) == 0 &&
      length(aws_iam_role_policy.glue_writer) == 0 && length(aws_cloudwatch_log_group.glue) == 0 &&
      output.glue_writer_role_arn == null && output.release_delivery_policy_arn == null &&
      output.glue_log_group_prefix == null
    )
    error_message = "The existing foundation defaults must not gain IAM roles or logging resources."
  }
}

run "reject_bad_identity" {
  command = plan
  variables { glue_release = merge(var.glue_release, { release_id = "INVALID" }) }
  expect_failures = [var.glue_release]
}

run "reject_missing_control" {
  command = plan
  variables {
    glue_release = merge(var.glue_release, {
      object_keys = [for key in var.glue_release.object_keys : key if !endswith(key, "/release.json")]
    })
  }
  expect_failures = [var.glue_release]
}

run "reject_warehouse_delivery" {
  command = plan
  variables {
    glue_release = merge(var.glue_release, { object_keys = setunion(var.glue_release.object_keys, ["warehouse/building_proximity/unexpected.parquet"]) })
  }
  expect_failures = [var.glue_release]
}

run "reject_wildcard_key" {
  command = plan
  variables {
    glue_release = merge(var.glue_release, { object_keys = setunion(var.glue_release.object_keys, ["inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/building_proximity/*.parquet"]) })
  }
  expect_failures = [var.glue_release]
}

run "reject_another_release" {
  command = plan
  variables {
    glue_release = merge(var.glue_release, { object_keys = setunion(var.glue_release.object_keys, ["runtime/releases/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc/run_glue_s3_writer.py"]) })
  }
  expect_failures = [var.glue_release]
}

run "reject_traversal" {
  command = plan
  variables {
    glue_release = merge(var.glue_release, { object_keys = setunion(var.glue_release.object_keys, ["inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/../private.parquet"]) })
  }
  expect_failures = [var.glue_release]
}

run "reject_excessive_inventory" {
  command = plan
  variables {
    glue_release = merge(var.glue_release, { object_keys = setunion(var.glue_release.object_keys, [for index in range(34) : "inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/building_proximity/part-${index}.parquet"]) })
  }
  expect_failures = [var.glue_release]
}

run "reject_oversized_iam_documents" {
  command = plan
  variables {
    glue_release = merge(var.glue_release, { object_keys = setunion(var.glue_release.object_keys, [for index in range(22) : "inputs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/building_proximity/part-${index}-${join("", [for letter in range(370) : "x"])}.parquet"]) })
  }
  expect_failures = [aws_iam_policy.release_delivery[0], aws_iam_role_policy.glue_writer[0]]
}
