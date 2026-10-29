# Every run uses a mocked provider and plan command: no AWS APIs or resources.
mock_provider "aws" {}

variables {
  aws_region      = "eu-west-1"
  aws_account_id  = "123456789012"
  deployment_name = "urban-geo-test"
}

run "private_storage" {
  command = plan

  assert {
    condition = alltrue([
      for bucket in aws_s3_bucket.storage : !bucket.force_destroy
    ])
    error_message = "Teardown must not recursively empty either bucket."
  }
  assert {
    condition = alltrue([
      for block in aws_s3_bucket_public_access_block.storage :
      block.block_public_acls && block.block_public_policy &&
      block.ignore_public_acls && block.restrict_public_buckets
    ])
    error_message = "Both buckets must block all public access."
  }
  assert {
    condition = alltrue([
      for owner in aws_s3_bucket_ownership_controls.storage :
      one(owner.rule).object_ownership == "BucketOwnerEnforced"
    ])
    error_message = "Object ACLs must be disabled."
  }
  assert {
    condition = alltrue([
      for encryption in aws_s3_bucket_server_side_encryption_configuration.storage :
      one(one(encryption.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256"
    ])
    error_message = "Both buckets must request SSE-S3 encryption."
  }
  assert {
    condition = alltrue([
      for policy in aws_s3_bucket_policy.tls :
      jsondecode(policy.policy).Statement[0].Effect == "Deny" &&
      jsondecode(policy.policy).Statement[0].Condition.Bool["aws:SecureTransport"] == "false" &&
      jsondecode(policy.policy).Statement[0].Principal == "*" &&
      jsondecode(policy.policy).Statement[0].Action == "s3:*"
    ])
    error_message = "Both buckets must deny requests without TLS."
  }
  assert {
    condition = (
      one(aws_s3_bucket_versioning.lake.versioning_configuration).status == "Enabled"
    )
    error_message = "Lake object versions must be retained."
  }
}

run "bounded_disabled_workgroup" {
  command = plan

  assert {
    condition = (
      aws_athena_workgroup.analytics.state == "DISABLED" &&
      !aws_athena_workgroup.analytics.force_destroy &&
      one(aws_athena_workgroup.analytics.configuration).enforce_workgroup_configuration &&
      one(aws_athena_workgroup.analytics.configuration).bytes_scanned_cutoff_per_query == 104857600 &&
      one(one(aws_athena_workgroup.analytics.configuration).engine_version).selected_engine_version == "Athena engine version 3"
    )
    error_message = "The workgroup must start disabled with enforced engine/scan settings."
  }
  assert {
    condition = (
      one(one(aws_athena_workgroup.analytics.configuration).result_configuration).expected_bucket_owner == var.aws_account_id &&
      one(one(one(aws_athena_workgroup.analytics.configuration).result_configuration).encryption_configuration).encryption_option == "SSE_S3" &&
      output.query_results_uri == "s3://urban-geo-test-results-123456789012-eu-west-1/queries/" &&
      output.warehouse_uri == "s3://urban-geo-test-lake-123456789012-eu-west-1/warehouse/" &&
      aws_glue_catalog_database.analytics.catalog_id == var.aws_account_id
    )
    error_message = "Query results and table data must use separate, account-scoped locations."
  }
}

run "results_only_expiration" {
  command = plan

  assert {
    condition = (
      one(aws_s3_bucket_lifecycle_configuration.query_results.rule).status == "Enabled" &&
      one(one(aws_s3_bucket_lifecycle_configuration.query_results.rule).filter).prefix == "queries/" &&
      one(one(aws_s3_bucket_lifecycle_configuration.query_results.rule).expiration).days == 7 &&
      one(one(aws_s3_bucket_lifecycle_configuration.query_results.rule).abort_incomplete_multipart_upload).days_after_initiation == 1
    )
    error_message = "Only disposable query results should expire automatically."
  }
}

run "reject_wrong_region" {
  command = plan
  variables { aws_region = "us-east-1" }
  expect_failures = [var.aws_region]
}

run "reject_invalid_account" {
  command = plan
  variables { aws_account_id = "invalid" }
  expect_failures = [var.aws_account_id]
}

run "reject_invalid_name" {
  command = plan
  variables { deployment_name = "Invalid_Name" }
  expect_failures = [var.deployment_name]
}

run "reject_scan_below_service_minimum" {
  command = plan
  variables { max_query_scan_bytes = 10485759 }
  expect_failures = [var.max_query_scan_bytes]
}

run "reject_scan_above_demonstration_ceiling" {
  command = plan
  variables { max_query_scan_bytes = 104857601 }
  expect_failures = [var.max_query_scan_bytes]
}

run "reject_fractional_scan_bytes" {
  command = plan
  variables { max_query_scan_bytes = 10485760.5 }
  expect_failures = [var.max_query_scan_bytes]
}
