# All provider data is synthetic; every scenario is a plan without AWS traffic.
mock_provider "aws" {
  override_during = plan
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/service-role/urban-geo-test-glue-writer" }
  }
}

variables {
  aws_region            = "eu-north-1"
  aws_account_id        = "123456789012"
  deployment_name       = "urban-geo-test"
  glue_job_release_json = file("../../tests/fixtures/glue_job_release.json")
  glue_release = {
    release_id      = jsondecode(file("../../tests/fixtures/glue_job_release.json")).release_id
    manifest_sha256 = filesha256("../../tests/fixtures/glue_job_release.json")
    summary_sha256  = jsondecode(file("../../tests/fixtures/glue_job_release.json")).identity.summary_sha256
    object_keys     = concat(values(jsondecode(file("../../tests/fixtures/glue_job_release.json")).keys), [jsondecode(file("../../tests/fixtures/glue_job_release.json")).completion_key])
  }
}

run "cleanup_is_omitted_by_default" {
  command = plan
  assert {
    condition     = (length(aws_iam_role.trial_cleanup) == 0 && length(aws_iam_role_policy.trial_cleanup) == 0 && output.trial_cleanup_role_arn == null)
    error_message = "Existing defaults must not create deletion authority."
  }
}

run "cleanup_exact_trust_and_separate_inline_policy" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:iam::123456789012:role/managed/project/login" }
  override_resource {
    override_during = plan
    target          = aws_iam_role.trial_cleanup[0]
    values          = { arn = "arn:aws:iam::123456789012:role/urban-geo-test-trial-cleanup" }
  }
  assert {
    condition = (
      aws_iam_role.trial_cleanup[0].name == "urban-geo-test-trial-cleanup" &&
      aws_iam_role.trial_cleanup[0].max_session_duration == 3600 &&
      jsondecode(aws_iam_role.trial_cleanup[0].assume_role_policy) == {
        Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { AWS = var.trial_cleanup_principal_arn } }]
      } && aws_iam_role_policy.trial_cleanup[0].role == aws_iam_role.trial_cleanup[0].id &&
      length(aws_iam_role.trial_cleanup) == 1 && length(aws_iam_role_policy.trial_cleanup) == 1 &&
      output.trial_cleanup_role_arn == "arn:aws:iam::123456789012:role/urban-geo-test-trial-cleanup" &&
      length(aws_iam_role.athena_reader) == 0 && length(aws_iam_role.dbt_view_builder) == 0 &&
      aws_athena_workgroup.analytics.state == "DISABLED" &&
      alltrue([for b in values(aws_s3_bucket.storage) : !b.force_destroy]) &&
      length(jsonencode(local.trial_cleanup_policy)) <= 10240
    )
    error_message = "Cleanup must have its own exact trust/policy; defaults remain disabled and buckets cannot force-empty."
  }
}

run "cleanup_versions_and_uploads_are_exact" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:iam::123456789012:role/login" }
  assert {
    condition = (
      toset(local.trial_cleanup_policy.Statement[1].Action) == toset(["s3:DeleteObjectVersion", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]) &&
      toset(local.trial_cleanup_policy.Statement[1].Resource) == toset(concat(local.release_object_arns, [local.table_objects_arn, "arn:aws:s3:::${local.bucket_names["lake"]}/${local.scratch_prefix}/*"])) &&
      local.trial_cleanup_policy.Statement[1].Condition.StringEquals["s3:ResourceAccount"] == var.aws_account_id &&
      local.trial_cleanup_policy.Statement[3].Effect == "Deny" && local.trial_cleanup_policy.Statement[3].Action == "s3:DeleteObject" &&
      local.trial_cleanup_policy.Statement[3].Resource == "arn:aws:s3:::${local.bucket_names["lake"]}/*" &&
      toset(local.trial_cleanup_policy.Statement[2].Action) == toset(["s3:DeleteObject", "s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]) &&
      local.trial_cleanup_policy.Statement[2].Resource == "arn:aws:s3:::${local.bucket_names["results"]}/queries/*"
    )
    error_message = "Delete enumerated lake versions rather than adding markers; only the bound release/table/scratch/results can be removed."
  }
}

run "cleanup_inventory_detects_unexpected_bucket_keys" {
  command = plan
  assert {
    condition = (
      toset(local.trial_cleanup_policy.Statement[0].Action) == toset(["s3:GetBucketLocation", "s3:GetBucketVersioning", "s3:ListBucket", "s3:ListBucketVersions", "s3:ListBucketMultipartUploads"]) &&
      toset(local.trial_cleanup_policy.Statement[0].Resource) == toset([for b in values(local.bucket_names) : "arn:aws:s3:::${b}"]) &&
      local.trial_cleanup_policy.Statement[0].Condition.StringEquals["s3:ResourceAccount"] == var.aws_account_id &&
      local.trial_cleanup_policy.Statement[4].Effect == "Deny" && local.trial_cleanup_policy.Statement[4].Action == "s3:PutObject"
    )
    error_message = "Read-only whole-bucket listing must find unknown keys while writes stay denied."
  }
}

run "cleanup_only_three_catalog_deletions" {
  command = plan
  assert {
    condition = (
      local.trial_cleanup_policy.Statement[6].Action == "glue:GetTables" &&
      toset(local.trial_cleanup_policy.Statement[6].Resource) == toset([local.catalog_arn, local.database_arn, "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${local.database_name}/*"]) &&
      toset(local.trial_cleanup_policy.Statement[7].Action) == toset(["glue:GetTable", "glue:DeleteTable"]) &&
      toset(local.trial_cleanup_policy.Statement[7].Resource) == toset(concat([local.catalog_arn, local.database_arn, local.table_arn], local.gold_view_arns)) &&
      !contains(flatten([for s in local.trial_cleanup_policy.Statement : try(tolist(s.Action), [s.Action])]), "glue:DeleteDatabase")
    )
    error_message = "Catalog discovery may reveal unexpected relations, but deletion names exactly the fact and two Gold views."
  }
}

run "cleanup_can_quiesce_but_never_start_work_or_destroy_infrastructure" {
  command = plan
  assert {
    condition = (
      toset(local.trial_cleanup_policy.Statement[8].Action) == toset(["athena:GetWorkGroup", "athena:UpdateWorkGroup", "athena:ListQueryExecutions", "athena:GetQueryExecution", "athena:StopQueryExecution"]) &&
      local.trial_cleanup_policy.Statement[8].Resource == "arn:aws:athena:eu-north-1:123456789012:workgroup/urban-geo-test-m4" &&
      toset(local.trial_cleanup_policy.Statement[9].Action) == toset(["glue:GetJob", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]) &&
      local.trial_cleanup_policy.Statement[9].Resource == local.job_arn &&
      alltrue([for a in flatten([for s in local.trial_cleanup_policy.Statement : try(tolist(s.Action), [s.Action])]) :
        !contains(["athena:StartQueryExecution", "glue:StartJobRun", "s3:DeleteBucket", "glue:DeleteJob", "athena:DeleteWorkGroup", "iam:PassRole"], a) && !startswith(a, "iam:")
      ])
    )
    error_message = "Cleanup grants no work launch, IAM or infrastructure deletion authority. UpdateWorkGroup remains scoped to one workgroup."
  }
}

run "cleanup_requires_the_configured_job" {
  command = plan
  variables {
    trial_cleanup_principal_arn = "arn:aws:iam::123456789012:role/login"
    glue_job_release_json       = null
  }
  expect_failures = [aws_iam_role.trial_cleanup[0]]
}

run "cleanup_rejects_cross_account_trust" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:iam::999999999999:role/login" }
  expect_failures = [var.trial_cleanup_principal_arn]
}

run "cleanup_rejects_user_trust" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:iam::123456789012:user/login" }
  expect_failures = [var.trial_cleanup_principal_arn]
}

run "cleanup_rejects_root_trust" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:iam::123456789012:root" }
  expect_failures = [var.trial_cleanup_principal_arn]
}

run "cleanup_rejects_session_trust" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:sts::123456789012:assumed-role/login/session" }
  expect_failures = [var.trial_cleanup_principal_arn]
}

run "cleanup_rejects_wildcard_trust" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:iam::123456789012:role/*" }
  expect_failures = [var.trial_cleanup_principal_arn]
}

run "cleanup_rejects_self_trust" {
  command = plan
  variables { trial_cleanup_principal_arn = "arn:aws:iam::123456789012:role/urban-geo-test-trial-cleanup" }
  expect_failures = [var.trial_cleanup_principal_arn]
}
