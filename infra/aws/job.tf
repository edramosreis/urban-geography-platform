locals {
  job_enabled    = var.glue_job_release_json != null && var.glue_release != null && can(jsondecode(var.glue_job_release_json))
  job_release    = try(jsondecode(var.glue_job_release_json), null)
  job_name       = "${var.deployment_name}-pilot-writer"
  job_arn        = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:job/${local.job_name}"
  job_script_uri = var.glue_release == null ? "" : "s3://${local.bucket_names["lake"]}/runtime/releases/${var.glue_release.release_id}/run_glue_s3_writer.py"
  scratch_prefix = var.glue_release == null ? "" : "scratch/glue/${var.glue_release.release_id}"
  job_release_matches_scope = var.glue_release == null ? false : try(
    (
      local.job_release.contract_version == 1 && local.job_release.status == "frozen_local_release" &&
      local.job_release.release_id == var.glue_release.release_id &&
      local.job_release.identity.summary_sha256 == var.glue_release.summary_sha256 &&
      local.job_release.identity.target.deployment_name == var.deployment_name &&
      local.job_release.identity.target.account_id == var.aws_account_id &&
      local.job_release.identity.target.region == var.aws_region &&
      local.job_release.bucket == local.bucket_names["lake"] &&
      local.job_release.job.glue_version == "5.1" &&
      local.job_release.job.script_location == local.job_script_uri &&
      local.job_release.completion_key == "runtime/releases/${var.glue_release.release_id}/release.json" &&
      toset(concat(values(local.job_release.keys), [local.job_release.completion_key])) == var.glue_release.object_keys &&
      toset(keys(local.job_release.job.arguments)) == toset([
        "--account-id", "--deployment-name", "--region", "--summary-sha256", "--manifest-sha256",
        "--table-name", "--additional-python-modules", "--python-modules-installer-option",
        "--extra-jars", "--user-jars-first", "--conf",
      ]) &&
      local.job_release.job.arguments["--account-id"] == var.aws_account_id &&
      local.job_release.job.arguments["--deployment-name"] == var.deployment_name &&
      local.job_release.job.arguments["--region"] == var.aws_region &&
      local.job_release.job.arguments["--summary-sha256"] == var.glue_release.summary_sha256 &&
      local.job_release.job.arguments["--manifest-sha256"] == local.job_release.identity.input_manifest_sha256 &&
      can(regex("^[0-9a-f]{64}$", local.job_release.identity.input_manifest_sha256)) &&
      local.job_release.job.arguments["--table-name"] == "building_proximity" &&
      local.job_release.job.arguments["--additional-python-modules"] == "s3://${local.bucket_names["lake"]}/runtime/releases/${var.glue_release.release_id}/urban-geography.gluewheels.zip" &&
      local.job_release.job.arguments["--python-modules-installer-option"] == "--no-index" &&
      local.job_release.job.arguments["--user-jars-first"] == "true" &&
      local.job_release.job.arguments["--extra-jars"] == join(",", [for key in local.release_jar_keys : "s3://${local.bucket_names["lake"]}/${key}"])
    ),
    false,
  )
  job_arguments = merge(try(tomap(local.job_release.job.arguments), {}), {
    "--scriptLocation"          = local.job_script_uri
    "--job-language"            = "python"
    "--job-bookmark-option"     = "job-bookmark-disable"
    "--enable-auto-scaling"     = "false"
    "--custom-logGroup-prefix"  = local.glue_log_prefix
    "--custom-logStream-prefix" = "${var.deployment_name}-pilot"
    "--TempDir"                 = "s3://${local.bucket_names["lake"]}/${local.scratch_prefix}/"
  })
}

# This defines a future job; no schedule or trigger is created.
resource "aws_glue_job" "pilot" {
  count = local.job_enabled ? 1 : 0

  name                      = local.job_name
  description               = "Frozen pilot release; execution requires separate trial approval."
  role_arn                  = aws_iam_role.glue_writer[0].arn
  glue_version              = "5.1"
  job_mode                  = "SCRIPT"
  execution_class           = "STANDARD"
  worker_type               = "G.1X"
  number_of_workers         = 2
  timeout                   = 10
  max_retries               = 0
  job_run_queuing_enabled   = false
  default_arguments         = {}
  non_overridable_arguments = local.job_arguments

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = local.job_script_uri
  }
  execution_property {
    max_concurrent_runs = 1
  }
  tags = {
    ReleaseId          = var.glue_release.release_id
    ReleaseManifestSHA = var.glue_release.manifest_sha256
  }

  lifecycle {
    precondition {
      condition     = local.job_release_matches_scope
      error_message = "The frozen job must match its IAM descriptor, target, script, keys and reviewed Glue 5.1 argument contract."
    }
    precondition {
      condition     = length(jsonencode(local.job_arguments)) <= 266240
      error_message = "Glue arguments exceed the 260-KiB job argument limit."
    }
  }
  depends_on = [
    aws_iam_role_policy.glue_writer,
    aws_cloudwatch_log_group.glue,
    aws_glue_catalog_database.analytics,
    aws_s3_bucket_versioning.lake,
    aws_s3_bucket_public_access_block.storage,
    aws_s3_bucket_ownership_controls.storage,
    aws_s3_bucket_server_side_encryption_configuration.storage,
    aws_s3_bucket_policy.tls,
  ]
}

# Unattached deployment/inspection policy; it deliberately grants no StartJobRun.
resource "aws_iam_policy" "glue_job_deployment" {
  count = local.job_enabled ? 1 : 0

  name        = "${var.deployment_name}-job-deployment"
  description = "Configure, inspect or stop one pilot job and pass only its Glue execution role."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ManageOneJob"
        Effect = "Allow"
        Action = [
          "glue:CreateJob", "glue:GetJob", "glue:UpdateJob", "glue:DeleteJob",
          "glue:GetTags", "glue:TagResource", "glue:UntagResource",
          "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun",
        ]
        Resource = local.job_arn
      },
      {
        Sid       = "PassOnlyTheGlueWriterRole"
        Effect    = "Allow"
        Action    = "iam:PassRole"
        Resource  = "arn:aws:iam::${var.aws_account_id}:role/service-role/${var.deployment_name}-glue-writer"
        Condition = { StringEquals = { "iam:PassedToService" = "glue.amazonaws.com" } }
      },
    ]
  })
}
