# Optional data cleanup/quiescence identity; infrastructure destroy stays separate.
locals {
  cleanup_lake_objects = concat(local.release_object_arns, [
    local.table_objects_arn,
    "arn:aws:s3:::${local.bucket_names["lake"]}/${local.scratch_prefix}/*",
  ])
  trial_cleanup_policy = {
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "InventoryDedicatedBuckets"
        Effect    = "Allow"
        Action    = ["s3:GetBucketLocation", "s3:GetBucketVersioning", "s3:ListBucket", "s3:ListBucketVersions", "s3:ListBucketMultipartUploads"]
        Resource  = [for bucket in values(local.bucket_names) : "arn:aws:s3:::${bucket}"]
        Condition = local.same_bucket_owner
      },
      {
        Sid       = "RemoveKnownLakeVersionsAndUploads"
        Effect    = "Allow"
        Action    = ["s3:DeleteObjectVersion", "s3:AbortMultipartUpload"]
        Resource  = local.cleanup_lake_objects
        Condition = local.same_bucket_owner
      },
      {
        Sid       = "RemoveQueryResultsAndUploads"
        Effect    = "Allow"
        Action    = ["s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource  = "arn:aws:s3:::${local.bucket_names["results"]}/queries/*"
        Condition = local.same_bucket_owner
      },
      {
        Sid      = "NeverCreateLakeDeleteMarkers"
        Effect   = "Deny"
        Action   = "s3:DeleteObject"
        Resource = "arn:aws:s3:::${local.bucket_names["lake"]}/*"
      },
      {
        Sid      = "NeverWriteReplacementObjects"
        Effect   = "Deny"
        Action   = "s3:PutObject"
        Resource = [for bucket in values(local.bucket_names) : "arn:aws:s3:::${bucket}/*"]
      },
      {
        Sid      = "InspectDedicatedDatabase"
        Effect   = "Allow"
        Action   = "glue:GetDatabase"
        Resource = [local.catalog_arn, local.database_arn]
      },
      {
        Sid      = "DiscoverUnexpectedRelations"
        Effect   = "Allow"
        Action   = "glue:GetTables"
        Resource = [local.catalog_arn, local.database_arn, "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${local.database_name}/*"]
      },
      {
        Sid      = "InspectAndRemoveThreeTrialRelations"
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:DeleteTable"]
        Resource = concat([local.catalog_arn, local.database_arn, local.table_arn], local.gold_view_arns)
      },
      {
        Sid      = "QuiesceOneWorkgroup"
        Effect   = "Allow"
        Action   = ["athena:GetWorkGroup", "athena:UpdateWorkGroup", "athena:ListQueryExecutions", "athena:GetQueryExecution", "athena:StopQueryExecution"]
        Resource = "arn:aws:athena:${var.aws_region}:${var.aws_account_id}:workgroup/${var.deployment_name}-m4"
      },
      {
        Sid      = "InspectAndStopOneWriterJob"
        Effect   = "Allow"
        Action   = ["glue:GetJob", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = local.job_arn
      },
    ]
  }
}

resource "aws_iam_role" "trial_cleanup" {
  count = var.trial_cleanup_principal_arn == null ? 0 : 1

  name                 = "${var.deployment_name}-trial-cleanup"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = var.trial_cleanup_principal_arn }
    }]
  })
  lifecycle {
    precondition {
      condition     = local.job_enabled
      error_message = "Cleanup requires the pinned release and configured trial writer job."
    }
  }
}

resource "aws_iam_role_policy" "trial_cleanup" {
  count = var.trial_cleanup_principal_arn != null && local.job_enabled ? 1 : 0

  name   = "trial-data-cleanup"
  role   = aws_iam_role.trial_cleanup[0].id
  policy = jsonencode(local.trial_cleanup_policy)
  lifecycle {
    precondition {
      condition     = length(jsonencode(local.trial_cleanup_policy)) <= 10240
      error_message = "The exact cleanup policy exceeds the inline IAM size ceiling."
    }
  }
}
