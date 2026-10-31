locals {
  athena_reader_policy = {
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "UsePilotWorkgroup"
        Effect   = "Allow"
        Action   = ["athena:GetWorkGroup", "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"]
        Resource = "arn:aws:athena:${var.aws_region}:${var.aws_account_id}:workgroup/${var.deployment_name}-m4"
      },
      {
        Sid      = "ReadPilotDatabase"
        Effect   = "Allow"
        Action   = "glue:GetDatabase"
        Resource = [local.catalog_arn, local.database_arn]
      },
      {
        Sid      = "ReadPilotTable"
        Effect   = "Allow"
        Action   = "glue:GetTable"
        Resource = [local.catalog_arn, local.database_arn, local.table_arn]
      },
      {
        Sid       = "LocatePilotBuckets"
        Effect    = "Allow"
        Action    = "s3:GetBucketLocation"
        Resource  = [for name in values(local.bucket_names) : "arn:aws:s3:::${name}"]
        Condition = local.same_bucket_owner
      },
      {
        Sid      = "ListPilotFacts"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::${local.bucket_names["lake"]}"
        Condition = {
          StringEquals = { "s3:ResourceAccount" = var.aws_account_id }
          StringLike   = { "s3:prefix" = [local.table_prefix, "${local.table_prefix}/*"] }
        }
      },
      {
        Sid       = "ReadPilotFiles"
        Effect    = "Allow"
        Action    = "s3:GetObject"
        Resource  = local.table_objects_arn
        Condition = local.same_bucket_owner
      },
      {
        Sid      = "ListQueryResults"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::${local.bucket_names["results"]}"
        Condition = {
          StringEquals = { "s3:ResourceAccount" = var.aws_account_id }
          StringLike   = { "s3:prefix" = ["queries", "queries/*"] }
        }
      },
      {
        Sid       = "UseQueryResults"
        Effect    = "Allow"
        Action    = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
        Resource  = "arn:aws:s3:::${local.bucket_names["results"]}/queries/*"
        Condition = local.same_bucket_owner
      },
      {
        Sid      = "PreservePilotFiles"
        Effect   = "Deny"
        Action   = ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]
        Resource = local.table_objects_arn
      },
      {
        Sid      = "PreservePilotCatalogEntry"
        Effect   = "Deny"
        Action   = ["glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:BatchDeleteTable"]
        Resource = local.table_arn
      },
    ]
  }
}

# No role, user or attachment: choosing a runner identity is a separate approved step.
resource "aws_iam_policy" "athena_reader" {
  count = var.enable_athena_reader_policy ? 1 : 0

  name        = "${var.deployment_name}-athena-reader"
  description = "One workgroup, read-only pilot facts, dedicated query result writes; unattached."
  policy      = jsonencode(local.athena_reader_policy)

  lifecycle {
    precondition {
      condition     = length(jsonencode(local.athena_reader_policy)) <= 6144
      error_message = "The Athena reader policy exceeds the managed IAM policy size limit."
    }
  }
}
