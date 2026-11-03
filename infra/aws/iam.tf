locals {
  release_jar_keys = [
    "runtime/jars/iceberg-spark-runtime-3.5_2.12-1.10.2.jar",
    "runtime/jars/sedona-spark-shaded-3.5_2.12-1.8.0.jar",
    "runtime/jars/geotools-wrapper-1.8.0-33.1.jar",
    "runtime/jars/iceberg-aws-bundle-1.10.2.jar",
  ]
  release_object_arns = var.glue_release == null ? [] : [
    for key in sort(tolist(var.glue_release.object_keys)) : "arn:aws:s3:::${local.bucket_names["lake"]}/${key}"
  ]
  delivery_prefix_arns = [for prefix in ["runtime", "inputs"] : "arn:aws:s3:::${local.bucket_names["lake"]}/${prefix}/*"]
  table_prefix         = "warehouse/building_proximity"
  table_objects_arn    = "arn:aws:s3:::${local.bucket_names["lake"]}/${local.table_prefix}/*"
  catalog_arn          = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog"
  database_arn         = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/${local.database_name}"
  table_arn            = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${local.database_name}/building_proximity"
  glue_log_prefix      = "/urban-geography/${var.deployment_name}/glue"
  log_groups           = var.glue_release == null ? toset([]) : toset(["error", "output"])
  same_bucket_owner    = { StringEquals = { "s3:ResourceAccount" = var.aws_account_id } }

  delivery_policy = {
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "ReadAndDeliverExactRelease"
        Effect    = "Allow"
        Action    = ["s3:GetObject", "s3:PutObject"]
        Resource  = local.release_object_arns
        Condition = local.same_bucket_owner
      },
      # Explicit denies also apply if another policy attached to the uploader grants PUT.
      {
        Sid       = "RequireCreateOnlyDelivery"
        Effect    = "Deny"
        Action    = "s3:PutObject"
        Resource  = local.delivery_prefix_arns
        Condition = { StringNotEquals = { "s3:if-none-match" = "*" } }
      },
      {
        Sid       = "RequireDeliveryEncryption"
        Effect    = "Deny"
        Action    = "s3:PutObject"
        Resource  = local.delivery_prefix_arns
        Condition = { StringNotEquals = { "s3:x-amz-server-side-encryption" = "AES256" } }
      },
      {
        Sid      = "PreserveDeliveredObjects"
        Effect   = "Deny"
        Action   = ["s3:DeleteObject", "s3:DeleteObjectVersion"]
        Resource = local.delivery_prefix_arns
      },
    ]
  }
  writer_policy = {
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid       = "ReadExactRelease"
        Effect    = "Allow"
        Action    = "s3:GetObject"
        Resource  = local.release_object_arns
        Condition = local.same_bucket_owner
      },
      {
        Sid       = "LocateLakeBucket"
        Effect    = "Allow"
        Action    = "s3:GetBucketLocation"
        Resource  = "arn:aws:s3:::${local.bucket_names["lake"]}"
        Condition = local.same_bucket_owner
      },
      {
        Sid      = "ListPilotTableOnly"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::${local.bucket_names["lake"]}"
        Condition = {
          StringEquals = { "s3:ResourceAccount" = var.aws_account_id }
          StringLike   = { "s3:prefix" = [local.table_prefix, "${local.table_prefix}/*"] }
        }
      },
      {
        Sid       = "WritePilotTable"
        Effect    = "Allow"
        Action    = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource  = local.table_objects_arn
        Condition = local.same_bucket_owner
      },
      {
        Sid      = "ReadExistingDatabase"
        Effect   = "Allow"
        Action   = "glue:GetDatabase"
        Resource = [local.catalog_arn, local.database_arn]
      },
      {
        Sid      = "CommitPilotTable"
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]
        Resource = [local.catalog_arn, local.database_arn, local.table_arn]
      },
      {
        Sid    = "WriteDedicatedLogs"
        Effect = "Allow"
        Action = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = [for kind in ["error", "output"] :
          "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:${local.glue_log_prefix}/${kind}:log-stream:*"
        ]
      },
      ], [for statement in [
        {
          Sid       = "UseReleaseScratch"
          Effect    = "Allow"
          Action    = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
          Resource  = "arn:aws:s3:::${local.bucket_names["lake"]}/${local.scratch_prefix}/*"
          Condition = local.same_bucket_owner
        },
        {
          Sid      = "ListReleaseScratch"
          Effect   = "Allow"
          Action   = "s3:ListBucket"
          Resource = "arn:aws:s3:::${local.bucket_names["lake"]}"
          Condition = {
            StringEquals = { "s3:ResourceAccount" = var.aws_account_id }
            StringLike   = { "s3:prefix" = [local.scratch_prefix, "${local.scratch_prefix}/*"] }
          }
        },
    ] : statement if local.job_enabled])
  }
}

# A future approved operator must explicitly attach this policy to its chosen identity.
resource "aws_iam_policy" "release_delivery" {
  count = var.glue_release == null ? 0 : 1

  name        = "${var.deployment_name}-release-delivery"
  description = "Create-only delivery and readback of one verified release; no execution permissions."
  policy      = jsonencode(local.delivery_policy)

  lifecycle {
    precondition {
      condition     = length(jsonencode(local.delivery_policy)) <= 6144
      error_message = "The exact-key delivery policy exceeds the managed IAM policy size limit."
    }
  }
}

resource "aws_iam_role" "glue_writer" {
  count = var.glue_release == null ? 0 : 1

  name                 = "${var.deployment_name}-glue-writer"
  path                 = "/service-role/"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "glue_writer" {
  count = var.glue_release == null ? 0 : 1

  name   = "pilot-writer"
  role   = aws_iam_role.glue_writer[0].id
  policy = jsonencode(local.writer_policy)

  lifecycle {
    precondition {
      condition     = length(jsonencode(local.writer_policy)) <= 10240
      error_message = "The exact-key writer policy exceeds the inline role policy size limit."
    }
  }
}

# The future job must use --custom-logGroup-prefix with this precreated prefix.
resource "aws_cloudwatch_log_group" "glue" {
  for_each = local.log_groups

  name              = "${local.glue_log_prefix}/${each.key}"
  retention_in_days = 7
}


# This is the human controller's role, separate from Glue's service execution role.
# IAM scopes the job, not an individual run, and does not enforce a one-run budget.
resource "aws_iam_role" "glue_operator" {
  count = var.glue_operator_principal_arn == null ? 0 : 1

  name                 = "${var.deployment_name}-glue-operator"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = var.glue_operator_principal_arn }
    }]
  })

  lifecycle {
    precondition {
      condition     = local.job_enabled
      error_message = "The operator requires the verified release and its optional pilot job."
    }
  }
}

resource "aws_iam_role_policy" "glue_operator" {
  count = var.glue_operator_principal_arn != null && local.job_enabled ? 1 : 0

  name = "pilot-control"
  role = aws_iam_role.glue_operator[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ControlOnePilotJob"
        Effect   = "Allow"
        Action   = ["glue:GetJob", "glue:StartJobRun", "glue:GetJobRun", "glue:BatchStopJobRun"]
        Resource = local.job_arn
      },
      {
        Sid       = "VerifyReleaseCompletion"
        Effect    = "Allow"
        Action    = "s3:GetObject"
        Resource  = "arn:aws:s3:::${local.bucket_names["lake"]}/runtime/releases/${var.glue_release.release_id}/release.json"
        Condition = local.same_bucket_owner
      },
      {
        Sid      = "ReadPilotOutputLogs"
        Effect   = "Allow"
        Action   = "logs:FilterLogEvents"
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:${local.glue_log_prefix}/output:*"
      },
    ]
  })
}
