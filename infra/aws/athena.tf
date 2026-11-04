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


# Separate, standalone policy: the reader remains read-only. Ordinary view models
# need catalog discovery and create/replace/version maintenance, not S3 table writes.
locals {
  gold_view_names = ["grid_proximity", "release_proximity"]
  gold_view_arns  = [for name in local.gold_view_names : "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${aws_glue_catalog_database.analytics.name}/${name}"]
  dbt_view_builder_policy = {
    Version = "2012-10-17"
    Statement = concat(local.athena_reader_policy.Statement, [
      {
        Sid      = "DiscoverPilotDatabase"
        Effect   = "Allow"
        Action   = "glue:GetDatabases"
        Resource = [local.catalog_arn, local.database_arn]
      },
      {
        Sid      = "DiscoverPilotRelations"
        Effect   = "Allow"
        Action   = "glue:GetTables"
        Resource = concat([local.catalog_arn, local.database_arn, local.table_arn], local.gold_view_arns)
      },
      {
        Sid      = "BuildGoldViews"
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetTableVersions", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTableVersion"]
        Resource = concat([local.catalog_arn, local.database_arn], local.gold_view_arns)
      },
    ])
  }
}

# Remains unattached unless the optional, explicitly trusted reader role is requested.
resource "aws_iam_policy" "athena_reader" {
  count = var.enable_athena_reader_policy ? 1 : 0

  name        = "${var.deployment_name}-athena-reader"
  description = "One workgroup, preserved pilot facts and dedicated query result writes."
  policy      = jsonencode(local.athena_reader_policy)

  lifecycle {
    precondition {
      condition     = length(jsonencode(local.athena_reader_policy)) <= 6144
      error_message = "The Athena reader policy exceeds the managed IAM policy size limit."
    }
  }
}

# Policy-only mode remains available; the optional builder role attaches only this policy.
resource "aws_iam_policy" "dbt_view_builder" {
  count = var.enable_dbt_view_builder_policy ? 1 : 0

  name        = "${var.deployment_name}-dbt-view-builder"
  description = "One workgroup, preserved pilot facts and two Gold view definitions."
  policy      = jsonencode(local.dbt_view_builder_policy)

  lifecycle {
    precondition {
      condition     = length(jsonencode(local.dbt_view_builder_policy)) <= 6144
      error_message = "The dbt view builder policy exceeds the managed IAM policy size limit."
    }
  }
}


# This role is for fact verification/querying; the dbt builder remains separate.
resource "aws_iam_role" "athena_reader" {
  count = var.athena_reader_principal_arn == null ? 0 : 1

  name                 = "${var.deployment_name}-athena-reader"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = var.athena_reader_principal_arn }
    }]
  })

  lifecycle {
    precondition {
      condition     = var.enable_athena_reader_policy
      error_message = "The reader role requires its explicitly enabled fact-reader policy."
    }
  }
}

resource "aws_iam_role_policy_attachment" "athena_reader" {
  count = var.athena_reader_principal_arn != null && var.enable_athena_reader_policy ? 1 : 0

  role       = aws_iam_role.athena_reader[0].name
  policy_arn = aws_iam_policy.athena_reader[0].arn
}


# Separate human-controlled view identity; never attach builder permissions to the reader.
resource "aws_iam_role" "dbt_view_builder" {
  count = var.dbt_view_builder_principal_arn == null ? 0 : 1

  name                 = "${var.deployment_name}-dbt-view-builder"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = var.dbt_view_builder_principal_arn }
    }]
  })

  lifecycle {
    precondition {
      condition     = var.enable_dbt_view_builder_policy
      error_message = "The builder role requires its explicitly enabled two-view policy."
    }
  }
}

resource "aws_iam_role_policy_attachment" "dbt_view_builder" {
  count = var.dbt_view_builder_principal_arn != null && var.enable_dbt_view_builder_policy ? 1 : 0

  role       = aws_iam_role.dbt_view_builder[0].name
  policy_arn = aws_iam_policy.dbt_view_builder[0].arn
}
