# Mocked plans only; no live AWS requests, policy attachments or queries.
mock_provider "aws" {}

variables {
  aws_region      = "eu-north-1"
  aws_account_id  = "123456789012"
  deployment_name = "urban-geo-test"
}

run "reader_disabled_by_default" {
  command = plan

  assert {
    condition     = length(aws_iam_policy.athena_reader) == 0 && output.athena_reader_policy_arn == null
    error_message = "Default foundation must omit the optional Athena policy."
  }
}

run "scoped_reader_policy" {
  command = plan
  variables { enable_athena_reader_policy = true }

  assert {
    condition = (
      length(aws_iam_policy.athena_reader) == 1 &&
      aws_iam_policy.athena_reader[0].name == "urban-geo-test-athena-reader" &&
      length(jsonencode(local.athena_reader_policy)) <= 6144 &&
      length(aws_iam_role.glue_writer) == 0 && length(aws_iam_role_policy.glue_writer) == 0 &&
      aws_athena_workgroup.analytics.state == "DISABLED"
    )
    error_message = "The optional policy must stay unattached and must not enable the workgroup or create a role."
  }
  assert {
    condition = (
      local.athena_reader_policy.Statement[0].Resource == "arn:aws:athena:eu-north-1:123456789012:workgroup/urban-geo-test-m4" &&
      toset(local.athena_reader_policy.Statement[0].Action) == toset(["athena:GetWorkGroup", "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"]) &&
      alltrue([for s in local.athena_reader_policy.Statement : !contains(try(tolist(s.Resource), [s.Resource]), "*")])
    )
    error_message = "Query permissions must name one workgroup; no unrestricted resource is allowed."
  }
  assert {
    condition = (
      local.athena_reader_policy.Statement[1].Action == "glue:GetDatabase" &&
      local.athena_reader_policy.Statement[2].Action == "glue:GetTable" &&
      toset(local.athena_reader_policy.Statement[2].Resource) == toset([
        "arn:aws:glue:eu-north-1:123456789012:catalog",
        "arn:aws:glue:eu-north-1:123456789012:database/urban_geo_test_m4",
        "arn:aws:glue:eu-north-1:123456789012:table/urban_geo_test_m4/building_proximity"
      ]) &&
      local.athena_reader_policy.Statement[4].Condition.StringLike["s3:prefix"] == ["warehouse/building_proximity", "warehouse/building_proximity/*"] &&
      local.athena_reader_policy.Statement[5].Action == "s3:GetObject" &&
      local.athena_reader_policy.Statement[5].Resource == "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-north-1/warehouse/building_proximity/*"
    )
    error_message = "Catalog and source reads must be confined to the pilot fact table."
  }
  assert {
    condition = (
      local.athena_reader_policy.Statement[6].Condition.StringLike["s3:prefix"] == ["queries", "queries/*"] &&
      local.athena_reader_policy.Statement[7].Resource == "arn:aws:s3:::urban-geo-test-results-123456789012-eu-north-1/queries/*" &&
      toset(local.athena_reader_policy.Statement[7].Action) == toset(["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]) &&
      alltrue([for index in [3, 4, 5, 6, 7] : local.athena_reader_policy.Statement[index].Condition.StringEquals["s3:ResourceAccount"] == var.aws_account_id])
    )
    error_message = "Results writes must use only the dedicated results prefix with the expected S3 owner."
  }
  assert {
    condition = (
      local.athena_reader_policy.Statement[8].Effect == "Deny" &&
      toset(local.athena_reader_policy.Statement[8].Action) == toset(["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]) &&
      local.athena_reader_policy.Statement[9].Effect == "Deny" &&
      local.athena_reader_policy.Statement[9].Resource == local.table_arn &&
      toset(local.athena_reader_policy.Statement[9].Action) == toset(["glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:BatchDeleteTable"])
    )
    error_message = "The query identity must preserve the source table's objects and catalog entry."
  }
}


run "view_builder_disabled_by_default" {
  command = plan
  assert {
    condition     = length(aws_iam_policy.dbt_view_builder) == 0 && output.dbt_view_builder_policy_arn == null
    error_message = "The foundation must omit dbt IAM by default."
  }
}

run "standalone_two_view_builder" {
  command = plan
  variables { enable_dbt_view_builder_policy = true }

  assert {
    condition = (
      length(aws_iam_policy.dbt_view_builder) == 1 &&
      aws_iam_policy.dbt_view_builder[0].name == "urban-geo-test-dbt-view-builder" &&
      length(jsonencode(local.dbt_view_builder_policy)) <= 6144 &&
      length(aws_iam_policy.athena_reader) == 0 &&
      length(aws_iam_role.glue_writer) == 0 &&
      length(aws_iam_role_policy.glue_writer) == 0 &&
      aws_athena_workgroup.analytics.state == "DISABLED"
    )
    error_message = "Builder must stand alone, remain unattached and leave the workgroup disabled."
  }
  assert {
    condition = (
      length(local.dbt_view_builder_policy.Statement) == 13 &&
      jsonencode(slice(local.dbt_view_builder_policy.Statement, 0, 10)) == jsonencode(local.athena_reader_policy.Statement) &&
      alltrue([for s in local.dbt_view_builder_policy.Statement : !contains(try(tolist(s.Resource), [s.Resource]), "*")]) &&
      alltrue([for s in slice(local.dbt_view_builder_policy.Statement, 10, 13) : s.Effect == "Allow"])
    )
    error_message = "Builder must retain every scoped reader permission/deny without unrestricted resources."
  }
  assert {
    condition = (
      local.dbt_view_builder_policy.Statement[10].Action == "glue:GetDatabases" &&
      toset(local.dbt_view_builder_policy.Statement[10].Resource) == toset([local.catalog_arn, local.database_arn]) &&
      local.dbt_view_builder_policy.Statement[11].Action == "glue:GetTables" &&
      toset(local.dbt_view_builder_policy.Statement[11].Resource) == toset(concat([local.catalog_arn, local.database_arn, local.table_arn], local.gold_view_arns)) &&
      toset(local.gold_view_arns) == toset([
        "arn:aws:glue:eu-north-1:123456789012:table/urban_geo_test_m4/grid_proximity",
        "arn:aws:glue:eu-north-1:123456789012:table/urban_geo_test_m4/release_proximity"
      ])
    )
    error_message = "Discovery must use one database and the source plus two exact Gold relations."
  }
  assert {
    condition = (
      toset(local.dbt_view_builder_policy.Statement[12].Action) == toset(["glue:GetTable", "glue:GetTableVersions", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTableVersion"]) &&
      toset(local.dbt_view_builder_policy.Statement[12].Resource) == toset(concat([local.catalog_arn, local.database_arn], local.gold_view_arns)) &&
      !contains(local.dbt_view_builder_policy.Statement[12].Resource, local.table_arn)
    )
    error_message = "Create/replace and version maintenance must name only two views and their ancestors, never source facts."
  }
  assert {
    condition = alltrue([for s in slice(local.dbt_view_builder_policy.Statement, 10, 13) :
      alltrue([for action in try(tolist(s.Action), [s.Action]) : startswith(action, "glue:") && !contains([
        "glue:CreateDatabase", "glue:DeleteDatabase", "glue:DeleteTable", "glue:BatchDeleteTable"
      ], action)])
    ])
    error_message = "The builder extension must grant no S3 writes, database creation/deletion or relation teardown."
  }
}

run "reader_and_builder_are_independent" {
  command = plan
  variables {
    enable_athena_reader_policy    = true
    enable_dbt_view_builder_policy = true
  }
  assert {
    condition = (
      length(aws_iam_policy.athena_reader) == 1 && length(aws_iam_policy.dbt_view_builder) == 1 &&
      length(local.athena_reader_policy.Statement) == 10 &&
      !contains(local.athena_reader_policy.Statement[2].Resource, local.gold_view_arns[0]) &&
      !contains(local.athena_reader_policy.Statement[2].Resource, local.gold_view_arns[1])
    )
    error_message = "Enabling dbt must not widen the independent fact-reader policy."
  }
}


run "reader_role_trust_and_policy_attachment" {
  command = plan
  variables {
    enable_athena_reader_policy = true
    athena_reader_principal_arn = "arn:aws:iam::123456789012:role/managed/project/login"
  }
  override_resource {
    override_during = plan
    target          = aws_iam_policy.athena_reader[0]
    values          = { arn = "arn:aws:iam::123456789012:policy/urban-geo-test-athena-reader" }
  }
  override_resource {
    override_during = plan
    target          = aws_iam_role.athena_reader[0]
    values          = { arn = "arn:aws:iam::123456789012:role/urban-geo-test-athena-reader" }
  }
  assert {
    condition = (
      aws_iam_role.athena_reader[0].name == "urban-geo-test-athena-reader" &&
      aws_iam_role.athena_reader[0].max_session_duration == 3600 &&
      jsondecode(aws_iam_role.athena_reader[0].assume_role_policy) == {
        Version = "2012-10-17"
        Statement = [{
          Effect    = "Allow"
          Action    = "sts:AssumeRole"
          Principal = { AWS = var.athena_reader_principal_arn }
        }]
      } &&
      aws_iam_role_policy_attachment.athena_reader[0].role == aws_iam_role.athena_reader[0].name &&
      aws_iam_role_policy_attachment.athena_reader[0].policy_arn == aws_iam_policy.athena_reader[0].arn &&
      output.athena_reader_role_arn == "arn:aws:iam::123456789012:role/urban-geo-test-athena-reader" &&
      aws_athena_workgroup.analytics.state == "DISABLED" &&
      length(aws_iam_policy.dbt_view_builder) == 0
    )
    error_message = "Trust one role, allow a one-hour maximum, attach only the fact-reader policy and leave query admission disabled."
  }
}

run "policy_only_keeps_identity_optional" {
  command = plan
  variables { enable_athena_reader_policy = true }
  assert {
    condition = (
      length(aws_iam_role.athena_reader) == 0 && length(aws_iam_role_policy_attachment.athena_reader) == 0 &&
      output.athena_reader_role_arn == null
    )
    error_message = "The existing policy-only preparation must not gain a role or attachment."
  }
}

run "reader_role_requires_reader_policy" {
  command = plan
  variables { athena_reader_principal_arn = "arn:aws:iam::123456789012:role/login" }
  expect_failures = [aws_iam_role.athena_reader[0]]
}

run "reader_rejects_cross_account_trust" {
  command = plan
  variables {
    enable_athena_reader_policy = true
    athena_reader_principal_arn = "arn:aws:iam::999999999999:role/login"
  }
  expect_failures = [var.athena_reader_principal_arn]
}

run "reader_rejects_session_trust" {
  command = plan
  variables {
    enable_athena_reader_policy = true
    athena_reader_principal_arn = "arn:aws:sts::123456789012:assumed-role/login/session"
  }
  expect_failures = [var.athena_reader_principal_arn]
}

run "reader_rejects_wildcard_trust" {
  command = plan
  variables {
    enable_athena_reader_policy = true
    athena_reader_principal_arn = "arn:aws:iam::123456789012:role/*"
  }
  expect_failures = [var.athena_reader_principal_arn]
}

run "reader_rejects_root_trust" {
  command = plan
  variables {
    enable_athena_reader_policy = true
    athena_reader_principal_arn = "arn:aws:iam::123456789012:root"
  }
  expect_failures = [var.athena_reader_principal_arn]
}

run "reader_rejects_self_trust" {
  command = plan
  variables {
    enable_athena_reader_policy = true
    athena_reader_principal_arn = "arn:aws:iam::123456789012:role/urban-geo-test-athena-reader"
  }
  expect_failures = [var.athena_reader_principal_arn]
}

run "reader_attachment_stays_separate_from_view_builder" {
  command = plan
  variables {
    enable_athena_reader_policy    = true
    enable_dbt_view_builder_policy = true
    athena_reader_principal_arn    = "arn:aws:iam::123456789012:role/login"
  }
  override_resource {
    override_during = plan
    target          = aws_iam_policy.athena_reader[0]
    values          = { arn = "arn:aws:iam::123456789012:policy/urban-geo-test-athena-reader" }
  }
  assert {
    condition = (
      length(aws_iam_role_policy_attachment.athena_reader) == 1 &&
      aws_iam_role_policy_attachment.athena_reader[0].policy_arn == aws_iam_policy.athena_reader[0].arn &&
      length(aws_iam_policy.dbt_view_builder) == 1 &&
      jsondecode(aws_iam_policy.athena_reader[0].policy).Statement == jsondecode(jsonencode(local.athena_reader_policy.Statement)) &&
      aws_athena_workgroup.analytics.state == "DISABLED"
    )
    error_message = "Preparing dbt's separate policy must not attach its view permissions to the reader."
  }
}
