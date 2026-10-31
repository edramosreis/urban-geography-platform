# Mocked plans only; no AWS requests, policy attachments or queries.
mock_provider "aws" {}

variables {
  aws_region      = "eu-west-1"
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
      local.athena_reader_policy.Statement[0].Resource == "arn:aws:athena:eu-west-1:123456789012:workgroup/urban-geo-test-m4" &&
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
        "arn:aws:glue:eu-west-1:123456789012:catalog",
        "arn:aws:glue:eu-west-1:123456789012:database/urban_geo_test_m4",
        "arn:aws:glue:eu-west-1:123456789012:table/urban_geo_test_m4/building_proximity"
      ]) &&
      local.athena_reader_policy.Statement[4].Condition.StringLike["s3:prefix"] == ["warehouse/building_proximity", "warehouse/building_proximity/*"] &&
      local.athena_reader_policy.Statement[5].Action == "s3:GetObject" &&
      local.athena_reader_policy.Statement[5].Resource == "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-west-1/warehouse/building_proximity/*"
    )
    error_message = "Catalog and source reads must be confined to the pilot fact table."
  }
  assert {
    condition = (
      local.athena_reader_policy.Statement[6].Condition.StringLike["s3:prefix"] == ["queries", "queries/*"] &&
      local.athena_reader_policy.Statement[7].Resource == "arn:aws:s3:::urban-geo-test-results-123456789012-eu-west-1/queries/*" &&
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
