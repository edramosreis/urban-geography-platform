output "warehouse_uri" {
  description = "Future Iceberg warehouse location; this configuration creates no tables."
  value       = aws_glue_catalog_database.analytics.location_uri
}

output "query_results_uri" {
  description = "Disposable query results only; never use this bucket for Iceberg table data."
  value       = "s3://${local.bucket_names["results"]}/queries/"
}

output "database_name" {
  value = aws_glue_catalog_database.analytics.name
}

output "workgroup_name" {
  description = "Created disabled. Enabling and executing queries need separate trial approval."
  value       = aws_athena_workgroup.analytics.name
}


output "release_delivery_policy_arn" {
  description = "Unattached policy; an approved uploader identity must be chosen separately."
  value       = try(aws_iam_policy.release_delivery[0].arn, null)
}

output "glue_writer_role_arn" {
  description = "Execution role for the optional, separately approved pilot job."
  value       = try(aws_iam_role.glue_writer[0].arn, null)
}

output "glue_log_group_prefix" {
  description = "The future job must set --custom-logGroup-prefix to this value."
  value       = var.glue_release == null ? null : local.glue_log_prefix
}


output "glue_job_name" {
  description = "Optional job definition; no run is started by this configuration."
  value       = try(aws_glue_job.pilot[0].name, null)
}

output "glue_job_deployment_policy_arn" {
  description = "Unattached policy for one job and exact-role PassRole; grants no StartJobRun."
  value       = try(aws_iam_policy.glue_job_deployment[0].arn, null)
}

output "glue_job_scratch_uri" {
  value = local.job_enabled ? "s3://${local.bucket_names["lake"]}/${local.scratch_prefix}/" : null
}


output "athena_reader_policy_arn" {
  description = "Unattached policy; runner identity, attachment and queries need separate approval."
  value       = try(aws_iam_policy.athena_reader[0].arn, null)
}


output "dbt_view_builder_policy_arn" {
  description = "Unattached standalone dbt policy; choose and approve its identity separately."
  value       = try(aws_iam_policy.dbt_view_builder[0].arn, null)
}


output "glue_operator_role_arn" {
  description = "Optional scoped controller role; creation and assuming it require separately approved AWS changes."
  value       = try(aws_iam_role.glue_operator[0].arn, null)
}
