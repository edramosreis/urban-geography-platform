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
