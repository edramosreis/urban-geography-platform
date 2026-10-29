locals {
  bucket_names = {
    for kind in ["lake", "results"] :
    kind => "${var.deployment_name}-${kind}-${var.aws_account_id}-${var.aws_region}"
  }
  database_name = "${replace(var.deployment_name, "-", "_")}_m4"
}

resource "aws_s3_bucket" "storage" {
  for_each = local.bucket_names

  bucket        = each.value
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "storage" {
  for_each = local.bucket_names

  bucket                  = aws_s3_bucket.storage[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "storage" {
  for_each = local.bucket_names

  bucket = aws_s3_bucket.storage[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "storage" {
  for_each = local.bucket_names

  bucket = aws_s3_bucket.storage[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "tls" {
  for_each = local.bucket_names

  bucket = aws_s3_bucket.storage[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource = [
        "arn:aws:s3:::${each.value}",
        "arn:aws:s3:::${each.value}/*"
      ]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.storage["lake"].id
  versioning_configuration {
    status = "Enabled"
  }
}

# Only disposable query results expire. Iceberg files require transactional maintenance.
resource "aws_s3_bucket_lifecycle_configuration" "query_results" {
  bucket = aws_s3_bucket.storage["results"].id
  rule {
    id     = "query-results-only"
    status = "Enabled"
    filter {
      prefix = "queries/"
    }
    expiration {
      days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_glue_catalog_database" "analytics" {
  catalog_id   = var.aws_account_id
  name         = local.database_name
  location_uri = "s3://${local.bucket_names["lake"]}/warehouse/"
}

resource "aws_athena_workgroup" "analytics" {
  name          = "${var.deployment_name}-m4"
  description   = "Bounded urban-geography demonstration; enable only for an approved trial."
  state         = "DISABLED"
  force_destroy = false

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.max_query_scan_bytes

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location       = "s3://${local.bucket_names["results"]}/queries/"
      expected_bucket_owner = var.aws_account_id
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  depends_on = [
    aws_s3_bucket_public_access_block.storage,
    aws_s3_bucket_ownership_controls.storage,
    aws_s3_bucket_server_side_encryption_configuration.storage,
    aws_s3_bucket_policy.tls,
  ]
}
