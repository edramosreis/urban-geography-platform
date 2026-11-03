variable "aws_region" {
  description = "Proposed demonstration region. This configuration does not authorize deployment."
  type        = string

  validation {
    condition     = var.aws_region == "eu-north-1"
    error_message = "This first demonstration is scoped to eu-north-1; another region needs review."
  }
}

variable "aws_account_id" {
  description = "Explicit target account; the provider rejects credentials for another account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "Supply the intended 12-digit AWS account ID."
  }
}

variable "deployment_name" {
  description = "Dedicated resource name prefix, 4..20 lowercase letters, digits or hyphens."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,18}[a-z0-9]$", var.deployment_name))
    error_message = "Use 4..20 lowercase characters, starting with a letter and ending alphanumeric."
  }
}

variable "max_query_scan_bytes" {
  description = "Proposed per-query scan cutoff, 10..100 MiB. This is not a total spending cap."
  type        = number
  default     = 104857600

  validation {
    condition = (
      var.max_query_scan_bytes >= 10485760 &&
      var.max_query_scan_bytes <= 104857600 &&
      floor(var.max_query_scan_bytes) == var.max_query_scan_bytes
    )
    error_message = "The per-query cutoff must be an integer between 10 MiB and 100 MiB."
  }
}


variable "glue_release" {
  description = "Optional verified release descriptor from prepare_glue_iam.py; null creates no IAM resources."
  type = object({
    release_id      = string
    manifest_sha256 = string
    summary_sha256  = string
    object_keys     = set(string)
  })
  default = null

  validation {
    condition = var.glue_release == null ? true : alltrue([
      for pin in [var.glue_release.release_id, var.glue_release.manifest_sha256, var.glue_release.summary_sha256] :
      can(regex("^[0-9a-f]{64}$", pin))
    ])
    error_message = "Release, manifest and summary identities must be lowercase SHA256 values."
  }

  validation {
    condition = var.glue_release == null ? true : (
      length(var.glue_release.object_keys) >= 10 && length(var.glue_release.object_keys) <= 33 &&
      alltrue([for key in var.glue_release.object_keys : length(key) <= 512 && (
        contains(local.release_jar_keys, key) ||
        can(regex("^runtime/releases/${var.glue_release.release_id}/(run_glue_s3_writer[.]py|urban-geography[.]gluewheels[.]zip|release[.]json)$", key)) ||
        can(regex("^inputs/${var.glue_release.summary_sha256}/(summary[.]json|input-manifest[.]json|(building_proximity|grid_proximity)/[A-Za-z0-9][A-Za-z0-9._-]*[.]parquet)$", key))
      )]) &&
      length(setsubtract(toset(concat(local.release_jar_keys, [
        "runtime/releases/${var.glue_release.release_id}/run_glue_s3_writer.py",
        "runtime/releases/${var.glue_release.release_id}/urban-geography.gluewheels.zip",
        "runtime/releases/${var.glue_release.release_id}/release.json",
        "inputs/${var.glue_release.summary_sha256}/summary.json",
        "inputs/${var.glue_release.summary_sha256}/input-manifest.json",
      ])), var.glue_release.object_keys)) == 0 &&
      anytrue([for key in var.glue_release.object_keys :
        startswith(key, "inputs/${var.glue_release.summary_sha256}/building_proximity/")
      ])
    )
    error_message = "Use 10..33 exact, safe keys for this release and pilot input, including all required software/control files."
  }
}


variable "glue_job_release_json" {
  description = "Optional exact pinned release.json bytes from --include-job; null creates no job."
  type        = string
  default     = null

  validation {
    condition = var.glue_job_release_json == null ? true : (
      var.glue_release == null ? false : (
        length(var.glue_job_release_json) <= 1048576 &&
        sha256(var.glue_job_release_json) == var.glue_release.manifest_sha256 &&
        can(jsondecode(var.glue_job_release_json))
      )
    )
    error_message = "Use exact, valid release JSON matching the explicit IAM manifest pin; IAM inputs are required."
  }
}


variable "enable_athena_reader_policy" {
  description = "Prepare an unattached pilot read/query policy; does not enable the workgroup or authorize execution."
  type        = bool
  default     = false
}


variable "enable_dbt_view_builder_policy" {
  description = "Prepare an unattached two-view dbt policy; no identity, workgroup enabling, dbt execution or teardown."
  type        = bool
  default     = false
}


variable "glue_operator_principal_arn" {
  description = "Optional exact same-account IAM role allowed to assume the pilot operator; null creates no operator."
  type        = string
  default     = null

  validation {
    condition = var.glue_operator_principal_arn == null ? true : (
      length(var.glue_operator_principal_arn) <= 576 &&
      can(regex("^arn:aws:iam::${var.aws_account_id}:role/([A-Za-z0-9_+=,.@-]+/)*[A-Za-z0-9_+=,.@-]{1,64}$", var.glue_operator_principal_arn)) &&
      var.glue_operator_principal_arn != "arn:aws:iam::${var.aws_account_id}:role/${var.deployment_name}-glue-operator"
    )
    error_message = "Choose one existing same-account IAM role ARN, including its path; no session ARN, root, wildcard or operator self-trust."
  }
}


variable "athena_reader_principal_arn" {
  description = "Optional exact same-account IAM role allowed to assume the fact reader; null leaves the policy unattached."
  type        = string
  default     = null

  validation {
    condition = var.athena_reader_principal_arn == null ? true : (
      length(var.athena_reader_principal_arn) <= 576 &&
      can(regex("^arn:aws:iam::${var.aws_account_id}:role/([A-Za-z0-9_+=,.@-]+/)*[A-Za-z0-9_+=,.@-]{1,64}$", var.athena_reader_principal_arn)) &&
      var.athena_reader_principal_arn != "arn:aws:iam::${var.aws_account_id}:role/${var.deployment_name}-athena-reader"
    )
    error_message = "Choose one existing same-account IAM role ARN; no user, root, session ARN, wildcard or reader self-trust."
  }
}
