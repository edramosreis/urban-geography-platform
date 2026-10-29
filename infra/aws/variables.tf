variable "aws_region" {
  description = "Proposed demonstration region. This configuration does not authorize deployment."
  type        = string

  validation {
    condition     = var.aws_region == "eu-west-1"
    error_message = "This first demonstration is scoped to eu-west-1; another region needs review."
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
