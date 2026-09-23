terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.88.0"
    }
  }
}

variable "workspace_host" {
  type        = string
  description = "Existing AWS Databricks workspace URL, not an account-console URL."
  validation {
    condition     = can(regex("^https://[A-Za-z0-9.-]+\\.cloud\\.databricks\\.com/?$", var.workspace_host)) && trimsuffix(lower(var.workspace_host), "/") != "https://accounts.cloud.databricks.com"
    error_message = "Use an HTTPS AWS Databricks workspace URL."
  }
}

variable "credential_name" {
  type        = string
  description = "Unique UC storage credential owned only by this Terraform root."
  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{0,62}$", var.credential_name))
    error_message = "Use a lowercase identifier of at most 63 characters."
  }
}

variable "storage_role_arn" {
  type        = string
  description = "Role ARN exported by the optional 06-databricks-storage-role stack."
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$", var.storage_role_arn))
    error_message = "Provide the actual AWS IAM role ARN; not a user or root principal."
  }
}

variable "validate_storage_access" {
  type        = bool
  default     = false
  description = "False only for bootstrap; set true after activating exact IAM trust."
}

provider "databricks" {
  host = var.workspace_host
}

resource "databricks_storage_credential" "ticks" {
  name            = var.credential_name
  comment         = "Hybrid tick storage; CloudFormation owns the underlying IAM role."
  isolation_mode  = "ISOLATION_MODE_ISOLATED"
  skip_validation = !var.validate_storage_access
  force_destroy   = false
  force_update    = false

  aws_iam_role {
    role_arn = var.storage_role_arn
  }
}

output "credential_name" {
  value       = databricks_storage_credential.ticks.name
  description = "Pass to the workspace root only after validating IAM trust."
}

output "unity_catalog_principal_arn" {
  value       = databricks_storage_credential.ticks.aws_iam_role[0].unity_catalog_iam_arn
  description = "Exact principal for CloudFormation UnityCatalogPrincipalArn."
}

output "unity_catalog_external_id" {
  value       = databricks_storage_credential.ticks.aws_iam_role[0].external_id
  description = "Exact external ID for CloudFormation UnityCatalogExternalId. Not an API secret."
}