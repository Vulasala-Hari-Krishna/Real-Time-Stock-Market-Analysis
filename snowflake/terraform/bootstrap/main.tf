terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.21.0"
    }
  }

  # Remote state, same reason as the Databricks credential root: this
  # root's generated IAM values must be read back after applying, fed into
  # CloudFormation trust activation, before the workspace root (a separate
  # state) can safely create the trust-dependent stage.
  backend "s3" {
    key     = "snowflake/bootstrap.tfstate"
    encrypt = true
  }
}

variable "bucket" {
  type        = string
  description = "Existing S3 bucket managed by CloudFormation; only publish/ is used."
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket))
    error_message = "Provide a bucket name, not an S3 URI or prefix."
  }
}

variable "storage_role_arn" {
  type        = string
  description = "Role ARN exported by the 07-snowflake-storage-role stack."
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$", var.storage_role_arn))
    error_message = "Provide the actual AWS IAM role ARN; not a user or root principal."
  }
}

variable "integration_name" {
  type    = string
  default = "STOCK_MARKET_DEV_PUBLISH_INTEGRATION"
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.integration_name))
    error_message = "Use an uppercase identifier of at most 64 characters."
  }
}

variable "organization_name" {
  type        = string
  description = "Organization segment of the Snowflake account identifier (before the hyphen, e.g. ILMRWBU in ILMRWBU-TX52777). Not a secret."
  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9]{0,63}$", var.organization_name))
    error_message = "Provide the organization name segment of the account identifier."
  }
}

variable "account_name" {
  type        = string
  description = "Account segment of the Snowflake account identifier (after the hyphen, e.g. TX52777 in ILMRWBU-TX52777). Not a secret."
  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9]{0,63}$", var.account_name))
    error_message = "Provide the account name segment of the account identifier."
  }
}

# User/private key come from SNOWFLAKE_USER/SNOWFLAKE_PRIVATE_KEY env vars.
# Provider 2.x does not read SNOWFLAKE_ACCOUNT (confirmed live 2026-09-25);
# organization_name/account_name are explicit instead.
provider "snowflake" {
  organization_name = var.organization_name
  account_name       = var.account_name
  authenticator      = "SNOWFLAKE_JWT"
}

# No lifecycle.precondition here, deliberately: this resource has nothing to
# wait for, and this root is always safe to apply. Gating anything on
# IAM trust belongs only in the workspace root's stage resource - verified
# live (2026-09-25) that a failed precondition blocks Terraform's entire
# plan/apply, not just the gated resource, so mixing a gated resource into
# the same root/apply as ungated ones would have blocked all of them too.
resource "snowflake_storage_integration_aws" "ticks" {
  name                      = var.integration_name
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = var.storage_role_arn
  storage_allowed_locations = ["s3://${var.bucket}/publish/"]
  comment                   = "Hybrid publish/ snapshots only; least-privilege read-only IAM role."
}

# Unverified: describe_output is documented as a List of Object; [0]
# indexing is the expected access pattern but untested live (the previous
# attempt never reached resource creation).
output "iam_principal_arn" {
  value       = snowflake_storage_integration_aws.ticks.describe_output[0].iam_user_arn
  description = "Exact principal for CloudFormation SnowflakeIamUserArn."
}

output "external_id" {
  value       = snowflake_storage_integration_aws.ticks.describe_output[0].external_id
  description = "Exact external ID for CloudFormation SnowflakeExternalId. Not an API secret."
}

output "integration_name" {
  value       = snowflake_storage_integration_aws.ticks.name
  description = "Pass to the workspace root's integration_name variable."
}
