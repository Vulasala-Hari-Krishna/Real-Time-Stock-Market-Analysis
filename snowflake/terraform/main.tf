terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.21.0"
    }
  }

  # Remote state, same reason as the Databricks roots: the storage
  # integration's generated IAM values must be read back after the first
  # apply, fed into CloudFormation trust activation, then the stage (gated
  # on that activation) applied in a later run. A disposable CI runner has
  # no local disk continuity between those applies.
  backend "s3" {
    key     = "snowflake/terraform.tfstate"
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

variable "database_name" {
  type    = string
  default = "STOCK_MARKET_DEV"
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.database_name))
    error_message = "Use an uppercase identifier of at most 64 characters."
  }
}

variable "schema_name" {
  type    = string
  default = "PUBLISH_STAGING"
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.schema_name))
    error_message = "Use an uppercase identifier of at most 64 characters."
  }
}

variable "warehouse_name" {
  type    = string
  default = "STOCK_MARKET_DEV_WH"
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.warehouse_name))
    error_message = "Use an uppercase identifier of at most 64 characters."
  }
}

variable "loader_role_name" {
  type    = string
  default = "STOCK_MARKET_DEV_LOADER"
  validation {
    condition = (
      can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.loader_role_name)) &&
      !contains(["ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN", "USERADMIN", "PUBLIC"], var.loader_role_name)
    )
    error_message = "Use a dedicated non-admin role identifier, not a built-in Snowflake role."
  }
}

variable "credit_quota" {
  type    = number
  default = 10
  validation {
    condition     = var.credit_quota > 0 && var.credit_quota <= 25
    error_message = "Choose a positive credit quota no greater than 25 for this personal slice."
  }
}

variable "trust_activation_confirmed" {
  type        = bool
  default     = false
  description = "Set true only after activating exact IAM trust on the storage role stack."
}

# Account/user/private key come from SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER/
# SNOWFLAKE_PRIVATE_KEY env vars, matching how the Databricks provider reads
# DATABRICKS_TOKEN - never pass credentials as Terraform variables.
provider "snowflake" {
  authenticator = "SNOWFLAKE_JWT"
}

resource "snowflake_resource_monitor" "ticks" {
  name            = "${var.warehouse_name}_MONITOR"
  credit_quota    = var.credit_quota
  suspend_trigger = 80
}

resource "snowflake_warehouse" "ticks" {
  name                = var.warehouse_name
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.ticks.fully_qualified_name
  comment             = "Personal hybrid slice; one X-Small warehouse shared by load/transform/query."
}

resource "snowflake_database" "ticks" {
  name    = var.database_name
  comment = "Dedicated hybrid database; loaded exclusively from completed Databricks gold snapshots."
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.ticks.name
  name     = var.schema_name
  comment  = "Staging tables for completed publish/ snapshot batches."
}

resource "snowflake_account_role" "loader" {
  name    = var.loader_role_name
  comment = "Least-privilege loader role; routine sessions must not use ACCOUNTADMIN."
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.ticks.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.ticks.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_privileges" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE"]
  on_schema {
    schema_name = snowflake_schema.staging.fully_qualified_name
  }
}

resource "snowflake_storage_integration_aws" "ticks" {
  name                      = "${var.database_name}_PUBLISH_INTEGRATION"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = var.storage_role_arn
  storage_allowed_locations = ["s3://${var.bucket}/publish/"]
  comment                   = "Hybrid publish/ snapshots only; least-privilege read-only IAM role."
}

resource "snowflake_stage" "publish" {
  name                = "${var.database_name}_PUBLISH_STAGE"
  database            = snowflake_database.ticks.name
  schema              = snowflake_schema.staging.name
  url                 = "s3://${var.bucket}/publish/"
  storage_integration = snowflake_storage_integration_aws.ticks.name
  comment             = "Read-only external stage over completed export batches."

  lifecycle {
    precondition {
      condition     = var.trust_activation_confirmed
      error_message = "Activate and validate exact CloudFormation trust first."
    }
  }
}

# Unverified: the on_schema_object block shape (object_type/object_name for a
# STAGE) is inferred from the confirmed on_account_object/on_schema patterns
# above, not confirmed directly against the provider docs. May need one
# fix-iteration against a real `terraform plan`, same as the Databricks
# databricks_permissions incident earlier this session.
resource "snowflake_grant_privileges_to_account_role" "stage_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage.publish.fully_qualified_name
  }
}

# Unverified: describe_output is documented as a List of Object; [0] indexing
# is the expected access pattern but has not been exercised against a live
# apply.
output "iam_principal_arn" {
  value       = snowflake_storage_integration_aws.ticks.describe_output[0].iam_user_arn
  description = "Exact principal for CloudFormation SnowflakeIamUserArn."
}

output "external_id" {
  value       = snowflake_storage_integration_aws.ticks.describe_output[0].external_id
  description = "Exact external ID for CloudFormation SnowflakeExternalId. Not an API secret."
}

output "warehouse_name" {
  value = snowflake_warehouse.ticks.name
}

output "database_name" {
  value = snowflake_database.ticks.name
}

output "schema_name" {
  value = snowflake_schema.staging.name
}

output "loader_role_name" {
  value = snowflake_account_role.loader.name
}

output "stage_name" {
  value = snowflake_stage.publish.fully_qualified_name
}
