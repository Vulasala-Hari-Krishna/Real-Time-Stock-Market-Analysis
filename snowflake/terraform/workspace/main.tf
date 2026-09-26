terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.21.0"
    }
  }

  backend "s3" {
    key     = "snowflake/workspace.tfstate"
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

variable "serving_schema_name" {
  type        = string
  default     = "SERVING"
  description = "Schema holding R4's published, queryable snapshot tables - separate from the staging/control schema so dashboards/marts never depend on disposable staging objects."
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.serving_schema_name))
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

variable "integration_name" {
  type        = string
  description = "Existing storage integration name, from the bootstrap root's integration_name output."
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.integration_name))
    error_message = "Provide the bootstrap root's integration identifier."
  }
}

variable "organization_name" {
  type        = string
  description = "Organization segment of the Snowflake account identifier. Not a secret."
  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9]{0,63}$", var.organization_name))
    error_message = "Provide the organization name segment of the account identifier."
  }
}

variable "account_name" {
  type        = string
  description = "Account segment of the Snowflake account identifier. Not a secret."
  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9]{0,63}$", var.account_name))
    error_message = "Provide the account name segment of the account identifier."
  }
}

variable "trust_activation_confirmed" {
  type        = bool
  default     = false
  description = "Set true only after activating exact IAM trust on the storage role stack and applying the bootstrap root."
}

variable "loader_grantee_user" {
  type        = string
  description = "Existing Snowflake username to grant the loader role to (the solo trial admin login, for this personal project - not a new identity created here)."
  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_]{0,63}$", var.loader_grantee_user))
    error_message = "Provide an existing Snowflake username."
  }
}

variable "reader_role_name" {
  type        = string
  default     = "STOCK_MARKET_DEV_READER"
  description = "Read-only role for R5 (the Streamlit historical view) - deliberately separate from the loader role: dashboards must never hold CREATE TABLE/staging privileges."
  validation {
    condition = (
      can(regex("^[A-Z][A-Z0-9_]{0,63}$", var.reader_role_name)) &&
      !contains(["ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN", "USERADMIN", "PUBLIC"], var.reader_role_name)
    )
    error_message = "Use a dedicated non-admin role identifier, not a built-in Snowflake role."
  }
}

variable "reader_grantee_user" {
  type        = string
  description = "Existing Snowflake username to grant the read-only reader role to (this personal project's one trial login, same as loader_grantee_user - assumes a different, read-only role for dashboard sessions)."
  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_]{0,63}$", var.reader_grantee_user))
    error_message = "Provide an existing Snowflake username."
  }
}

provider "snowflake" {
  organization_name = var.organization_name
  account_name       = var.account_name
  authenticator      = "SNOWFLAKE_JWT"
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

resource "snowflake_schema" "serving" {
  database = snowflake_database.ticks.name
  name     = var.serving_schema_name
  comment  = "Published, queryable snapshot tables - the loader's transactional publish target (R4)."
}

resource "snowflake_account_role" "loader" {
  name    = var.loader_role_name
  comment = "Least-privilege loader role; routine sessions must not use ACCOUNTADMIN."
}

# Without this, the role exists but nothing can assume it - a real gap left
# over from the original R3 build, caught while preparing R4 (which actually
# needs to connect as this role). loader_grantee_user references an existing
# user (this trial's sole admin login), not a new identity created here.
resource "snowflake_grant_account_role" "loader_to_user" {
  role_name = snowflake_account_role.loader.name
  user_name = var.loader_grantee_user
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

# CREATE TABLE lets the loader itself run the idempotent CREATE TABLE IF NOT
# EXISTS DDL in snowflake/sql/ at startup (the loader owns any table it
# creates, so no further per-table grants are needed for its own DML).
resource "snowflake_grant_privileges_to_account_role" "serving_schema_privileges" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE"]
  on_schema {
    schema_name = snowflake_schema.serving.fully_qualified_name
  }
}

# Read-only role for R5 (the Streamlit historical view) - deliberately
# separate from the loader role, per the Snapshot Load Contract's
# role-separation requirement ("role-separated loader, transformer, and
# dashboard access, not ACCOUNTADMIN for application sessions"). This role
# never touches PUBLISH_STAGING or the stage; it can only SELECT from
# SERVING.
resource "snowflake_account_role" "reader" {
  name    = var.reader_role_name
  comment = "Read-only role for dashboard/analytics sessions; SELECT-only on SERVING, no write/staging privileges."
}

resource "snowflake_grant_account_role" "reader_to_user" {
  role_name = snowflake_account_role.reader.name
  user_name = var.reader_grantee_user
}

resource "snowflake_grant_privileges_to_account_role" "reader_warehouse_usage" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.ticks.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_database_usage" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.ticks.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_serving_schema_usage" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.serving.fully_qualified_name
  }
}

# SELECT on existing SERVING tables, plus future ones, so a new published
# dataset doesn't need a Terraform change to become dashboard-readable.
# on_schema_object all/future block (object_type_plural + in_schema)
# confirmed against the provider's own docs before writing, not guessed.
resource "snowflake_grant_privileges_to_account_role" "reader_serving_select_existing" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema           = snowflake_schema.serving.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_serving_select_future" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema           = snowflake_schema.serving.fully_qualified_name
    }
  }
}

# This root is applied exactly once, only after the bootstrap root has run
# and CloudFormation trust is real - so this precondition never actually
# blocks anything in ordinary use. Kept anyway as a defensive guard against
# applying this root too early (same reasoning as the Databricks workspace
# root's identical pattern). Verified live (2026-09-25): a failed
# precondition blocks the ENTIRE apply, not just this resource - which is
# exactly why the storage integration lives in the separate, ungated
# bootstrap root instead of here.
#
# snowflake_stage (generic) is deprecated AND gated behind a
# `preview_features_enabled` opt-in in provider 2.x (confirmed live
# 2026-09-25: "snowflake_stage_resource is currently a preview feature").
# snowflake_stage_external_s3 is the stable, non-preview AWS-specific
# replacement the deprecation warning pointed to - same argument names
# (name/database/schema/url/storage_integration/comment), confirmed against
# the provider's own docs before switching, not guessed again.
resource "snowflake_stage_external_s3" "publish" {
  name                = "${var.database_name}_PUBLISH_STAGE"
  database            = snowflake_database.ticks.name
  schema              = snowflake_schema.staging.name
  url                 = "s3://${var.bucket}/publish/"
  storage_integration = var.integration_name
  comment             = "Read-only external stage over completed export batches."

  lifecycle {
    precondition {
      condition     = var.trust_activation_confirmed
      error_message = "Activate and validate exact CloudFormation trust and apply the bootstrap root first."
    }
  }
}

# on_schema_object confirmed live 2026-09-25: this exact block planned
# correctly (object_type = "STAGE", object_name known-after-apply) before
# the stage itself failed on the preview-feature error above - only the
# stage resource type needed fixing, not this grant.
resource "snowflake_grant_privileges_to_account_role" "stage_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage_external_s3.publish.fully_qualified_name
  }
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

output "serving_schema_name" {
  value = snowflake_schema.serving.name
}

output "loader_role_name" {
  value = snowflake_account_role.loader.name
}

output "reader_role_name" {
  value = snowflake_account_role.reader.name
}

output "stage_name" {
  value = snowflake_stage_external_s3.publish.fully_qualified_name
}
