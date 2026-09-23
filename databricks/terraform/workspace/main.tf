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
  description = "Existing identity-federated AWS Databricks workspace with a UC metastore."
  validation {
    condition     = can(regex("^https://[A-Za-z0-9.-]+\\.cloud\\.databricks\\.com/?$", var.workspace_host)) && trimsuffix(lower(var.workspace_host), "/") != "https://accounts.cloud.databricks.com"
    error_message = "Use an HTTPS AWS Databricks workspace URL."
  }
}

variable "catalog" {
  type        = string
  description = "New dedicated catalog; do not point this configuration at shared data."
  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{0,30}$", var.catalog))
    error_message = "Use a lowercase identifier of at most 31 characters."
  }
}

variable "schema_prefix" {
  type        = string
  default     = "stocks"
  description = "Prefix of the bronze, silver and gold schemas; matches bundle input."
  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{0,30}$", var.schema_prefix))
    error_message = "Use a lowercase identifier of at most 31 characters."
  }
}

variable "bucket" {
  type        = string
  description = "Existing S3 bucket managed by CloudFormation."
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket))
    error_message = "Provide a bucket name, not an S3 URI or prefix."
  }
}

variable "credential_name" {
  type        = string
  description = "Existing credential from the bootstrap root after exact IAM trust activation."
  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{0,62}$", var.credential_name))
    error_message = "Provide the bootstrap credential identifier."
  }
}

variable "trust_activation_confirmed" {
  type        = bool
  default     = false
  description = "Set true only after updating CloudFormation trust and validating the credential."
}

variable "spark_version" {
  type        = string
  description = "Verified UC-compatible Spark 3.5/Python 3.11+ runtime ID; also pass to the bundle."
  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+[A-Za-z0-9._-]+$", var.spark_version))
    error_message = "Provide an explicit runtime ID, not an automatic/latest selector."
  }
}

variable "node_type_id" {
  type        = string
  description = "Approved region-specific AWS node type; also pass to the bundle."
  validation {
    condition     = can(regex("^[A-Za-z0-9.-]+$", var.node_type_id))
    error_message = "Provide the exact approved node type ID."
  }
}

variable "max_dbus_per_hour" {
  type        = number
  description = "Per-cluster DBU/hour policy bound, not an AWS or account-wide spending cap."
  validation {
    condition     = var.max_dbus_per_hour > 0 && var.max_dbus_per_hour <= 10
    error_message = "Choose a positive DBU/hour bound no greater than 10 for this personal slice."
  }
}

variable "deployment_group" {
  type        = string
  description = "Existing account-level group allowed to deploy/use this job policy; not admins or users."
  validation {
    condition     = length(trimspace(var.deployment_group)) > 0 && !contains(["admins", "users"], lower(var.deployment_group))
    error_message = "Use a dedicated deployment group, not a workspace-wide group."
  }
}

provider "databricks" {
  host = var.workspace_host
}

locals {
  name = "${var.catalog}_${var.schema_prefix}"
  locations = merge({
    landing = {
      path       = "landing/ticks"
      read_only  = true
      privileges = ["READ_FILES"]
    }
    checkpoints = {
      path       = "checkpoints/hybrid/${var.catalog}/${var.schema_prefix}"
      read_only  = false
      privileges = ["READ_FILES", "WRITE_FILES"]
    }
    managed = {
      path       = "lakehouse/managed/${var.catalog}"
      read_only  = false
      privileges = []
    }
    }, {
    for layer in ["bronze", "silver", "gold"] : layer => {
      path       = "lakehouse/${layer}/${var.catalog}/${var.schema_prefix}"
      read_only  = false
      privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
    }
  })
}

resource "databricks_service_principal" "runtime" {
  display_name               = "${local.name}_ticks_runtime"
  active                     = true
  workspace_access           = true
  allow_cluster_create       = false
  allow_instance_pool_create = false
  databricks_sql_access      = false
  disable_as_user_deletion   = false

  lifecycle {
    precondition {
      condition     = var.trust_activation_confirmed
      error_message = "Complete the credential/IAM trust bootstrap before applying workspace resources."
    }
  }
}

resource "databricks_external_location" "ticks" {
  for_each = local.locations

  name               = "${local.name}_${each.key}"
  url                = "s3://${var.bucket}/${each.value.path}"
  credential_name    = var.credential_name
  read_only          = each.value.read_only
  isolation_mode     = "ISOLATION_MODE_ISOLATED"
  skip_validation    = false
  fallback           = false
  enable_file_events = false
  force_destroy      = false
  force_update       = false
  comment            = "Hybrid ${each.key}; explicit prefix, no automatic file-event services."

  lifecycle {
    precondition {
      condition     = var.trust_activation_confirmed
      error_message = "Activate and validate exact CloudFormation trust first."
    }
  }
}

resource "databricks_catalog" "ticks" {
  name                           = var.catalog
  storage_root                   = databricks_external_location.ticks["managed"].url
  isolation_mode                 = "ISOLATED"
  enable_predictive_optimization = "DISABLE"
  force_destroy                  = false
  comment                        = "Dedicated hybrid quote catalog; external table paths are separate from managed storage."
}

resource "databricks_schema" "layers" {
  for_each = toset(["bronze", "silver", "gold"])

  catalog_name                   = databricks_catalog.ticks.name
  name                           = "${var.schema_prefix}_${each.key}"
  enable_predictive_optimization = "DISABLE"
  force_destroy                  = false
  comment                        = "Hybrid ${each.key}; table definitions belong to the runtime job, not Terraform."
}

resource "databricks_grant" "catalog_runtime" {
  catalog    = databricks_catalog.ticks.name
  principal  = databricks_service_principal.runtime.application_id
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "schemas_runtime" {
  for_each = databricks_schema.layers

  schema     = each.value.id
  principal  = databricks_service_principal.runtime.application_id
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
}

resource "databricks_grant" "locations_runtime" {
  for_each = { for name, location in local.locations : name => location if length(location.privileges) > 0 }

  external_location = databricks_external_location.ticks[each.key].name
  principal         = databricks_service_principal.runtime.application_id
  privileges        = each.value.privileges
}

resource "databricks_cluster_policy" "ticks" {
  name                  = "${local.name}_manual_ticks"
  description           = "Single-node, job-only quote processing; no compute is created by this resource."
  max_clusters_per_user = 1
  definition = jsonencode({
    "cluster_type"                                = { type = "fixed", value = "job" }
    "spark_version"                               = { type = "fixed", value = var.spark_version }
    "node_type_id"                                = { type = "fixed", value = var.node_type_id }
    "driver_node_type_id"                         = { type = "fixed", value = var.node_type_id }
    "num_workers"                                 = { type = "fixed", value = 0 }
    "data_security_mode"                          = { type = "fixed", value = "SINGLE_USER" }
    "single_user_name"                            = { type = "fixed", value = databricks_service_principal.runtime.application_id }
    "spark_conf.spark.databricks.cluster.profile" = { type = "fixed", value = "singleNode" }
    "spark_conf.spark.master"                     = { type = "fixed", value = "local[*]" }
    "custom_tags.ResourceClass"                   = { type = "fixed", value = "SingleNode" }
    "custom_tags.Project"                         = { type = "fixed", value = "stock-market-hybrid" }
    "custom_tags.Environment"                     = { type = "fixed", value = "dev" }
    "dbus_per_hour"                               = { type = "range", maxValue = var.max_dbus_per_hour }
    "aws_attributes.availability"                 = { type = "fixed", value = "ON_DEMAND" }
    "aws_attributes.instance_profile_arn"         = { type = "forbidden" }
    "instance_pool_id"                            = { type = "forbidden" }
    "driver_instance_pool_id"                     = { type = "forbidden" }
    "autoscale.min_workers"                       = { type = "forbidden" }
    "autoscale.max_workers"                       = { type = "forbidden" }
  })
}

resource "databricks_permissions" "policy_use" {
  cluster_policy_id = databricks_cluster_policy.ticks.id

  access_control {
    service_principal_name = databricks_service_principal.runtime.application_id
    permission_level       = "CAN_USE"
  }
  access_control {
    group_name       = var.deployment_group
    permission_level = "CAN_USE"
  }
}

output "bundle_variables" {
  description = "Non-secret BUNDLE_VAR inputs; not proof of live job validation."
  value = {
    catalog                  = databricks_catalog.ticks.name
    schema_prefix            = var.schema_prefix
    bucket                   = var.bucket
    spark_version            = var.spark_version
    node_type_id             = var.node_type_id
    cluster_policy_id        = databricks_cluster_policy.ticks.id
    run_as_service_principal = databricks_service_principal.runtime.application_id
  }
}

output "external_locations" {
  description = "Dedicated governed paths, including a distinct catalog-managed root."
  value       = { for name, location in databricks_external_location.ticks : name => location.url }
}