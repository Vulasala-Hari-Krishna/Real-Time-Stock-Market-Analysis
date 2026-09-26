terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.88.0"
    }
  }

  # Remote state (see credential/main.tf for why): keeps this root's created
  # objects (catalog/schemas/external locations/service principal) durable
  # across separate CI runs instead of a disposable runner's local disk.
  backend "s3" {
    key     = "workspace/terraform.tfstate"
    encrypt = true
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

provider "databricks" {
  host = var.workspace_host
}

locals {
  name = "${var.catalog}_${var.schema_prefix}"
  locations = merge({
    # Broadened from "landing/ticks" to the "landing" parent prefix (R6):
    # fundamentals now lands under landing/fundamentals/, and this covers
    # any future landing/<dataset>/ without a new external location per
    # dataset - a read-only location already grants only READ_FILES, so
    # widening its prefix doesn't grant any new write capability.
    landing = {
      path       = "landing"
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

# Least-privilege identity holding this slice's Unity Catalog grants. The
# bundle does not currently set `run_as` to this principal (see the note in
# databricks/databricks.yml): doing so needs its "Service Principal User"
# role granted to the deploying identity, which this provider version only
# exposes through an account-level rule-set resource of unverified
# workspace-token compatibility. The job runs as the deploying admin instead;
# this principal/grants stay provisioned for when that's revisited.
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

# No cluster policy: this workspace runs on serverless compute only (Free Edition
# and serverless-first workspaces have no classic/job clusters to attach a
# compute policy to). Cost/scope control comes from the platform's own serverless
# job/task quotas instead of a per-cluster DBU/hour bound.

output "bundle_variables" {
  description = "Non-secret BUNDLE_VAR inputs; not proof of live job validation."
  value = {
    catalog                  = databricks_catalog.ticks.name
    schema_prefix            = var.schema_prefix
    bucket                   = var.bucket
    run_as_service_principal = databricks_service_principal.runtime.application_id
  }
}

output "external_locations" {
  description = "Dedicated governed paths, including a distinct catalog-managed root."
  value       = { for name, location in databricks_external_location.ticks : name => location.url }
}