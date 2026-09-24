mock_provider "databricks" {}

variables {
  workspace_host             = "https://dbc-offline.cloud.databricks.com"
  catalog                    = "portfolio_dev"
  schema_prefix              = "stocks"
  bucket                     = "offline-test-bucket"
  credential_name            = "stocks_dev_storage"
  trust_activation_confirmed = true
}

run "unconfirmed_trust_blocks_platform_creation" {
  command = plan
  variables {
    trust_activation_confirmed = false
  }
  expect_failures = [
    databricks_service_principal.runtime,
    databricks_external_location.ticks,
  ]
}

run "platform_boundaries" {
  command = apply

  assert {
    condition = (
      !databricks_service_principal.runtime.allow_cluster_create &&
      !databricks_service_principal.runtime.allow_instance_pool_create &&
      !databricks_service_principal.runtime.databricks_sql_access &&
      !databricks_service_principal.runtime.disable_as_user_deletion
    )
    error_message = "Runtime must not receive unrestricted compute, pools or SQL access; explicit deletion must remove the workspace identity."
  }
  assert {
    condition = alltrue([
      for location in databricks_external_location.ticks :
      !location.skip_validation && !location.fallback && !location.enable_file_events &&
      !location.force_destroy && !location.force_update &&
      location.isolation_mode == "ISOLATION_MODE_ISOLATED"
    ])
    error_message = "All external locations require validation, isolation and non-destructive defaults."
  }
  assert {
    condition = (
      databricks_external_location.ticks["landing"].read_only &&
      databricks_external_location.ticks["landing"].url == "s3://offline-test-bucket/landing/ticks" &&
      toset(databricks_grant.locations_runtime["landing"].privileges) == toset(["READ_FILES"])
    )
    error_message = "Runtime must only read the consumer's raw tick prefix."
  }
  assert {
    condition = (
      databricks_external_location.ticks["managed"].url == "s3://offline-test-bucket/lakehouse/managed/portfolio_dev" &&
      !contains(keys(databricks_grant.locations_runtime), "managed") &&
      databricks_catalog.ticks.storage_root == databricks_external_location.ticks["managed"].url
    )
    error_message = "Managed storage must not overlap external table paths or be granted as raw file access."
  }
  assert {
    condition = alltrue([
      for layer in ["bronze", "silver", "gold"] :
      databricks_external_location.ticks[layer].url == "s3://offline-test-bucket/lakehouse/${layer}/portfolio_dev/stocks" &&
      databricks_schema.layers[layer].name == "stocks_${layer}" &&
      !databricks_schema.layers[layer].force_destroy
    ])
    error_message = "Table locations and schema names must match the implemented job contract."
  }
  assert {
    condition = (
      databricks_external_location.ticks["checkpoints"].url == "s3://offline-test-bucket/checkpoints/hybrid/portfolio_dev/stocks" &&
      toset(databricks_grant.locations_runtime["checkpoints"].privileges) == toset(["READ_FILES", "WRITE_FILES"])
    )
    error_message = "Checkpoint grants must stay in the job-specific prefix."
  }
  assert {
    condition = (
      databricks_catalog.ticks.isolation_mode == "ISOLATED" &&
      databricks_catalog.ticks.enable_predictive_optimization == "DISABLE" &&
      !databricks_catalog.ticks.force_destroy &&
      toset(databricks_grant.catalog_runtime.privileges) == toset(["USE_CATALOG"])
    )
    error_message = "Catalog must not enable automatic compute, broad grants or cascade destruction."
  }
  assert {
    condition = (
      output.bundle_variables.run_as_service_principal == databricks_service_principal.runtime.application_id &&
      output.bundle_variables.catalog == var.catalog
    )
    error_message = "Bundle inputs must refer to resources managed by this root."
  }
}