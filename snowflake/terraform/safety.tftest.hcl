mock_provider "snowflake" {}

variables {
  bucket                     = "offline-test-bucket"
  storage_role_arn           = "arn:aws:iam::123456789012:role/stock-market-pipeline-snowflake-storage-dev"
  database_name              = "PORTFOLIO_DEV"
  schema_name                = "PUBLISH_STAGING"
  warehouse_name              = "PORTFOLIO_DEV_WH"
  loader_role_name           = "PORTFOLIO_DEV_LOADER"
  credit_quota               = 10
  trust_activation_confirmed = true
}

run "unconfirmed_trust_blocks_stage_creation" {
  command = plan
  variables {
    trust_activation_confirmed = false
  }
  expect_failures = [
    snowflake_stage.publish,
  ]
}

run "reject_builtin_role_name" {
  command = plan
  variables {
    loader_role_name = "ACCOUNTADMIN"
  }
  expect_failures = [var.loader_role_name]
}

run "platform_boundaries" {
  command = apply

  assert {
    condition     = snowflake_storage_integration_aws.ticks.storage_allowed_locations == toset(["s3://offline-test-bucket/publish/"])
    error_message = "The storage integration must be scoped to publish/ only."
  }
  assert {
    condition = (
      snowflake_warehouse.ticks.warehouse_size == "XSMALL" &&
      snowflake_warehouse.ticks.auto_suspend == 60 &&
      snowflake_warehouse.ticks.auto_resume == true &&
      snowflake_warehouse.ticks.resource_monitor == snowflake_resource_monitor.ticks.fully_qualified_name
    )
    error_message = "The warehouse must be a small, short-auto-suspend warehouse under the resource monitor."
  }
  assert {
    condition     = snowflake_resource_monitor.ticks.credit_quota == 10
    error_message = "The resource monitor must enforce the configured credit quota."
  }
  assert {
    condition     = snowflake_account_role.loader.name != "ACCOUNTADMIN"
    error_message = "Routine sessions must use a dedicated non-admin role, not ACCOUNTADMIN."
  }
  assert {
    condition = (
      toset(snowflake_grant_privileges_to_account_role.warehouse_usage.privileges) == toset(["USAGE"]) &&
      toset(snowflake_grant_privileges_to_account_role.database_usage.privileges) == toset(["USAGE"])
    )
    error_message = "The loader role must receive USAGE only on the warehouse and database."
  }
  assert {
    condition     = snowflake_stage.publish.url == "s3://offline-test-bucket/publish/"
    error_message = "The stage must point only at the publish/ prefix."
  }
}
