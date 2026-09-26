mock_provider "snowflake" {}

variables {
  bucket                     = "offline-test-bucket"
  database_name              = "PORTFOLIO_DEV"
  schema_name                = "PUBLISH_STAGING"
  serving_schema_name        = "SERVING"
  warehouse_name              = "PORTFOLIO_DEV_WH"
  loader_role_name           = "PORTFOLIO_DEV_LOADER"
  credit_quota               = 10
  integration_name           = "PORTFOLIO_DEV_PUBLISH_INTEGRATION"
  organization_name          = "ILMRWBU"
  account_name               = "TX52777"
  loader_grantee_user        = "TESTUSER"
  trust_activation_confirmed = true
}

run "unconfirmed_trust_blocks_entire_apply" {
  command = plan
  variables {
    trust_activation_confirmed = false
  }
  expect_failures = [
    snowflake_stage_external_s3.publish,
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
    condition     = snowflake_stage_external_s3.publish.url == "s3://offline-test-bucket/publish/"
    error_message = "The stage must point only at the publish/ prefix."
  }
  assert {
    condition     = snowflake_stage_external_s3.publish.storage_integration == var.integration_name
    error_message = "The stage must reference the bootstrap root's storage integration."
  }
  assert {
    condition = (
      snowflake_grant_account_role.loader_to_user.role_name == snowflake_account_role.loader.name &&
      snowflake_grant_account_role.loader_to_user.user_name == var.loader_grantee_user
    )
    error_message = "The loader role must actually be granted to a user, or nothing can assume it."
  }
  assert {
    condition = (
      snowflake_schema.serving.name == var.serving_schema_name &&
      toset(snowflake_grant_privileges_to_account_role.serving_schema_privileges.privileges) == toset(["USAGE", "CREATE TABLE"])
    )
    error_message = "The loader role must be able to create and use its own serving-table objects in a schema separate from staging."
  }
}
