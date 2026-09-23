mock_provider "databricks" {}

variables {
  workspace_host   = "https://dbc-offline.cloud.databricks.com"
  credential_name  = "stocks_dev_storage"
  storage_role_arn = "arn:aws:iam::123456789012:role/stock-market-pipeline-uc-storage-dev"
}

run "bootstrap_is_isolated_and_not_force_deleted" {
  command = plan

  assert {
    condition     = databricks_storage_credential.ticks.skip_validation
    error_message = "Bootstrap must not require a usable role before generating the external ID."
  }
  assert {
    condition     = databricks_storage_credential.ticks.isolation_mode == "ISOLATION_MODE_ISOLATED"
    error_message = "Credential must be scoped to the current workspace."
  }
  assert {
    condition     = !databricks_storage_credential.ticks.force_destroy && !databricks_storage_credential.ticks.force_update
    error_message = "Credential updates/destruction must respect dependent locations."
  }
  assert {
    condition     = databricks_storage_credential.ticks.aws_iam_role[0].role_arn == var.storage_role_arn
    error_message = "Credential must use only the explicitly supplied role."
  }
}

run "activate_validation_without_replacing_other_resources" {
  command = plan
  variables {
    validate_storage_access = true
  }
  assert {
    condition     = !databricks_storage_credential.ticks.skip_validation
    error_message = "Validation must be re-enabled after exact trust activation."
  }
}

run "reject_non_workspace_host" {
  command = plan
  variables {
    workspace_host = "https://accounts.cloud.databricks.com"
  }
  expect_failures = [var.workspace_host]
}