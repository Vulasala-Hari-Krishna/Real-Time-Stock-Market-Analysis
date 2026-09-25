mock_provider "snowflake" {}

variables {
  bucket             = "offline-test-bucket"
  storage_role_arn    = "arn:aws:iam::123456789012:role/stock-market-pipeline-snowflake-storage-dev"
  integration_name   = "PORTFOLIO_DEV_PUBLISH_INTEGRATION"
  organization_name  = "ILMRWBU"
  account_name       = "TX52777"
}

run "storage_integration_scoped_to_publish_only" {
  command = apply

  assert {
    condition     = snowflake_storage_integration_aws.ticks.storage_allowed_locations == toset(["s3://offline-test-bucket/publish/"])
    error_message = "The storage integration must be scoped to publish/ only."
  }
  assert {
    condition     = snowflake_storage_integration_aws.ticks.storage_aws_role_arn == var.storage_role_arn
    error_message = "The storage integration must trust the exact role from stack 07."
  }
}
