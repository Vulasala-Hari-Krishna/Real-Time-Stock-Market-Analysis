# Snowflake Platform IaC

## Status

Prepared and offline-validated (`cfn-lint`, YAML syntax) plus **one real
first attempt (2026-09-25) that failed at provider configuration** - fixed,
not yet re-run. Terraform CLI has not been available in the authoring
environment this session, so `terraform fmt`/`validate`/`test` have not been
run either. This root uses an existing Snowflake trial account
(`ILMRWBU-TX52777`, AWS `ap-southeast-1`) with `ACCOUNTADMIN` access for the
deploying identity - account/workspace creation is an explicit prerequisite,
not provisioned here.

**Confirmed live** (from the failed first attempt, before it failed):
`snowflake_warehouse`'s `resource_monitor` argument and
`snowflake_account_role`/`snowflake_grant_privileges_to_account_role`'s
`on_account_object`/`on_schema` blocks parsed and planned without
complaint. Also confirmed by that same run: provider 2.x does **not** read
`SNOWFLAKE_ACCOUNT` (see "State and Authentication" below) - this was the
actual failure, now fixed with explicit `organization_name`/`account_name`
variables.

**Two things remain unverified against a real apply**, flagged inline where
they occur, in case the next real run needs a fix-iteration (the same
category of issue as the Databricks `databricks_permissions` argument
mistake earlier this migration):
1. `snowflake_storage_integration_aws.ticks.describe_output[0].iam_user_arn`/
   `external_id` - `describe_output` is documented as a "List of Object";
   `[0]` indexing is the expected access pattern but untested live (the first
   attempt never got this far).
2. The `snowflake_grant_privileges_to_account_role.stage_usage` resource's
   `on_schema_object { object_type = "STAGE" ... }` block - inferred from the
   confirmed `on_account_object`/`on_schema` block shapes on the other grants
   in this file, not confirmed directly for a stage object.

## Architecture

This is deliberately **one Terraform root**, not two like the Databricks
`credential`/`workspace` split, because only one resource (the external
stage) needs to wait for IAM trust - everything else (the storage
integration definition, warehouse, database, schema, role, grants) can be
created in the very first apply. The bootstrap is a two-apply sequence
within this one root, gated by a `lifecycle.precondition` on
`snowflake_stage.publish`, exactly mirroring how the Databricks `workspace`
root gated `databricks_external_location`/`databricks_service_principal` on
`trust_activation_confirmed`.

The whole sequence is automated by
[deploy-snowflake-platform.yaml](../../.github/workflows/deploy-snowflake-platform.yaml):

1. **`bootstrap`** - ensures the shared Terraform state backend (stack `00`,
   same one the Databricks roots use) exists, deploys
   [`07-snowflake-storage-role.yaml`](../../cloudformation/07-snowflake-storage-role.yaml)
   with `EnableSnowflakeTrust=false` (deny-all, same fail-closed pattern as
   the Databricks storage role), reads the role ARN and the project S3
   bucket name.
2. **`platform-bootstrap`** - first `terraform apply`, with
   `trust_activation_confirmed=false`. Creates the storage integration,
   warehouse, database, schema, loader role, and warehouse/database/schema
   grants. **The external stage's precondition deliberately fails this
   apply** - that failure is expected, not a bug; the job step uses
   `continue-on-error: true` and then verifies the storage integration's
   outputs are non-empty before proceeding (to distinguish "the stage was
   correctly blocked" from "something else actually broke").
3. **`activate-trust`** - re-deploys stack `07` with `EnableSnowflakeTrust=true`
   and the storage integration's generated `iam_user_arn`/`external_id`.
4. **`platform-finish`** - second `terraform apply`, with
   `trust_activation_confirmed=true`. Everything else is already in state
   (no-op); the stage and its `USAGE` grant are created now that trust is
   real.

## Required Decisions

- Least-privilege loader role (`loader_role_name`, default
  `STOCK_MARKET_DEV_LOADER`) is separate from the `ACCOUNTADMIN` identity
  Terraform itself runs as - matches
  [the Snowflake scoped instructions](../../.github/instructions/snowflake.instructions.md)'s
  "not `ACCOUNTADMIN` for application sessions." R4 (the snapshot loader)
  should authenticate as this role, not as the setup identity.
- One `XSMALL` warehouse (`auto_suspend=60`, `auto_resume=true`), one
  resource monitor (`credit_quota`, default 10, capped at 25 by variable
  validation) attached to it - per-warehouse credit cap, not an account-wide
  spending cap. Resource monitors do not cap Snowpipe/serverless charges;
  none are enabled here.
- The storage integration and external stage are scoped to `s3://<bucket>/publish/`
  only, matching the already-existing (previously unattached)
  `SnowflakePublishPolicy` in
  [`05-hybrid-access.yaml`](../../cloudformation/05-hybrid-access.yaml) -
  read-only, no access to `lakehouse/`, `landing/`, or `checkpoints/hybrid/`.

## State and Authentication

Remote S3 backend, same bucket/lock-table as the Databricks roots
(`backend "s3" { key = "snowflake/terraform.tfstate" }`), for the same
reason: the two-apply sequence needs state continuity that a disposable CI
runner's local disk can't provide.

Authentication is key-pair (JWT), not a password: the provider reads
`SNOWFLAKE_USER`/`SNOWFLAKE_PRIVATE_KEY` from the environment (GitHub secrets
in the workflow); `authenticator = "SNOWFLAKE_JWT"` is set explicitly in
`main.tf`. **`SNOWFLAKE_ACCOUNT` is not read by provider 2.x** (confirmed
live 2026-09-25: it warns "environment variable is ignored" and requires an
opt-in `PROVIDER_CONFIGURATION_ACCOUNT_FALLBACK` experiment) - `main.tf`
takes explicit `organization_name`/`account_name` variables instead (the two
halves of the account identifier, e.g. `ILMRWBU`/`TX52777` split from
`ILMRWBU-TX52777`; not secrets). The workflow splits the existing
`SNOWFLAKE_ACCOUNT` secret into these two at runtime rather than needing a
new secret. Never pass the private key as a Terraform
variable or commit it; `*.p8`/`*.pem`/`rsa_key*` are gitignored.

## Offline Validation

```bash
terraform init -backend=false -input=false
terraform fmt -check
terraform validate
terraform test
```

Not run this session (Terraform CLI unavailable in the authoring
environment) - run these before the next real apply, not just via CI.

## Pause and Full Destruction

No teardown workflow exists yet for this root - deliberately deferred until
after the first real apply confirms (or corrects) the two unverified schema
points above, to avoid needing the same fix in two places. Manual teardown
order until then: drop dependent staging tables (R4, once they exist) ->
`terraform destroy` this root (destroys the stage/grants/role/schema/
database/warehouse/resource monitor/storage integration) -> delete stack
`07` -> confirm no Snowflake objects remain in the trial account before its
own cleanup/expiry.
