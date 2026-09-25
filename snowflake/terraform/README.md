# Snowflake Platform IaC

## Status

Two real live attempts (2026-09-25), both instructive, neither yet
successful end to end - the second attempt's fix is unrun. Terraform CLI
has not been available in the authoring environment this session, so
`terraform fmt`/`validate`/`test` have not been run locally either; `cfn-lint`
and YAML syntax checks have. This uses an existing Snowflake trial account
(`ILMRWBU-TX52777`, AWS `ap-southeast-1`) with `ACCOUNTADMIN` access for the
deploying identity - account/workspace creation is an explicit
prerequisite, not provisioned here.

**Attempt 1** failed immediately at provider configuration:
`snowflakedb/snowflake` provider 2.x does not read `SNOWFLAKE_ACCOUNT` (it
warns "environment variable is ignored" and requires an opt-in
`PROVIDER_CONFIGURATION_ACCOUNT_FALLBACK` experiment); it wants
`organization_name`/`account_name` as separate fields instead. Fixed by
adding those two variables; the workflow splits the existing
`SNOWFLAKE_ACCOUNT` secret into both at runtime rather than needing a new
secret.

**Attempt 2**, with that fix applied, got much further - Terraform
successfully planned all 9 resources (confirming `snowflake_warehouse`'s
`resource_monitor` argument and the `on_account_object`/`on_schema` grant
blocks all parse and plan correctly) - but then failed differently:
**a `lifecycle.precondition` failure blocks Terraform's entire plan/apply,
not just the gated resource.** The original design put the storage
integration, warehouse, database, schema, role, and grants in the same root
and apply as the trust-gated external stage, expecting the ungated
resources to still get created while only the stage was blocked (based on
how a *runtime* API failure behaves, e.g. the Databricks `landing` external
location incident). A precondition failure doesn't work that way: it's
evaluated during planning, before anything is created, so it blocked all 9
resources, not just the stage. Fixed by splitting into two Terraform roots -
see "Architecture" below - the same structural pattern the Databricks
`credential`/`workspace` split already used, now understood for the right
reason.

**Two things remain unverified against a real apply** (attempt 2 never
reached actual resource creation, so this is still open), flagged inline
where they occur:
1. `snowflake_storage_integration_aws.ticks.describe_output[0].iam_user_arn`/
   `external_id` (`bootstrap/main.tf`) - `describe_output` is documented as
   a "List of Object"; `[0]` indexing is the expected access pattern but
   untested live.
2. The `workspace/main.tf` `stage_usage` grant's
   `on_schema_object { object_type = "STAGE" ... }` block - inferred from
   the confirmed `on_account_object`/`on_schema` block shapes on the other
   grants, not confirmed directly for a stage object.

## Architecture

**Two Terraform roots, not one** - `bootstrap/` and `workspace/` - because
only the external stage needs to wait for IAM trust, and a
`lifecycle.precondition` failure blocks its *entire* root's apply, not just
the resource it's attached to (see "Status" above for how this was learned
live). Splitting means the trust-gated resource lives in its own root,
applied exactly once after trust is confirmed, so its precondition never
actually blocks anything in ordinary use; the ungated resources (storage
integration in one root, warehouse/database/schema/role/grants/stage in the
other) can always apply cleanly.

The whole sequence is automated by
[deploy-snowflake-platform.yaml](../../.github/workflows/deploy-snowflake-platform.yaml):

1. **`bootstrap`** - ensures the shared Terraform state backend (stack `00`,
   same one the Databricks roots use) exists, deploys
   [`07-snowflake-storage-role.yaml`](../../cloudformation/07-snowflake-storage-role.yaml)
   with `EnableSnowflakeTrust=false` (deny-all, same fail-closed pattern as
   the Databricks storage role), reads the role ARN and the project S3
   bucket name, and splits the `SNOWFLAKE_ACCOUNT` secret into
   `organization_name`/`account_name`.
2. **`snowflake-bootstrap`** - applies the `bootstrap/` root: creates only
   the storage integration. Nothing gates it; this apply always succeeds
   cleanly. Reads back the integration's generated
   `iam_user_arn`/`external_id`.
3. **`activate-trust`** - re-deploys stack `07` with `EnableSnowflakeTrust=true`
   and those generated values.
4. **`snowflake-workspace`** - applies the `workspace/` root, with
   `trust_activation_confirmed=true` and the bootstrap root's
   `integration_name` output: creates the warehouse, database, schema,
   loader role, grants, external stage, and the stage's grant. Applied
   exactly once, only now that trust is real.

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

Remote S3 backend, same bucket/lock-table as the Databricks roots, one state
key per root (`snowflake/bootstrap.tfstate`, `snowflake/workspace.tfstate`) -
for the same reason as Databricks: applies happen across separate CI runs/
jobs, and a disposable runner's local disk can't provide continuity.

Authentication is key-pair (JWT), not a password: the provider reads
`SNOWFLAKE_USER`/`SNOWFLAKE_PRIVATE_KEY` from the environment (GitHub secrets
in the workflow); `authenticator = "SNOWFLAKE_JWT"` is set explicitly in
both roots. **`SNOWFLAKE_ACCOUNT` is not read by provider 2.x** (confirmed
live 2026-09-25: it warns "environment variable is ignored" and requires an
opt-in `PROVIDER_CONFIGURATION_ACCOUNT_FALLBACK` experiment) - both roots
take explicit `organization_name`/`account_name` variables instead (the two
halves of the account identifier, e.g. `ILMRWBU`/`TX52777` split from
`ILMRWBU-TX52777`; not secrets). The workflow splits the existing
`SNOWFLAKE_ACCOUNT` secret into these two at runtime rather than needing a
new secret. Never pass the private key as a Terraform variable or commit
it; `*.p8`/`*.pem`/`rsa_key*` are gitignored.

## Offline Validation

For each root:

```bash
terraform init -backend=false -input=false
terraform fmt -check
terraform validate
terraform test
```

Not run this session (Terraform CLI unavailable in the authoring
environment) - run these before the next real apply, not just via CI.

## Pause and Full Destruction

No teardown workflow exists yet - deliberately deferred until a real apply
succeeds end to end, so cleanup logic matches what actually got created,
not what was designed. Manual teardown order until then: drop dependent
staging tables (R4, once they exist) -> `terraform destroy` the `workspace`
root (stage/grants/role/schema/database/warehouse/resource monitor) ->
`terraform destroy` the `bootstrap` root (storage integration) -> delete
stack `07` -> confirm no Snowflake objects remain in the trial account
before its own cleanup/expiry.
