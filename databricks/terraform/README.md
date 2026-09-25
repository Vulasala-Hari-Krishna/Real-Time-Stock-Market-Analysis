# Databricks Platform IaC

## Status and Ownership

Prepared and locally validated; **no real plan/apply, resources, credentials, or
compute were created**. These configurations use an existing AWS Databricks
workspace with an assigned Unity Catalog metastore and identity federation.
Workspace/account creation, networking, metastore assignment, and account-level
administration are explicit prerequisites, not silently provisioned services.

| Owner | Resources |
|-------|-----------|
| [S3 CloudFormation](../../cloudformation/01-s3-datalake.yaml) | Existing bucket, isolated paths, encryption/TLS and retention controls |
| [Access CloudFormation](../../cloudformation/05-hybrid-access.yaml) | Unattached local, Databricks, and Snowflake S3 policies |
| [Role CloudFormation](../../cloudformation/06-databricks-storage-role.yaml) | Unity Catalog IAM role, S3 policy attachment and exact trust |
| [Credential Terraform root](credential/main.tf) | One workspace-isolated storage credential and generated trust outputs |
| [Workspace Terraform root](workspace/main.tf) | Runtime service principal, external locations, catalog/schemas, grants (serverless compute; no cluster policy) |
| [Deployment bundle](../databricks.yml) | Manual job and packaged application code, not platform objects |
| Job runtime | Five Delta table definitions and data; not owned by Terraform |

Neither Terraform root uses the AWS provider or creates clusters/jobs/SQL
warehouses. The two roots have separate
states: do not merge them or use `-target` as a bootstrap/destruction mechanism.

Terraform 1.9+ is required; local checks used 1.9.8. Databricks provider 1.88.0 is
pinned with checked-in dependency lock files. Upgrade deliberately after schema
and safety tests, not implicitly during deployment.

## Required Decisions

- Confirm AWS account/region and existing S3 stack names. **Verified on Free
  Edition** (2026-09-24): a Unity Catalog storage credential + external location
  against a self-owned S3 bucket passed a full Test Connection (Read/List/Write/
  Delete/Path Exists/Assume Role/Self-Assume Role/External ID Condition), and a
  serverless notebook wrote and read back a real Delta table through it. Free
  Edition has no classic compute at all, so this root creates no cluster policy
  and the job runs on serverless compute instead; cost/scope control comes from
  the platform's serverless job/task quotas, not a per-cluster DBU/hour bound.
- Select a unique dev catalog and schema prefix, matching the bundle input.
- Use separate platform-setup and job-runtime identities. The setup identity needs
  sufficient workspace/Unity Catalog administration privileges to create credentials,
  locations/catalogs/schemas, a service principal, policy, and grants. Routine jobs
  use only the newly created non-admin runtime service principal.
- **Correction (verified live 2026-09-25):** deploying a job with `run_as:
  service_principal_name` requires the deploying identity to hold the
  "Service Principal User" role on that exact service principal, or
  `databricks bundle deploy` fails with a 403 `PERMISSION_DENIED` even for a
  workspace admin — admin status alone does not grant it. This is a
  **workspace-level** permission (`databricks_permissions` with
  `service_principal_id`, `permission_level = "CAN_USE"`), independent of
  account-console access, so it applies on Free Edition too; an earlier
  version of this doc incorrectly claimed Free Edition's lack of account
  console meant this step could be skipped. `workspace/main.tf` grants it
  automatically to `var.deployer_user_name` (the workspace login that runs
  `bundle deploy`) via the `runtime_service_principal_user` resource.
- Review inherited group entitlements: explicit false flags do not negate inherited
  cluster-create/admin access. Verify the runtime principal cannot bypass its grants.
- Establish state storage/locking and an explicit spending/deployment window.
  No authorization to execute applies or billable jobs is inferred from these files.

## State and Authentication

Both roots use a **remote S3 backend** (`backend "s3" {}` in each `main.tf`),
not local state. This is required, not a preference: the `credential` root is
applied twice (bootstrap, then again after CloudFormation trust activation)
and must remember the storage credential it already created between those
applies — a disposable CI runner's local disk can't provide that. The backend
bucket and DynamoDB lock table are created by
[cloudformation/00-terraform-state.yaml](../../cloudformation/00-terraform-state.yaml),
deliberately separate from the `01-s3-datalake` data bucket so that emptying
the data bucket at full teardown never touches Terraform state. Backend values
(bucket/region/table) are supplied at `terraform init` time via
`-backend-config`, since the bucket name includes the AWS account ID and isn't
known when these files are authored. `-backend=false` (used for offline
`terraform validate`/`terraform test`, see below) skips the backend entirely
and is unaffected.

The full bootstrap-through-workspace-objects sequence is automated end to end
by [deploy-databricks-platform.yaml](../../.github/workflows/deploy-databricks-platform.yaml)
(and reversed by `teardown-databricks-platform.yaml`): CloudFormation stack 06's
role ARN and the credential root's generated principal/external-ID are passed
between jobs as GitHub Actions outputs, not copied by hand. Run it from the
Actions tab; it needs repository secrets `AWS_DEPLOY_ROLE_ARN` (already used by
the other AWS workflows), `DATABRICKS_HOST`, and `DATABRICKS_TOKEN` (a personal
access token — sufficient for a solo Free Edition workspace; see the account-
console note above for why an OAuth service principal isn't needed here). For a
human-approval gate before each apply, add required reviewers to the GitHub
Environment named by the workflow's `environment` input, in repo Settings ->
Environments — the same mechanism this repo already uses for the AWS-only
workflows, applied here to every job that runs `terraform apply`/`destroy`.

Use Databricks unified authentication via a securely configured CLI profile or
supported workload identity/OAuth environment variables when running these
roots outside that workflow (e.g. `terraform plan` locally against the same
remote state to review a change before triggering the workflow). Do not pass
secrets in Terraform variables, source files, command history, or chat.
`workspace_host` must be the workspace URL, not `accounts.cloud.databricks.com`.

Supply non-secret values through `TF_VAR_<name>` or ignored `*.tfvars`. Treat saved
plans and state as sensitive even when this module creates no client secrets.
Commit `.terraform.lock.hcl`, not `.terraform/`, state, plans, crash logs, or tfvars.
The offline validation commands below use `-backend=false` and never touch the
remote backend or real credentials.

## Bootstrap Sequence

**This entire sequence is automated by
[deploy-databricks-platform.yaml](../../.github/workflows/deploy-databricks-platform.yaml)**
(steps 1-5 below map to its `bootstrap` / `credential-bootstrap` /
`activate-trust` / `credential-validate` / `workspace-objects` jobs). Run the
workflow rather than these steps by hand; they're kept here as the reference
description of what each stage actually does, for review and troubleshooting.

All apply/deploy steps below need explicit authorization and reviewed change sets
or Terraform plans. Use the same AWS project/environment and same Databricks
workspace throughout; the current bundle/policy is dev-only.

1. Deploy/update only the S3 and hybrid-access stacks (`01`, `05`) after reviewing
   existing data/lifecycle policies. Deploy `06` with
   `EnableUnityCatalogTrust=false` (default). It attaches the prepared Databricks
   storage policy but denies all role assumption; no placeholder role is trusted.
   Existing legacy deployment scripts do not include these optional stacks and
   must not be used as a substitute for the staged sequence.
2. In `credential/`, set `workspace_host`, `credential_name`, and
   `storage_role_arn` to the actual `06` output. Keep
   `validate_storage_access=false` only for this bootstrap. Initialize, plan,
   review, and apply. The storage credential uses `skip_validation` to obtain its
   generated IAM principal and external ID before the role is usable. Keep the
   credential owned by the privileged setup identity; do not grant it to the job.
3. Read `unity_catalog_principal_arn` and `unity_catalog_external_id` outputs from
   the credential root. Update stack `06` with those exact values and
   `EnableUnityCatalogTrust=true`. The trust policy permits only the returned
   principal and the role itself, both conditioned on the external ID. The account
   root in the self-assume statement is narrowed by exact `aws:PrincipalArn`; it
   does not allow arbitrary account identities. Self-assume permission is scoped
   to that role ARN and it is not a cluster instance profile.
4. Set `validate_storage_access=true` in `credential/`, plan/review/apply, then
   explicitly validate the credential in Databricks. A successful Terraform update
   alone is not proof that STS/S3 works; IAM propagation and platform validation
   behavior must be checked. Never leave validation bypassed as a workaround.
5. In `workspace/`, supply the variables declared in [main.tf](workspace/main.tf):
   workspace host, catalog, schema prefix, bucket, credential name, and
   `trust_activation_confirmed=true`. Plan/review/apply. This is a human
   confirmation gate, not an AWS trust probe.
   External-location validation is enabled and fallback/file-event services are
   disabled; access failures stop the apply rather than weakening grants.
6. Inspect the created objects and grants. The runtime has read-only file access
   to `landing/ticks`; read/write to its checkpoint and external table paths;
   catalog use plus schema-local table creation/read/write. It has no grants to
   the storage credential, raw producer backups, other catalogs, or `publish/`.
   The underlying role policy is broader for future publishing, but is not exposed
   as an instance profile. UC grants enforce this job's narrower use.
7. Transfer the non-secret `bundle_variables` output to corresponding
   `BUNDLE_VAR_<name>` values and use the [job guide](../README.md). The
   deploying identity's Service Principal User grant (a workspace-level
   permission, not account-level) is created automatically by this root's
   `runtime_service_principal_user` resource — no separate manual step.
   Restrict the bundle deployment directory and confirm the runtime can read
   the installed wheel. Run bundle validation before an explicitly authorized
   deployment and small billable run.

For either Terraform root, use a reviewed saved plan, not auto-approve:

```bash
terraform init -input=false
terraform plan -out=review.tfplan
terraform show review.tfplan
terraform apply review.tfplan
```

Do not change credential/catalog names or storage locations casually: replacements
can change external IDs or detach data. Review all replacements/deletions and
existing object imports explicitly; never import a shared object merely to bypass
a name collision. The credentials/grants/policy must have one IaC owner.

## Storage and Cost Boundaries

The catalog has an explicit managed root under `lakehouse/managed/<catalog>`.
This is separate from job-owned external tables under
`lakehouse/<bronze|silver|gold>/<catalog>/<prefix>`. No raw-file grants are given
to the job on that managed root. The privileged setup identity owns the new
location and must be permitted to create managed storage there.

Catalog, locations, and credential are isolated to the current workspace.
Predictive optimization and managed file events are disabled. Catalog/schema
deletion does not cascade (`force_destroy=false`); the user must confirm and remove
dependent tables first. This supports explicit full destruction, not an indefinite
retention rule or automatic data deletion on ordinary updates.

There is no cluster policy: the job runs on serverless compute, governed by the
platform's own serverless job/task quotas rather than a per-cluster DBU/hour
bound or instance-profile restriction. The bundle still owns the 30-minute
timeout, no retries/queue, and no schedule.

## Offline Validation

For each root, run these commands before authorization:

```bash
terraform init -backend=false -input=false
terraform fmt -check
terraform validate
terraform test
```

The checked-in tests use `mock_provider "databricks"`; their `command = apply`
constructs only simulated resources and mock state, not a real workspace apply.
Both roots were tested in network-disabled Terraform containers after provider
download. Tests cover the staged guard, URL validation, grants, storage isolation,
and compute bounds. Run `pytest tests/unit/test_databricks_platform.py -q` and
`cfn-lint cloudformation/*.yaml` for AWS trust-template checks.

Local tests cannot prove IAM assumption, current account features, inherited
entitlements, real UC privilege validation, compute-policy compatibility, or job
execution. Those remain gates for the first authorized workspace deployment.

## Pause and Full Destruction

Pause: stop local writers, cancel active Databricks runs, and verify job compute
has terminated. Do not destroy storage or state for an ordinary pause. Retained
S3/workspace storage can still incur charges.

Full destruction is the intended end-of-project option. After explicit data-loss
confirmation, perform this dependency order with the privileged setup identity:

1. Stop local producer/consumer/Airflow and cancel active jobs. Delete the deployed
   bundle/job/assets with its own tooling; Terraform does not own those objects.
2. Inventory and drop the five runtime-owned UC external tables plus any deliberate
   additions. They are absent from Terraform state. Verify schema contents before
   removal; drop external table metadata does not delete its S3 data.
3. Review `terraform plan -destroy` in `workspace/` and apply that reviewed plan.
   Nonempty catalogs/schemas or dependent locations must fail, not force-cascade.
   This also removes the `runtime_service_principal_user` permission grant and
   the service principal itself, since both are Terraform-managed resources in
   this root, not an externally assigned binding.
4. Destroy `credential/` after all its external locations are removed. Delete stack
   `06`, then `05`, before the S3 stack because of exported-policy/bucket imports.
   Do not delete IAM access while dependent cleanup still needs it.
5. Explicitly remove project S3 objects, noncurrent versions, delete markers, and
   unfinished uploads, then delete the bucket stack. Include dedicated managed
   catalog storage and checkpoints, not only external Delta tables. Check retained
   buckets, workspace artifacts/storage, any pre-existing compute, and unrelated
   provisioned resources separately. This IaC does not own the workspace itself.
6. Remove local spool/Kafka state only when replay loss is intended. Keep Terraform
   state/backups until platform destruction is verified, then securely dispose of
   them. Check usage/billing for residual resources and previously incurred charges.

The legacy teardown workflow does not yet automate this order or future Snowflake
cleanup. Do not run it against a hybrid deployment expecting complete removal.
No destruction commands have been executed by this implementation step.