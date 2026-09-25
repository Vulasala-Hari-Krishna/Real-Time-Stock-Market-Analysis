# Hybrid Integration Handover

## Read This First

Last updated: **2026-09-25**. This is the model-independent progress ledger for
the Databricks/Snowflake migration. It must remain usable without the original
chat history, assistant memory, or access to a particular model.

- Branch: `feature/databricks_snowflake_impl`. Do not change branches or commit
  unless the user requests it. Preserve intervening user edits.
- Repository anchor before this documentation update: `07cb728`, with a clean
  worktree. Check `git status --short --branch` and `git log -5 --oneline` on resume;
  this anchor is historical, not a claim about future HEAD.
- **Current position:** raw landing and one Databricks quote-processing slice are
  implemented and locally validated. Platform IaC has been revised for the account
  actually in use (Databricks **Free Edition**, workspace region `us-east-2`,
  serverless compute only — see "2026-09-24: Free Edition preflight" below) but
  not yet applied. Deployment and live end-to-end job validation remain pending.
  Snowflake integration is not implemented.
- Prior AWS resources: the user confirmed a full AWS teardown/cleanup was
  completed before this session, so the account is a clean slate. No real cloud
  deployment, Terraform apply, ingestion run, or destruction has been performed
  by the migration work recorded here.
- The next task is **deploying CloudFormation stacks `01`/`05`/`06` in `us-east-2`**
  and running the credential Terraform root, not silently adding more services or
  declaring the pipeline done. See "Next Session" below for the exact sequence.

### Reading Order

1. [Repository instructions](../.github/copilot-instructions.md) and the relevant
   [scoped instructions](../.github/instructions/).
2. This guide for progress, decisions, evidence, blockers, and remaining scope.
3. [Hybrid architecture and contracts](hybrid-migration.md) for storage, raw data,
   snapshot handoff, and local consumer operation.
4. [Databricks job guide](../databricks/README.md) for data semantics and retries.
5. [Platform bootstrap and destroy guide](../databricks/terraform/README.md) for
   account prerequisites, trust activation, Terraform state, and cleanup order.

The [main README](../README.md) begins with migration status; the older architecture,
diagrams, schedules, and examples below it describe the **legacy local system**.
This guide owns progress, the contract guide owns data contracts, and platform
guides own operational procedures. Update those owners instead of copying divergent
versions. Code/tests and measured runtime evidence take precedence over stale prose.

## Agreed Target

```text
LOCAL LAPTOP
  API -> producer -> Kafka -> raw consumer -> S3 landing/ticks/
                           |-> local live cache -> Streamlit (planned)
  Historical/fundamental fetchers --------> S3 landing/<dataset>/ (planned)

AWS + DATABRICKS
  S3 landing -> triggered Auto Loader -> bronze Delta -> silver Delta -> gold Delta
  Unity Catalog governs tables and S3 access; Jobs own internal dependencies
  Gold -> explicit immutable S3 exports -> completed manifest written last (planned)

SNOWFLAKE (planned)
  Completed exports -> COPY staging -> validation -> transactional native tables
  Native tables -> dimensional models / screening and reporting marts
  One X-Small warehouse provides compute; local Streamlit queries/caches results

LOCAL AIRFLOW (hybrid coordination planned)
  New-input gate -> Databricks run -> completed publication -> Snowflake -> reconcile
```

### Decisions To Preserve

- Personal project: minimize recurring billed activity. Kafka, producer, consumer,
  fetchers, Airflow/PostgreSQL, and Streamlit stay local. Preserve local Spark until
  its replacements are verified. No MSK, MWAA, EMR, or always-on cloud compute.
- S3 is the durable outbound-only boundary; do not expose Docker services to the
  internet or make Databricks connect to a laptop Kafka hostname.
- Raw landing files are not bronze tables. Databricks owns canonical bronze,
  silver, and gold Delta processing; do not create two competing silver pipelines.
- Consumer-landed events are the cloud stream source. Producer API backups stay
  separate; ingesting both as ticks would double-count source activity.
- Default cloud operation is manual, later optionally daily. No per-file compute
  startup, recurring tasks, or Snowpipe activation as incidental implementation.
- Snowflake owns replicated tables and SQL serving, not duplicate indicator logic.
  Its virtual warehouse is compute, not the physical storage layer.
- Start with small complete snapshots. CDC/CDF, Snowpipe, Streams/Tasks, declarative
  quality pipelines, and Dynamic Tables are later opt-in demonstrations.
- Full destructive cleanup of project data/resources is an explicit user requirement
  after use. Also support pause. Do not replace destroy with suspend-only behavior;
  do not execute destruction without explicit confirmation.
- Infrastructure ownership: CloudFormation for AWS; Terraform for Databricks and
  later Snowflake platform objects; bundles for Databricks jobs/code; versioned SQL
  for Snowflake models/migrations. Give every object/grant one owner.
- Never store secrets, live Terraform state, saved plans, or credentials in this
  document or Git. Collect authentication through local/CI secret mechanisms, not chat.
- **Databricks Free Edition is the target platform, not a paid workspace.**
  Verified 2026-09-24: Unity Catalog storage credentials/external locations
  against a self-owned S3 bucket work fully (Test Connection passed Read/List/
  Write/Delete/Path Exists/Assume Role/Self-Assume Role/External ID Condition),
  and a serverless notebook wrote/read a real Delta table through one. Free
  Edition has no classic compute at all (serverless only) and no account
  console/account-level API access; it does **not** lack external-S3 support,
  which earlier repository docs incorrectly assumed. The `databricks_cluster_policy`
  resource and its bundle `job_clusters` block were removed accordingly (see
  [workspace/main.tf](../databricks/terraform/workspace/main.tf) and
  [databricks.yml](../databricks/databricks.yml)); the job now runs on serverless
  compute via a bundle `environment_key`/`environments` block.
- **AWS region for this project's hybrid resources is `us-east-2`**, chosen to
  match the Databricks Free Edition workspace's fixed region (workspace region
  cannot be changed on Free Edition). CloudFormation stacks `01`/`05`/`06` and
  their GitHub Actions workflows default to `us-east-2` accordingly; the local/
  legacy stack defaults were also moved off `us-east-1` for consistency.
  Databricks workspace ID `7474656307742289`; region confirmed via a Unity
  Catalog metastore ID in the form `aws:us-east-2:<uuid>`.
- GitHub Actions with OIDC (`aws-actions/configure-aws-credentials`, role
  `secrets.AWS_DEPLOY_ROLE_ARN`) is the user's preferred deployment mechanism for
  every platform, not just AWS — avoid asking them to configure local AWS/
  Databricks/Snowflake CLI credentials. New workflows
  ([deploy-hybrid-infra.yaml](../.github/workflows/deploy-hybrid-infra.yaml),
  [teardown-hybrid-infra.yaml](../.github/workflows/teardown-hybrid-infra.yaml))
  extend this pattern to stacks `05`/`06`. A Databricks-Terraform-apply workflow
  is not yet built — it needs real stack `06` output values first, and the
  existing runbook's own "reviewed plan, not auto-approve" guidance argues for a
  GitHub Environment manual-approval gate rather than an unattended apply.

## Implementation Status

"Implemented/local" means code exists and recorded local checks passed; it does
not mean deployed, integrated, production-ready, or cost-measured.

| Workstream | Status | Primary evidence / remaining boundary |
|------------|--------|---------------------------------------|
| Architecture and coding guidance | Documented | Contract guide and scoped instructions; keep current |
| AWS storage foundation | Prepared/local | [S3 template](../cloudformation/01-s3-datalake.yaml); no live update verified |
| Raw capture | Implemented/local, opt-in | [Consumer](../src/consumers/raw_landing.py), [tests](../tests/unit/test_raw_landing.py); live Kafka/S3 pending |
| Databricks processing | Implemented/local | [Runner](../src/batch/databricks_ticks.py), [transforms](../src/batch/landed_ticks.py); Auto Loader/UC/Delta execution pending |
| Databricks platform IaC | Prepared/local | [Credential root](../databricks/terraform/credential/main.tf), [workspace root](../databricks/terraform/workspace/main.tf), [IAM role](../cloudformation/06-databricks-storage-role.yaml) |
| Manual job deployment | Prepared/local | [Bundle](../databricks/databricks.yml), [wheel configuration](../databricks/pyproject.toml); CLI/workspace validation pending |
| Gold export and manifest | Contract only | No exporter, executable manifest contract, or publication audit yet |
| Snowflake platform/load/marts | Planned | [Publish-read policy](../cloudformation/05-hybrid-access.yaml) and [scoped rules](../.github/instructions/snowflake.instructions.md) only; no Snowflake SQL/Terraform yet |
| Historical/fundamental migration | Planned | Existing local batch jobs remain; no cloud historical indicators yet |
| Streamlit integration/live cache | Planned | Existing S3 dashboard unchanged; no Snowflake connector/cache implemented |
| Hybrid Airflow coordination | Planned | Existing local DAGs unchanged; no pre-compute new-input gate implemented |
| Deploy/pause/full destroy automation | Partial | Legacy AWS scripts exist; hybrid dependency order documented, not automated |
| CI | Implemented/local checks | [Workflow](../.github/workflows/ci.yaml) includes mocked Terraform checks; runs on main push/PR to main, not every feature-branch push |
| Cloud reconciliation/cost measurements | Not performed | Need authorized account access, run evidence, and billing observations |
| Advanced demonstrations | Deferred | No CDC/Snowpipe/Streams/Tasks/continuous pipelines to enable yet |

### What The Implemented Slices Actually Do

**Storage and access:** legacy bucket identities/paths are retained. The new
`landing/`, `lakehouse/`, `checkpoints/hybrid/`, and `publish/` boundaries avoid
expiry on active Delta files/checkpoints. Existing legacy `bronze/` still expires
after 30 days; the legacy gold cold-tier transition is removed in the template.
Multipart cleanup affects incomplete uploads only. Stack `05` creates unattached
policies; stack `06` attaches the Databricks policy to an initially unassumable role.

**Raw consumer:** disabled by default in [settings](../src/config/settings.py) and
the `hybrid-raw` [Compose profile](../docker/docker-compose.yaml). It retains source
bytes, tombstones, ordered headers, timestamps, and transport identity in gzip
NDJSON. SQLite stores pending records; S3 upload precedes explicit Kafka commits.
Restart/rebalance can replay records: delivery is at least once. Databricks must
deduplicate `(source_id, topic, partition, offset)`. Preserve `.state/` or the
`raw-landing-state` volume across normal stops, remove it deliberately during full
cleanup, and never replay old state after intentionally emptying the destination.

**Quote processing:** Auto Loader text mode retains raw lines in `ticks_raw`;
validation produces `quote_samples` and `ticks_quarantine`; gold is
`daily_quote_summary`; `ticks_pipeline_state` records processing/completion and
input/output versions. Conflicting transport/business identities are quarantined,
not arbitrarily overwritten. Tombstones are not implemented as historical deletes.
Gold is sampled quotes by `(provider, symbol, capture_date_utc)`, **not** exchange
OHLCV or the existing `daily_summaries` indicator product. Provider volume is kept
as last reported volume, never summed. Legacy polling time is not exchange time.

The first slice fully rebuilds up to a default 100,000 bronze rows. It pins source
versions, reconciles counts/keys, writes individual Delta snapshots, and marks
completion last. There is no cross-table Delta transaction. Consumers must require
completed state and read recorded versions. Increment `PIPELINE_REVISION` when
changing transformation semantics. The job has no schedule, queue, or automatic
retries; limits are 15 minutes for ingestion and 30 minutes overall. A manually
started no-op job still incurs startup cost until an external input gate is built.

**Platform bootstrap:** two independent Terraform roots avoid destructive mode
switching. Apply credential bootstrap only after the disabled role exists, use
generated principal/external ID to activate exact IAM trust, re-enable credential
validation, then create the workspace resources. Terraform creates policy metadata,
not a running cluster. Tables belong to the runtime; catalogs/schemas/locations
belong to Terraform. Managed catalog storage has its own nonoverlapping prefix.

## Next Session: Concrete Starting Point

Account/capability preflight is done (see "Decisions To Preserve" above): AWS is
a confirmed clean slate, the Databricks account is Free Edition (workspace
`7474656307742289`, region `us-east-2`, storage credentials to a self-owned S3
bucket verified working), and the user deploys everything through GitHub
Actions with OIDC rather than local CLI credentials. The concrete remaining
sequence:

1. Read this guide and check Git state; inspect touched files before edits.
2. Via [deploy-infra.yaml](../.github/workflows/deploy-infra.yaml)
   (`aws-region: us-east-2`), deploy stacks `01`-`04` if not already present.
   This creates the real project bucket
   `stock-market-pipeline-datalake-dev-<account-id>`, separate from any personal
   test bucket used during preflight.
3. Run [deploy-databricks-platform.yaml](../.github/workflows/deploy-databricks-platform.yaml)
   once. This single workflow now automates the entire remaining bootstrap:
   Terraform state backend (stack `00`) -> stacks `05`/`06` (deny-all trust) ->
   `credential` root bootstrap apply -> reads its generated
   `unity_catalog_principal_arn`/`unity_catalog_external_id` -> re-deploys `06`
   with real trust -> re-applies `credential` root with validation -> applies
   the `workspace` root (catalog/schemas/external locations/service principal).
   No manual value copy-pasting between steps; both Terraform roots now use a
   remote S3 backend (see [terraform/README.md](../databricks/terraform/README.md#state-and-authentication))
   so state persists correctly across this workflow's separate jobs. Needs repo
   secrets `AWS_DEPLOY_ROLE_ARN` (existing), `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
   (a personal access token).
4. Confirm the real external locations with "Test Connection" in the Databricks
   UI (same check already proven against the personal test bucket).
5. Set `BUNDLE_VAR_*` from the workflow's final `bundle_variables` output,
   `databricks bundle validate -t dev`, then an explicitly authorized
   `databricks bundle deploy -t dev` and one `databricks bundle run -t dev
   landed_ticks` against a small fixture. The serverless
   `environment_key`/`environments` block in `databricks.yml` is unverified
   against a live workspace — check `bundle validate` output carefully. This
   step has no GitHub workflow yet (Databricks CLI + bundle deploy/run is a
   natural next automation candidate, deferred until the platform bootstrap
   above is proven live).
6. Record versions/counts, quarantine cases, replay/no-op behavior,
   failed-publication recovery, and measured cost in this ledger before
   proceeding to the first snapshot exporter/Snowflake slice. To tear down,
   run [teardown-databricks-platform.yaml](../.github/workflows/teardown-databricks-platform.yaml)
   (same catalog/schema_prefix/credential_name inputs as the deploy run) — it
   deliberately does not delete stack `00`'s state bucket/lock table
   (`DeletionPolicy: Retain`); remove those manually only after confirming both
   Terraform roots destroyed cleanly.

## Remaining Roadmap And Completion Criteria

| ID | Work to implement or verify | Done when |
|----|----------------------------|-----------|
| R1 | Databricks Free Edition bootstrap and live quote slice (`us-east-2`) | Real IAM/UC/bundle checks pass on the actual Free Edition workspace; isolated fixture, replay, quarantine, and recovery evidence recorded. Storage-credential/external-location half already verified against a personal test bucket; remaining work is stacks `05`/`06` + both Terraform roots against the real project bucket, then one authorized serverless job run |
| R2 | Gold snapshot exporter and manifest validation | Reads completed version set through Delta APIs; immutable files/checksums/counts/schema/sequence; manifest last; incomplete/stale/repeated batches tested |
| R3 | Snowflake platform IaC and safe identity bootstrap | One auto-suspending X-Small warehouse, role-separated grants, storage integration/stage, database/schemas, cost controls and destroy path; live access authorized and tested |
| R4 | Snowflake snapshot loader and analytical model | Exact manifest files load to staging; key/schema/count checks; atomic DML publication plus batch ledger; empty snapshots/corrections/deletions/retries covered; results reconcile to Delta |
| R5 | One historical Streamlit view using Snowflake | Cached queries with least-privilege credentials; source/freshness explicit; failed cloud access cannot masquerade as demo success; old path preserved until verified |
| R6 | Historical/fundamental cloud migration | Local fetchers land raw sources; documented schemas/calendars/source priority; Databricks owns governed history and indicators with warm-up/correction tests; migrate one legacy product at a time |
| R7 | Local live-data cache | Durable bounded local serving path with explicit freshness and outage semantics; useful live UI without waking Snowflake continuously |
| R8 | Local Airflow cross-platform workflow | Manual-only initially; skip compute before startup on no input; durable run/batch IDs, reconnect instead of duplicate submission, finite retries/timeouts, manifest gates, and reconciliation |
| R9 | Hybrid deployment, pause and full destroy automation | Preview/confirmation, staged dependency ordering, no secret outputs, active-run cancellation, data/version/checkpoint removal when confirmed, state preserved until verified, residual-resource checks |
| R10 | Portfolio completion and operational evidence | New architecture diagram/README replace legacy claims only when true; repeatable small demo, measured latency/cost, recovery/RBAC evidence, supported runtime/dependency documentation |
| R11 | Optional advanced demonstrations | Separately approved CDC/CDF bootstrap/order/tombstone/retention recovery; Snowpipe + Streams/Tasks, quality expectations, or suitable Dynamic Tables; off by default and stoppable |

R1 is the current gate. R2-R5 provide the first two-platform serving slice. R6/R7
extend business coverage; R8/R9 complete operations, with cleanup for each new
resource required as it is introduced. R11 is not a prerequisite for the baseline.
MLflow, Cortex, Iceberg interop, and duplicate pipelines were not agreed baseline
requirements. Do not add them merely to increase the technology list.

## Verification Record

The 2026-09-23 rows below are **recorded results from that implementation
session**. The 2026-09-24 row is new, real evidence from this session, gathered
directly in the user's actual Databricks account; it is not a new run of the
2026-09-23 checks. Rerun relevant gates after edits; do not treat counts as
permanently fixed or evidence of live cloud CI.

| Check | Recorded result | Boundary |
|-------|-----------------|----------|
| Free Edition external S3 storage (2026-09-24) | Unity Catalog storage credential + external location created via workspace UI against personal bucket `s3://vulasala-dbcks-de-project/`; "Test Connection" passed Read/List/Write/Delete/Path Exists/Assume Role/Self-Assume Role/External ID Condition (all permissions confirmed); a serverless notebook then wrote and read back a real Delta table through the same external location | Real Databricks Free Edition workspace (`7474656307742289`, `us-east-2`) and real AWS account; against a **personal test bucket**, not yet the project's own CFN-managed bucket/prefixes or the actual Auto Loader job |
| Full unit suite | 368 passed; 87.34% overall coverage, >=80% gate | Local Windows Python 3.12.10; external services mocked |
| Real Spark transformations | Four tests passed with installed wheel in Linux Spark 3.5.3/Python 3.11 container | Actual DataFrame semantics, not Auto Loader/UC/S3/Delta runtime validation |
| Windows Spark run | Python worker crashed | Not a successful validation; Linux run provided behavior evidence |
| Raw-consumer image | Built; non-root Python 3.11 offline SDK/SQLite smoke passed | No broker-to-S3 connectivity verification |
| Databricks wheel | Built and imported/executed from temporary installed package | No workspace deployment or bundle CLI validation |
| Terraform | Both roots validated; six mock tests passed with network disabled, **before** the 2026-09-24 removal of `databricks_cluster_policy`/`databricks_permissions` from the `workspace` root and the matching test-file update | Terraform 1.9.8, provider 1.88.0; simulated apply only. Re-run `terraform fmt -check && terraform validate && terraform test` for both roots after the 2026-09-24 edit — not done this session; Terraform CLI was unavailable in this environment (native and Docker) |
| AWS templates | All six passed cfn-lint; five IAM contract tests passed | Syntax/contracts, not deployed trust or grants. Unchanged by the 2026-09-24 edits (05/06 templates were not modified) |
| Code checks | Black, Ruff, mypy passed | Existing repository gates; re-run after 2026-09-24 edits, not done this session |
| Databricks bundle test (2026-09-24) | `test_bundle_is_manual_bounded_and_uses_explicit_platform_inputs` updated and passing (34/34 in `test_databricks_ticks.py`) for the new serverless `environment_key`/`environments` structure | Validates the YAML shape only; does not prove `databricks bundle validate` accepts this schema against a live workspace |

Useful local commands from the repository root (use the selected interpreter):

```text
python -m pytest tests/unit -q --cov=src --cov-report=term-missing --cov-fail-under=80
python -m ruff check src/ tests/
python -m black --check src/ tests/
python -m mypy src/ --ignore-missing-imports
cfn-lint cloudformation/*.yaml
docker compose -f docker/docker-compose.yaml --profile hybrid-raw config --quiet
python -m pip wheel --no-deps --wheel-dir databricks/dist ./databricks
```

For each Terraform root: `terraform init -backend=false -input=false
-lockfile=readonly`, then `terraform fmt -check`, `terraform validate`, and
`terraform test`. Provider initialization downloads tooling; the checked-in tests
use mock providers, not real platform applies. CI uses the pinned Terraform Docker
image; see the workflow for reproducible commands. Do not expose secrets through
resolved Compose output. Cloud integration tests require separate authorization.

Tooling observed on the last implementation machine: native Terraform and
Databricks CLI were not available; Terraform ran in Docker. If the installed
`cfn-lint` Windows launcher is absent, invoke its console entry point through
Python package metadata; `python -m cfnlint` did not work in that installation.
PowerShell 5.1 needs careful native-argument quoting. Recheck tool availability on
resume rather than assuming this environment is unchanged.

## Known Gaps And Safety Boundaries

- The legacy deploy script prints credential outputs. Legacy teardown empties
  versioned buckets and handles only stacks `04/03/02/01`; it misses `05/06` and
  all Databricks/Snowflake resources. Destructive intent is valid; coverage and
  error reporting need improvement before hybrid use. See R9 and the runbook.
- Runtime workspace service-principal removal can leave an account-level identity
  or binding. Inventory both scopes during final cleanup. Bundle destroy does
  not delete external tables/data, credentials, state, or the workspace itself.
- Terraform `trust_activation_confirmed` is an operator acknowledgement, not proof
  that IAM works. False entitlements do not override inherited grants. Serverless
  job/task quotas, the bundle's `environment_key`/`environments` schema, and
  wheel permissions are untested against a live workspace.
- Consumer immutability is enforced by application convention/checks, not S3 Object
  Lock. Replayed files can consume duplicate storage; deduplication is downstream.
- No executed cross-cloud transaction exists. Current gold completion state is not
  the future export manifest; validate that manifest and publication ledger explicitly.
- New raw/export retention and noncurrent-version cleanup need a policy based on
  recovery windows. Removing a lifecycle transition does not reclassify old objects.
- Local shutdown does not cancel remote jobs. Account budgets/warehouse monitors
  are not guaranteed hard caps on all charges. Stored data and Snowflake retention/
  Fail-safe may remain billable after compute stops or objects are dropped.
- No measured cloud costs, production reliability, or recovery of never-captured
  market events may be claimed from the local tests.

## Change Ledger

This is a milestone ledger, not a fabricated per-file commit history. Git is the
source for exact diffs. Dates/revisions below describe completed prior work.

| Date | Milestone | Result / revision anchor |
|------|-----------|--------------------------|
| 2026-09-22 to 2026-09-23 | Architecture/instructions, storage foundation, raw consumer | Standard Copilot entry point/scoped rules, contracts, optional IAM policies, durable opt-in consumer and tests; `8a0f233` is the recorded foundation/raw snapshot |
| 2026-09-23 | Databricks quote slice | Runner, normalization/deduplication, sampled summaries, wheel/bundle, real Spark tests; `0f5ff64` |
| 2026-09-23 | Databricks platform IaC | Fail-closed role, two Terraform roots/locks, mock tests, CI and staged cleanup guide; `07cb728` |
| 2026-09-24 | Cross-assistant handover | This ledger/roadmap and durable instruction to maintain it; documentation-only, based on the preceding Git anchor |
| 2026-09-25 | Fully automated Databricks platform bootstrap/teardown pipeline | User merged the feature branch to `main` and asked for zero-manual-step, remote-state-backed IaC ("no manual creation... like big companies do") instead of the local-Docker/copy-paste sequence from 2026-09-24. Added [cloudformation/00-terraform-state.yaml](../cloudformation/00-terraform-state.yaml) (S3 bucket + DynamoDB lock table, both `DeletionPolicy: Retain`, deliberately outside the data bucket's teardown blast radius). Added `backend "s3" {}` to both `databricks/terraform/credential/main.tf` and `workspace/main.tf` (backend values supplied via `-backend-config` at init time; `-backend=false` offline validation unaffected) — required because the `credential` root is applied twice and a disposable CI runner has no local disk continuity between those applies. Added [deploy-databricks-platform.yaml](../.github/workflows/deploy-databricks-platform.yaml) (5 chained jobs: bootstrap state+role -> credential bootstrap apply -> activate real IAM trust -> credential validate apply -> workspace-objects apply, each job's outputs feeding the next automatically) and its reverse, [teardown-databricks-platform.yaml](../.github/workflows/teardown-databricks-platform.yaml). Updated [databricks/terraform/README.md](../databricks/terraform/README.md) to describe the remote backend and point to the new workflow as the primary path, keeping the manual step-by-step description as reference/troubleshooting docs. Verified: `cfn-lint` passed on the new template and the full `cloudformation/*.yaml` set; both new workflow YAML files parse; **not verified**: an actual run of either new workflow (needs `DATABRICKS_HOST`/`DATABRICKS_TOKEN` secrets added first, and stack `01` deployed in `us-east-2`), and `terraform validate`/`terraform test` were not re-run against the backend-block change (Terraform CLI still unavailable in this environment) |
| 2026-09-24 | Free Edition preflight and serverless re-plan | User confirmed AWS teardown complete and provided Databricks account details (workspace `7474656307742289`, `us-east-2`). Live-tested Unity Catalog storage credential + external location against a personal S3 bucket in the user's actual Free Edition workspace: Test Connection passed all checks, and a serverless notebook wrote/read a real Delta table through it — corrected the prior (incorrect) assumption that Free Edition cannot use external S3 storage. Removed `databricks_cluster_policy`/`databricks_permissions` from `workspace/main.tf` and the classic `job_clusters` block from `databricks.yml` (replaced with a serverless `environment_key`/`environments` block); updated the matching Terraform test (`workspace/safety.tftest.hcl`) and Python test (`tests/unit/test_databricks_ticks.py`); updated `databricks/README.md`, `databricks/terraform/README.md`, `docs/hybrid-migration.md` to state verified Free Edition compatibility instead of the earlier uncertainty. Moved the project's AWS region default from `us-east-1` to `us-east-2` in `deploy-infra.yaml`/`teardown-infra.yaml`. Added `cloudformation/deploy-hybrid.sh` + `teardown-hybrid.sh` and `.github/workflows/deploy-hybrid-infra.yaml` + `teardown-hybrid-infra.yaml` to bring stacks `05`/`06` under the same GitHub-Actions-with-OIDC pattern already used for stacks `01`-`04`, since the user deploys everything via GitHub Actions rather than local CLI credentials. Uncommitted; `pytest tests/unit/test_databricks_ticks.py` (34 passed) and a YAML syntax check on all edited workflow/bundle files were run — `terraform fmt/validate/test` for the edited `workspace` root was **not** re-run (Terraform CLI unavailable in this environment) and remains a gate before the next real apply |

### Required Update After Every Implementation Session

1. Update the date, status table, current gate, and next concrete action.
2. Append a ledger row with changes, relevant file links/revision, checks actually
   run, results, blockers, and any change to prior decisions. Use "uncommitted"
   when appropriate; never invent a commit or imply CI ran because local tests passed.
3. Separate implementation, local/static validation, deployment, and live verification.
   Record non-secret environment/run identifiers and cleanup status for authorized
   cloud work. Never paste credentials, full state, or secret-bearing outputs.
4. Update the owning contract/runbook when behavior changes and link it here. Mark
   superseded assumptions and failed checks explicitly rather than hiding them.
5. Leave the next assistant a small actionable task, its acceptance checks, and
   any authorization/account information still needed. No hidden TODOs in chat only.

## Continuation Prompt

```text
Read docs/implementation-handover.md first, then .github/copilot-instructions.md
and the scoped instructions for the files you touch. Verify the branch/worktree
and current file contents. Continue from the current gate and remaining roadmap,
preserving the local baseline and existing user edits. Do not deploy, start paid
workloads, or delete data without explicit authorization. Distinguish local
checks from real cloud evidence. Update the handover status, validation record,
change ledger, blockers, and next step before finishing the session.
```

## Later Review And Optimization

When the original assistant or another reviewer returns, compare this ledger to
the code and Git changes since `07cb728`. Review actual deployed resource state,
least-privilege grants, version/manifest semantics, failure/replay/correction/delete
handling, no-input cost gates, dashboard freshness, state protection, and full
destruction including residual resources. Reconcile sample results across platforms,
run relevant gates, and assess measured spend/latency before optimizing compute or
adding abstractions. A feature inventory is not evidence of end-to-end correctness.