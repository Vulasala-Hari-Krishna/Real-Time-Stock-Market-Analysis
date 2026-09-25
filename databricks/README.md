# Databricks Quote Slice

## Status

Implemented code and a manual deployment bundle; **not yet run end-to-end in a
workspace**. Local Kafka/raw capture and the legacy silver consumer are unchanged.
This slice requires a workspace with Unity Catalog and external S3 access; it
runs on **serverless compute**, not a classic job cluster.

Databricks Free Edition has been verified capable of every part of this
slice's platform bootstrap through a real run (2026-09-25): the storage
credential, all six external locations, the catalog/schemas, and the runtime
service principal + its Unity Catalog grants applied cleanly against the real
project bucket. Free Edition has **no classic compute at all**, which is why
this bundle targets serverless.

The bundle does **not** set `run_as: service_principal_name`, even though
`workspace/main.tf` provisions a least-privilege `runtime` service principal
with exactly that intent. Doing so needs the deploying identity to hold the
"Service Principal User" role on that principal, which this provider version
(1.88.0) only exposes through `databricks_access_control_rule_set`, an
account-level rule-set resource (`name = "accounts/<id>/servicePrincipals/..."`)
— and it's unverified whether that account-level API path is reachable from a
workspace-scoped provider/token on Free Edition. Rather than guess at that a
third time, the job currently runs as the deploying admin directly (the
default when `run_as` is omitted). The `runtime` principal and its grants stay
provisioned, unused for now, ready for whoever revisits this — worth retrying
if the workspace is ever upgraded to one with account-console access, where
the account-level rule-set resource is known to work.

`databricks bundle validate` has been run once against a live workspace and
passed, including the serverless `environment_key`/`environments` syntax.
`bundle deploy`/`bundle run` (the actual Auto Loader job) have not yet
completed successfully — that's the next concrete step.

```text
landing/ticks/**/*.json.gz
    -> Auto Loader text ingestion, AvailableNow
    -> <catalog>.<prefix>_bronze.ticks_raw
    -> strict envelope/quote validation + deterministic deduplication
    -> <catalog>.<prefix>_silver.quote_samples
    -> <catalog>.<prefix>_gold.daily_quote_summary
    |-> <catalog>.<prefix>_silver.ticks_quarantine
    |-> <catalog>.<prefix>_gold.ticks_pipeline_state (completion/version record)
```

The runner is [databricks_ticks.py](../src/batch/databricks_ticks.py); reusable
transformations are in [landed_ticks.py](../src/batch/landed_ticks.py).
The [foundation contract](../docs/hybrid-migration.md) defines raw transport fields.

## Data Semantics

- Bronze appends original NDJSON lines, file path/modification time, and bronze
  ingestion time. Text ingestion deliberately retains malformed JSON for later
  quarantine; gzip envelopes are not read as cleansed market records.
- Version-one envelopes require Kafka source/topic/partition/offset, timezone-aware
  capture metadata, ordered headers, and valid base64. Payload validation reuses
  the existing `StockTick` model with additional finite-price, integer-volume,
  symbol, and timezone checks. Rejections retain raw lines and a reason code.
- Transport duplicates share `(source_id, topic, partition, offset)`. Their
  content fingerprint excludes only `ingested_at`, which can change on local
  recovery. Conflicting content for one identity is quarantined in full.
- Business duplicates share `(provider, symbol, quote_timestamp)`. Identical
  price/volume values select a deterministic transport representative. Conflicting
  values are quarantined, not resolved by ordering offsets across partitions or
  Kafka incarnations. Quarantined records cannot suppress valid candidates.
- Tombstones are quarantined as unsupported tick events, not interpreted as
  deletes of a historical quote. Explicit upstream correction/deletion semantics
  must be designed before supporting them; bronze history is not mutated here.
- Gold reports first/highest/lowest/last observed price, last reported volume,
  quote count, first/last capture time, and observed price change by provider,
  symbol, and **UTC capture date**. It is not exchange-session OHLCV, total traded
  volume, official daily return, or proof of a live quote entitlement. The current
  producer timestamps API polling; provider volume is not summed.
- No historical indicators, historical/fundamental ingestion, exports, Snowflake,
  Airflow changes, or CDC are included. Daily indicators require authoritative
  historical bars and explicit warm-up/correction windows in a later slice.

## Replay and Publication

Auto Loader uses a native append Delta sink with its own durable checkpoint under
`checkpoints/hybrid/<catalog>/<prefix>/ticks_raw/stream`. Schema state has a separate
subpath. Source text files are immutable; do not delete/reset the checkpoint or
reuse it for another target. Re-uploaded files can still duplicate messages;
silver performs transport and business deduplication across the bronze snapshot.

Each run pins one bronze version and fully rebuilds this small dataset. A default
100,000-row guard stops larger rebuilds explicitly; do not raise it blindly. This
processes late observations anywhere in retained bronze, not just the latest day.
Classification counts must sum to input rows, silver keys must be unique, and
gold quote counts must equal accepted rows before output writes.

Tables use dedicated external paths:
`lakehouse/<layer>/<catalog>/<prefix>/<table>`. Catalogs/schemas and storage grants
must already exist. The job registers/writes its five owned Delta tables but does
not create catalog-level identities or permissions. Existing names pointing to
unexpected formats/paths are rejected before ingestion. Use empty dedicated table
paths on first deployment; do not adopt unrelated unregistered Delta directories.

Output writes are individually atomic Delta overwrites, **not one transaction
across all tables**. State is marked `processing` before rebuilding and `completed`
only after all writes, with input/output versions, code revision, and counts.
Downstream consumers must require completed state and read those exact table
versions. A failure leaves `processing` state; rerun to recover even if Auto Loader
has already advanced its checkpoint. A completed, unchanged input/output version
set skips recomputation. Change `PIPELINE_REVISION` when transformation semantics
change so existing input is reprocessed.

Only one job/deployment may write these table paths. Job concurrency is one and
queueing is disabled; that does not coordinate independent jobs or manual writers.
Runtime/permission errors fail the run. Auto Loader has a 15-minute bound; the job
has a 30-minute timeout and no automatic retries. A retry is an explicit action.

## Preflight

Before any deployment or billable execution:

1. Confirm Databricks AWS workspace, region, Unity Catalog capabilities, and
   workspace service authentication. No credentials go into this bundle.
2. Configure the supported S3 IAM trust role/storage credential/external locations
   for landing read, lakehouse read/write, and checkpoint read/write. The optional
   [AWS policy template](../cloudformation/05-hybrid-access.yaml) is unattached;
   it does not complete Unity Catalog role trust, including required self-assume
   configuration. Verify exact platform permission requirements in the workspace.
3. Provide a dedicated catalog and `<prefix>_bronze`, `<prefix>_silver`, and
   `<prefix>_gold` schemas. Give the runtime service principal `USE CATALOG`,
   `USE SCHEMA`, appropriate `CREATE TABLE` and table read/write permissions,
   and external-location file/external-table grants needed by this job. Do not
   grant routine jobs administrator privileges or overlapping managed locations.
4. This job runs on serverless compute; there is no runtime/node-type/compute-
   policy selection to make. Confirm serverless jobs are enabled for the
   workspace and that Free Edition's job/task concurrency quota (5 concurrent
   tasks per account, at last check) is sufficient. Verify deployment identity
   permissions to use the runtime service principal.
5. Use a unique dev catalog/schema prefix and restrict permissions on the bundle's
   shared workspace deployment root. Deploying the same target twice is not an
   isolated environment. Do not start a job with no newly landed files or pending
   processing failure. A manual invocation still starts billed compute even if the
   runner later determines outputs are current; a pre-compute Airflow input gate
   is deferred to the orchestration phase.
6. Confirm budget, timeout, retry, and cleanup ownership. Free/trial credits are
   not a guarantee that this path is supported or cost-free.

Platform IaC is now prepared, not provisioned. Follow the
[staged Terraform/IAM setup guide](terraform/README.md): bootstrap the credential
against a disabled CloudFormation role, activate its exact generated trust, then
create the isolated catalog/schemas, external locations, and runtime principal.
There is no compute policy to create; the job runs on serverless compute.
Account/workspace capabilities and live permissions still require verification.
Neither root creates compute or deploys this job.

CloudFormation owns AWS IAM/S3, Terraform owns platform catalog/schema/identity
configuration, and this bundle owns the job plus its wheel artifact. The runtime
owns the five table definitions; do not also declare them under a second IaC owner.

## Build and Validate

From the repository root, build without installing Spark/Delta into the wheel:

```bash
python -m pip wheel --no-deps --wheel-dir databricks/dist ./databricks
pytest tests/unit/test_landed_ticks.py tests/unit/test_databricks_ticks.py -q
```

Spark/Delta come from the managed runtime. The wheel includes reusable `src`
modules but installs only Pydantic; it does not import local producer settings or
require Kafka/API credentials for this job. Local development uses Python 3.11+
and the repository tools. Real Spark checks are opt-in and require compatible Java:

```bash
RUN_LOCAL_SPARK_TESTS=1 pytest tests/integration/test_landed_ticks_spark.py -q
```

Real DataFrame tests passed in a Linux Spark 3.5.3/Python 3.11 container. The Windows
Python worker crashed in this environment; that run did not validate behavior.
Auto Loader, Unity Catalog, S3 permissions, actual Delta table publication/retry,
and billed usage still require an authorized workspace integration run.

Install a supported Databricks CLI and authenticate securely, then set non-secret
bundle variables using `BUNDLE_VAR_<name>` for every required variable in
[databricks.yml](databricks.yml). Set `DATABRICKS_HOST` or a CLI profile for the
workspace. From `databricks/`, run `databricks bundle validate -t dev` first. It
requires workspace access; local YAML checks are not a substitute. Inspect the
job definition and resolved policy before any authorized deployment/run:

```bash
databricks bundle deploy -t dev
databricks bundle run -t dev landed_ticks
```

These last commands are **not** part of local validation and were not executed.
No schedule or file-arrival trigger is defined. Run a small landed fixture first,
inspect quarantine reasons and completion state, reconcile values, rerun unchanged
input, then exercise failure/retry before connecting downstream systems.

## Pause and Destroy

After a demo, confirm the job has finished or cancel its active run in Databricks
and verify job compute terminates. Local Docker/AWS teardown does not stop it.
Do not run OPTIMIZE/VACUUM/maintenance schedules just for this slice.

For explicitly authorized permanent cleanup, cancel active runs before
`databricks bundle destroy -t dev`. Bundle destruction removes deployed job/assets;
it does **not** erase external Delta data, checkpoints, catalog/schema objects,
IAM roles, or workspace/account storage. Drop the five owned UC tables after
checking their ownership, then remove the dedicated S3 prefixes and all versions
only when their loss is intended. Coordinate removal of external locations,
credentials, schemas/catalog, platform identities, and CloudFormation imports in
dependency order. Pause any local writers before emptying the bucket. Full
cross-platform destroy automation is still deferred; verify residual compute,
storage, and retention charges rather than assuming deleting the bundle is enough.