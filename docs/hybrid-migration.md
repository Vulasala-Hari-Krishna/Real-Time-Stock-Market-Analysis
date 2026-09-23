# Hybrid Migration Foundation

## Status and Scope

This is the target contract for incremental work on
`feature/databricks_snowflake_impl`, not a description of deployed cloud services.
The [README](../README.md) documents the existing local workflow. This first step
prepared storage/access templates and the contracts below. The next local slice
now implements an opt-in raw consumer and tests without replacing the legacy
silver path. No resources, credentials, or cloud schedules have been deployed.

```text
Local API producer -> local Kafka -> local raw consumer -> S3 landing
Local historical/fundamental fetchers ------------------> S3 landing
S3 landing -> Databricks AvailableNow -> bronze Delta -> silver Delta -> gold Delta
Gold -> immutable S3 snapshots + completed manifest -> Snowflake tables -> SQL marts
Local Streamlit -> Snowflake historical analytics / planned local live-data cache
Local Airflow -> Databricks run -> validate publication -> Snowflake load/reconcile
```

The consumer persists source messages; Databricks owns canonical cleaning and
indicators. Snowflake owns dimensional/reporting SQL, not duplicate indicator
calculation. A Snowflake virtual warehouse is compute, distinct from stored data.
All laptop/cloud connections are outbound; cloud jobs never connect to local Kafka.

## Storage Layout

Reuse the existing S3 bucket without moving legacy data. Prefixes are logical
contracts, not directories that CloudFormation must create.

| Prefix | Format and owner | Retention in this step |
|--------|------------------|------------------------|
| `bronze/`, `silver/`, `gold/` | Legacy raw JSON, silver Parquet, gold Delta | Existing raw bronze 30-day expiry remains; gold cold-tier transition removed |
| `landing/ticks/` | Consumer-written gzip NDJSON envelopes | No automatic expiry |
| `landing/historical/`, `landing/fundamentals/` | Local source snapshots with extraction metadata | No automatic expiry; dataset schemas defined with each ingestion slice |
| `lakehouse/bronze/`, `lakehouse/silver/`, `lakehouse/gold/` | Databricks Delta tables registered in Unity Catalog | No object lifecycle expiry or storage-class transition |
| `checkpoints/hybrid/` | Per-query checkpoints and Auto Loader schema state | No lifecycle cleanup |
| `publish/` | Immutable per-batch Parquet snapshots and JSON manifests | No automatic expiry |

The [S3 template](../cloudformation/01-s3-datalake.yaml) exports the four hybrid
root URIs and denies non-TLS requests. It aborts incomplete multipart uploads
after seven days only under `landing/` and `publish/`; completed objects are not
expired. Removing the old transition does not move already transitioned gold
objects back to Standard. Versioning remains enabled.

No expiry is a conservative bootstrap default, not a permanent retention policy.
Before long-running capture, choose raw/export retention covering the longest
offline/replay window and a deletion procedure that checks processing progress.
Account for noncurrent versions and delete markers; deleting a versioned object
alone does not release all its storage. Use Delta-aware maintenance for table
files, never generic S3 expiration of active data or transaction logs.

## Raw Tick Contract v1

The opt-in [raw consumer](../src/consumers/raw_landing.py) writes gzip-compressed newline-delimited
JSON, one envelope per Kafka record, including malformed payloads and tombstones.
All instants use UTC ISO 8601 with an explicit timezone.

| Field | Required value |
|-------|----------------|
| `schema_version` | Integer `1` |
| `source_id` | Stable configured identifier for the Kafka source incarnation |
| `topic` | Original Kafka topic |
| `partition`, `offset` | Nonnegative Kafka partition/offset integers |
| `kafka_timestamp` | Kafka record timestamp or null when absent |
| `ingested_at` | Local capture timestamp, distinct from source event time |
| `key_base64` | Original key bytes encoded as base64, or null |
| `value_base64` | Original value bytes encoded as base64, or null for a tombstone |
| `headers` | Ordered array of `{key, value_base64}` records; preserve duplicate headers and null values |

Use `(source_id, topic, partition, offset)` for durable transport deduplication.
Change `source_id` after source recreation/offset reset; it must remain stable
across ordinary restarts. Market/business deduplication is a separate silver rule.
Parse source event times in Databricks; do not relabel polling time as exchange
event time or assume provider volume represents an incremental trade volume.

Group records by source/topic/partition and batch by bounded time/size, not one
object per quote. Use ingestion-date prefixes and keys containing offset ranges
and a content digest. A retry may reproduce a key or overlap a previous range;
never overwrite a completed object with different bytes. Durably track pending
batches and advance Kafka progress only after successful S3 persistence. This is
at-least-once transport with downstream deduplication, not an exactly-once claim.

Producer API backups remain in the legacy path and are not a second Auto Loader
input for ticks. Preserve the old silver-writing consumer until the raw path is
tested and explicitly selected. Use separate consumer groups/checkpoints for a
side-by-side comparison; do not let two modes compete for the same offsets.

## Running the Raw Consumer

The legacy Spark silver path remains the default. The raw path requires no Spark
and is a separate process using the existing kafka-python/boto3 dependencies.
It is excluded from default Compose startup and exits without creating clients
unless `RAW_LANDING_ENABLED=true`. `RUN_PIPELINE`/`MAX_ITERATIONS` still control
the producer, not this consumer; stop the consumer explicitly when finished.

Before running against AWS, authorize deployment/access, ensure the bucket
exists, and configure a dedicated identity with the landing policy. Templates
have not been applied for you. For local host execution, the standard boto3
credential chain is supported; configured access keys and optional session token
from settings are also supported. Never commit credentials.

Set these values in the ignored `.env` (see [the example](../.env.example)):

| Setting | Default and meaning |
|---------|---------------------|
| `RAW_LANDING_ENABLED` | `false`; set `true` only when intentionally starting capture |
| `RAW_SOURCE_ID` | Empty; required stable source-incarnation ID such as `local-kafka-v1` |
| `RAW_CONSUMER_GROUP` | `stock-raw-landing-v1`; keep independent of legacy readers |
| `RAW_TOPIC` | `raw_stock_ticks` |
| `RAW_SPOOL_PATH` | `.state/raw-landing.sqlite3` on the host; named volume path in Docker |
| `RAW_FLUSH_INTERVAL_SECONDS` | `60`; normal buffering interval, not an upload latency SLA |
| `RAW_MAX_RECORDS` | `500`; per-poll and per-object record limit |
| `RAW_MAX_BATCH_BYTES` | `5242880`; uncompressed envelope bytes per object, not compressed bytes |

Start only the raw consumer and its local Kafka dependencies when ready; this
does not launch Spark, the producer, Airflow, Databricks, or Snowflake:

```bash
docker compose -f docker/docker-compose.yaml --profile hybrid-raw up --build -d raw-consumer
docker compose -f docker/docker-compose.yaml logs --tail 100 raw-consumer
```

Start the producer separately under its existing bounded demo controls to
capture new quotes. Alternatively, consume messages already retained in Kafka.
For host execution against a running broker, use `python -m src.consumers.raw_landing`.
The first assignment with no committed offsets starts at the earliest retained
offset. This cannot recover data Kafka already expired. Existing committed
offsets outside broker bounds cause an explicit failure, not a silent reset.

Normal stop preserves the spool and Kafka volumes:

```bash
docker compose -f docker/docker-compose.yaml stop raw-consumer
```

SIGINT/SIGTERM requests a final flush after bounded I/O. A forced kill or failed
flush leaves captured rows in the SQLite spool. The Docker service intentionally
does not auto-restart on failure; fix access/connectivity/retention issues and
restart explicitly to avoid uncontrolled retries or billing. Its health check
indicates process liveness only, not successful delivery or market freshness.

### Replay and Failure Semantics

- SQLite commits captured envelopes before any upload. It locks out another
  process sharing the same spool and binds state to source, broker, group, topic,
  bucket, and region. Use distinct spools for distinct deployments.
- Uploads group a partition's records into bounded gzip NDJSON objects whose keys
  include offset range, first capture date, and SHA-256. Existing identical objects
  are reused; different bytes at the same key fail closed. Writers must follow
  this contract; IAM does not independently prevent overwrites.
- Kafka auto-commit is disabled. Each normal commit names only that successfully
  uploaded partition's next offset. Upload/commit failure aborts the process and
  retains the affected rows. Disk errors and oversized envelopes fail before
  committing source progress; adjust limits deliberately rather than drop records.
- Startup/rebalance recovery uploads pending rows without committing old offsets.
  Broker replay can then produce overlapping files with different capture times.
  Databricks must deduplicate `(source_id, topic, partition, offset)`; this is
  at-least-once delivery, not an exactly-once pipeline.
- The spool batches records, not a fixed immutable upload plan. Changing batch
  limits or runtime compression across recovery can change file boundaries/hashes
  but preserves transport identity. Duplicate storage is an accepted recovery cost.
- A source recreation requires a new `RAW_SOURCE_ID` and spool. Review group
  offsets explicitly; do not blindly reset progress to resolve a retention error.
  Loss of both local state and Kafka retention cannot be repaired by this consumer.
- Source bytes, null tombstones, duplicate headers, and Kafka timestamps are
  preserved. Malformed market data is not dropped; canonical parsing and
  quarantine will be implemented in Databricks.

The unit suite uses real SQLite/gzip with mocked Kafka/S3, covering interrupted
uploads, commit failure, restart/rebalance replay, size limits, and unchanged
legacy behavior. The Python 3.11 image is built and smoke-tested without network
access using SDK stubs. Live Kafka-to-S3 delivery and IAM are not yet verified.

## Gold Snapshot Contract v1

Start with complete snapshots of selected small datasets, initially
`daily_summaries` with key `(symbol, date)`. Pin Delta source versions before
export, establish an upstream batch boundary, and read using Delta APIs. No
loader may scan the physical files beneath a Delta table as ordinary Parquet.

Use `publish/batches/<batch_id>/<dataset>/part-*.parquet` and write
`publish/batches/<batch_id>/manifest.json` last. The batch ID identifies one
immutable publication; a retry cannot reuse it for different data. Data files
can exist before the manifest, but consumers must ignore that incomplete batch.

The manifest must include:

- `manifest_version: 1`, `status: completed`, `batch_id`, and `created_at`.
- A source table/version map and an explicit ordered publication sequence for
  the dataset group. Serialize publication; do not infer ordering from UUIDs.
- A `datasets` list with dataset/schema identity, ordered column names and
  logical types/nullability, business keys, and total row count.
- Each dataset's exact file keys, sizes, SHA-256 checksums, and per-file row
  counts. ETags are not universally content hashes; record actual file hashes.
- The logical data cutoff and the producer job run ID for traceability. Multiple
  Delta tables are not automatically one transaction; verify their batch boundary.

Validate keys stay within this batch prefix, schema versions are supported, all
files exist, checksums/counts match, and business keys are unique. A zero-row
snapshot is valid only when explicitly declared and validated; distinguish it
from missing data. Never publish success based merely on a nonempty S3 prefix.

Snowflake loads the exact file list into isolated staging through a storage
integration and `COPY INTO`. After validation, transactional DML replaces the
selected small replicated datasets and records success in the same transaction.
Create staging/target objects beforehand: avoid implicitly committing DDL inside
the publication transaction. Complete replacement handles removed rows as well
as corrections. Reject stale publication sequences and make successful batch
replays a no-op. A failed load must leave the previous serving snapshot intact.

The first slice must finalize dataset-specific column/decimal/timestamp mappings
and executable manifest validation before implementing either producer or loader.
CDC, Snowpipe, and Streams/Tasks are a later explicit phase with their own
bootstrap, ordering, tombstone, retention, and recovery contract.

## Access and IaC Ownership

The optional [hybrid access template](../cloudformation/05-hybrid-access.yaml)
creates three **unattached** managed policies, importing the existing bucket ARN:

| Policy | Allowed S3 access |
|--------|-------------------|
| Local landing | Read/list/write `landing/`, multipart cleanup; no delete |
| Databricks storage | Read/list landing; read/write/delete lakehouse, hybrid checkpoints, and publish |
| Snowflake publish | Read/list only `publish/`, including object-version reads |

These policies prepare access, not a working identity integration. They do not
create users, access keys, role trust, Unity Catalog credentials, or Snowflake
integrations. IAM permits writers to overwrite objects: application contracts
and tests must enforce immutable publication. Do not attach a narrow policy to
the legacy bucket-wide user and assume that removes its existing broad grants.

| Resource ownership | Tool and implementation status |
|--------------------|--------------------------------|
| AWS S3, IAM policies, future platform trust roles | CloudFormation; S3/access-policy templates prepared |
| Databricks platform objects and permissions | Terraform planned after account/capability checks |
| Databricks jobs and code deployments | Versioned deployment bundles planned |
| Snowflake warehouse, integrations, roles and database/schema containers | Terraform planned |
| Snowflake tables, views, marts, schema evolution | Versioned SQL migrations planned |

Give every object and grant one owner; do not manage the same resource through
Terraform and bundles/SQL. Pin providers and secure Terraform state/locking;
never commit state, credentials, or sensitive variable files. No Terraform or
platform deployment scaffold is added until its vertical slice requires it.

## Preflight Before Deployment

- Confirm AWS account, region, existing bucket/stack names, and CloudFormation
  change-set permissions. Review lifecycle/bucket policies and any externally
  managed policy before applying the S3 update; protect current data and exports.
- Confirm Databricks workspace access, Unity Catalog support, permitted external
  S3 storage, service authentication, selected runtime, and supported terminating
  compute. Free Edition/trials may not support the required integration.
- Confirm Snowflake account/cloud/region, storage integration and role-creation
  privileges, service authentication, and X-Small warehouse pricing. Prefer a
  compatible AWS region to reduce unnecessary transfer and setup complexity.
- Obtain actual platform IAM principals/external IDs through each supported
  setup process. Keep trust policies separate from S3 permissions; do not guess
  account IDs or enable wildcard trust to get past bootstrap restrictions.
- Agree on a spending budget and authorized deployment window. Use manual jobs,
  finite run/query timeouts, one Snowflake warehouse with short auto-suspend, and
  no continuous refresh or cloud schedule. Monitors do not cap serverless costs.
- Verify local data-provider quotas/entitlements, Kafka retention, persistent
  consumer state, and UTC/exchange-date semantics. A laptop outage stops capture;
  checkpoints cannot recreate events never collected.

## Validation and Operations

Run `make validate-cfn` or its direct equivalent:

```text
cfn-lint cloudformation/*.yaml
```

This checks templates locally; it does not validate cloud grants, connectivity,
runtime support, or billing. Runtime contracts above still need implementation
tests in their owning migration steps.

The optional access stack is intentionally absent from the existing deployment
and teardown scripts. An authorized deployment must explicitly select the S3
stack then the hybrid access stack; review change sets before execution. Never
deploy the legacy IAM stack merely to obtain a hybrid identity: it generates
long-lived credentials and exposes them in stack outputs.

Do not use `make teardown` to pause the hybrid environment. The legacy script
empties buckets, including versions, before deleting stacks, even when resource
retention was intended. It neither handles the new access-stack dependency nor
stops remote compute. Deleting all project data/resources is an intentional
end-of-use requirement, not a defect in the legacy workflow. Safe cross-platform
pause/delete automation is deferred to operations;
until implemented, hybrid deployment requires an explicit reviewed procedure.

When cloud services are introduced, stop schedules first, cancel active
Databricks work, pause any enabled Snowflake pipes/tasks/refreshes, then suspend
unused warehouse compute. Keep data unless destruction is explicitly requested.
Before deleting the S3 stack, detach/delete consuming role resources and delete
the optional access stack that imports its ARN. Suspending compute does not stop
storage billing; resource removal is not the same as a recoverable rollback.

The new `raw-landing-state` Docker volume contains source messages and is retained
on ordinary stops. Full, explicitly confirmed destruction must include this
volume (or host `.state/`), Kafka state, S3 objects/versions, and subsequently added
Databricks/Snowflake resources. Do not restart an old spool against a freshly
emptied bucket after destructive teardown: it would re-upload pending records.
Stop writers before deleting data. Existing `docker compose down --volumes`
is destructive to all project volumes, not just the raw spool; use only when that
loss is intended. Cloud retention/Fail-safe and previously incurred charges can
outlive resource deletion.

## Next Slice

Implement a single Databricks ingestion/transformation job consuming the raw
envelope, after verifying account/runtime/storage capabilities and authorizing
any required resource deployment. Reconcile outputs before adding Snowflake.
Keep the current silver path as the default until the replacement is verified;
do not migrate every job or add CDC at once.