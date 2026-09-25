---
applyTo: "cloudformation/**/*.yaml"
description: "Use when changing AWS S3, IAM, lifecycle policies, storage integrations, or CloudFormation for the low-cost hybrid pipeline."
---

# CloudFormation Template Instructions

- CloudFormation owns AWS resources, not all Databricks/Snowflake objects. Follow [the repository architecture](../copilot-instructions.md); platform bundles and SQL manage those platforms separately.
- Keep AWS minimal: S3 and narrowly scoped IAM for the primary path. Glue/Athena remain optional legacy resources. Do not introduce MSK, MWAA, EMR, always-on EC2, NAT gateways, or paid networking as incidental dependencies.
- Use `AWSTemplateFormatVersion: '2010-09-09'`, a `Description`, parameterized `Environment` (dev/prod) and `ProjectName`, and consistent `{project}-{template-name}-{environment}` stack names.
- Use `!Sub`, `!Ref`, and exported outputs/`Fn::ImportValue` for real cross-stack dependencies. Tag resources supporting tags with at least `Project` and `Environment`.
- Block all public S3 access, enforce encryption at rest and TLS in transit, and scope IAM actions/resources to the required bucket prefixes. Do not put access keys or private keys in templates, parameters, or outputs.
- Separate local landing writers, Databricks storage roles, and Snowflake publish-prefix readers. Follow the supported trust/external-ID setup for each platform; obtain generated identity values explicitly rather than guessing ARNs or using wildcard trust.
- Choose compatible regions deliberately. Do not claim AWS or SaaS services are free based on free-tier eligibility; review request, storage, transfer, and networking costs.
- Separate `landing/`, lakehouse table storage, `checkpoints/`, and `publish/` lifecycle scopes. Never attach raw-file expiration policies to active Delta tables, transaction logs, or checkpoints.
- The existing `bronze/` expiration and `gold/` storage-class transition were designed for the legacy layout. Review them before placing new Delta data there; changing prefix meaning without adjusting retention risks data loss.
- Use table-aware Delta maintenance for obsolete table files. Expire raw/export files only after the ingestion/replay window; account for noncurrent versions, delete markers, and incomplete multipart uploads in versioned buckets.
- Prefer S3 Standard for this small active workload unless measured usage justifies another class. Avoid minimum-size/retrieval charges from automatic cold-tier transitions of tiny files.
- Retain production data with `DeletionPolicy` and `UpdateReplacePolicy`. Development deletion must also be deliberate; stack deletion does not empty a versioned bucket automatically.
- Add event-notification resources for Snowpipe only in its explicitly requested phase. Ensure lifecycle and notification prefixes exclude incomplete exports.
- Validate with the existing `make validate-cfn`/`cfn-lint` checks before any authorized deployment. CloudFormation teardown does not cancel Databricks runs or suspend Snowflake compute.