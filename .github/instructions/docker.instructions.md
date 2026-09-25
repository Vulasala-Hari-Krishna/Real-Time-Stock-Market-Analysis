---
applyTo: "docker/**"
description: "Use when changing local Docker services, Kafka, consumers, Airflow, Streamlit, or optional hybrid execution profiles."
---

# Docker Instructions

- Kafka, producer, consumer, Airflow/PostgreSQL, and Streamlit stay local as defined in [the repository architecture](../copilot-instructions.md). Databricks and Snowflake are remote services, not new application containers.
- Keep the existing local Spark services working during migration. Make legacy-only services optional only when the replacement is verified; the consumer may still need local Spark even after batch jobs migrate.
- Reuse existing Dockerfiles and build contexts, pin compatible image/package versions, and never use `latest`. Use multi-stage builds where useful and non-root runtime users.
- Add health checks to long-lived services and completion checks to one-shot initialization tasks. Use `depends_on` health/completion conditions plus client retries; startup ordering alone is not runtime readiness.
- Use named volumes for Kafka state, Airflow metadata, and any implemented consumer checkpoint/live-cache state. Do not remove volumes as a routine stop operation.
- Bind host-facing ports to loopback by default. Use Compose service DNS inside containers and localhost only for host clients; do not expose Kafka, databases, or Airflow publicly for cloud access.
- Configure outbound HTTPS access to S3, Databricks, and Snowflake. No inbound cloud-to-laptop connections should be required.
- Keep non-secret settings configurable and inject local secrets via ignored environment files, securely mounted credentials, or secret mechanisms. Never bake keys into images, build arguments, logs, or committed Compose values.
- Keep Airflow provider and SDK dependencies compatible and scoped to services needing them. Do not add Databricks runtime internals to local images or make tests require a cloud account.
- Long-running containers must flush pending data and stop cleanly. Preserve producer kill switches and bounded demo runs; use resource limits appropriate for a personal laptop.
- New cloud-triggering services or schedules must be opt-in. `docker compose down` stops local services only; provide separate cloud cancel/pause/suspend procedures before claiming a complete kill switch.
- Validate Compose with `docker compose -f docker/docker-compose.yaml config --quiet` without starting services. Build/run only the affected services when needed and authorized; validation must not print resolved secrets.