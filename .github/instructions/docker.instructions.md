---
applyTo: "docker/**"
---

# Docker Instructions

- Use specific image tags, never `latest` (e.g., `confluentinc/cp-kafka:7.5.0`)
- Each service gets its own Dockerfile in `docker/{service}/Dockerfile`
- Use multi-stage builds where possible to reduce image size
- Run containers as non-root users
- Use health checks for all services
- Docker Compose services must declare `depends_on` with proper ordering
- Use named volumes for data persistence (Kafka data, Airflow metadata DB)
- Expose only necessary ports
- Use `.env` file for all configurable values (API keys, bucket names, etc.)
- All services must stop cleanly with `docker compose down` (no orphaned processes)