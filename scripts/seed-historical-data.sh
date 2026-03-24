#!/usr/bin/env bash
# seed-historical-data.sh — Quick historical data seed.
#
# Runs the historical_backfill.py PySpark job directly (bypassing Airflow)
# to populate bronze + silver layers with 5 years of OHLCV data.
#
# Usage: bash scripts/seed-historical-data.sh
set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

COMPOSE_FILE="docker/docker-compose.yaml"

# ── Check Spark is running ──────────────────────────────────────────
if ! docker inspect --format='{{.State.Health.Status}}' stock-spark-master 2>/dev/null | grep -q "healthy"; then
    info "Spark master not running. Starting required services..."
    docker compose -f "${COMPOSE_FILE}" up -d spark-master spark-worker
    info "Waiting for Spark master to become healthy..."
    for _ in $(seq 1 24); do
        if docker inspect --format='{{.State.Health.Status}}' stock-spark-master 2>/dev/null | grep -q "healthy"; then
            break
        fi
        sleep 5
    done
fi

info "Spark master is healthy."

# ── Run historical backfill ─────────────────────────────────────────
info "Running historical backfill (bronze + silver layers)..."
docker compose -f "${COMPOSE_FILE}" run --rm \
    -e RUN_PIPELINE=true \
    spark-streaming \
    spark-submit \
        --master spark://spark-master:7077 \
        /app/src/batch/historical_backfill.py

info "Historical backfill complete."

# ── Optionally run enrichment ───────────────────────────────────────
read -rp "Run daily aggregation + fundamental enrichment? [y/N] " ENRICH
if [[ "${ENRICH}" =~ ^[Yy]$ ]]; then
    info "Running daily aggregation (gold layer)..."
    docker compose -f "${COMPOSE_FILE}" run --rm \
        spark-streaming \
        spark-submit \
            --master spark://spark-master:7077 \
            /app/src/batch/daily_aggregation.py

    info "Running fundamental enrichment (gold layer)..."
    docker compose -f "${COMPOSE_FILE}" run --rm \
        spark-streaming \
        spark-submit \
            --master spark://spark-master:7077 \
            /app/src/batch/fundamental_enrichment.py

    info "All enrichment jobs complete."
fi

info "Data seeding finished. Start the dashboard with: make start"
