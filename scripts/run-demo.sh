#!/usr/bin/env bash
# run-demo.sh — Automated end-to-end demo for the stock market pipeline.
#
# Starts Docker Compose services, runs the producer for a limited number of
# iterations, triggers a historical backfill, prints a summary, and opens
# the Streamlit dashboard.
#
# Usage: bash scripts/run-demo.sh
set -euo pipefail

COMPOSE_FILE="docker/docker-compose.yaml"
MAX_WAIT=120  # seconds to wait for Kafka healthy

# ── Colours ──────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 1. Start services ───────────────────────────────────────────────
info "Starting Docker Compose services..."
docker compose -f "${COMPOSE_FILE}" up -d

# ── 2. Wait for Kafka healthy ───────────────────────────────────────
info "Waiting for Kafka to become healthy (up to ${MAX_WAIT}s)..."
elapsed=0
until docker inspect --format='{{.State.Health.Status}}' stock-kafka 2>/dev/null | grep -q "healthy"; do
    sleep 5
    elapsed=$((elapsed + 5))
    if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
        error "Kafka did not become healthy within ${MAX_WAIT}s"
        exit 1
    fi
done
info "Kafka is healthy."

# ── 3. Kafka topics (created by kafka-init, verify) ─────────────────
info "Verifying Kafka topics..."
docker exec stock-kafka kafka-topics --list --bootstrap-server localhost:9092

# ── 4. Run producer for 5 iterations ────────────────────────────────
info "Running Kafka producer for 5 iterations..."
docker exec -e MAX_ITERATIONS=5 -e RUN_PIPELINE=true stock-kafka-producer \
    python -m src.producers.stock_producer 2>&1 | tail -20 || warn "Producer exited (may be expected)."

# ── 5. Seed historical data (backfill) ──────────────────────────────
info "Running historical backfill via Spark..."
docker exec stock-spark-streaming spark-submit \
    --master spark://spark-master:7077 \
    /app/src/batch/historical_backfill.py 2>&1 | tail -20 || warn "Backfill completed with warnings."

# ── 6. Summary ──────────────────────────────────────────────────────
echo ""
info "=========================================="
info "  Demo pipeline is running!"
info "=========================================="
echo ""
info "Services:"
info "  Streamlit Dashboard : http://localhost:8501"
info "  Spark Master UI     : http://localhost:8080"
info "  Airflow UI          : http://localhost:8081  (admin / admin)"
echo ""
info "Kafka topics:"
docker exec stock-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || true
echo ""

# ── 7. Wait for user ────────────────────────────────────────────────
read -rp "Press Enter to stop all services..."

# ── 8. Tear down ────────────────────────────────────────────────────
info "Stopping Docker Compose services..."
docker compose -f "${COMPOSE_FILE}" down
info "Done. All services stopped."
