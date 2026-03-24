#!/usr/bin/env bash
# Create Kafka topics for the stock market pipeline.
# Usage: ./create-kafka-topics.sh [bootstrap-server]
set -euo pipefail

BOOTSTRAP_SERVER="${1:-localhost:9092}"

TOPICS=(
    "raw_stock_ticks"
    "stock_indicators"
    "stock_alerts"
)

PARTITIONS=3
REPLICATION_FACTOR=1

echo "Creating Kafka topics on ${BOOTSTRAP_SERVER}..."

for topic in "${TOPICS[@]}"; do
    echo ">>> Creating topic: ${topic}"
    kafka-topics --create \
        --if-not-exists \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        --partitions "${PARTITIONS}" \
        --topic "${topic}"
done

echo ""
echo "Topics created:"
kafka-topics --list --bootstrap-server "${BOOTSTRAP_SERVER}"
