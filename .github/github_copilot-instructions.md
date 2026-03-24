# Stock Market Real-Time Analytics Pipeline — Copilot Instructions

## Project Overview

This is an **end-to-end real-time stock market analytics platform** that:

1. **Ingests** live stock data from Alpha Vantage API via a Kafka producer
2. **Streams** data through Apache Kafka into Spark Structured Streaming
3. **Processes** real-time indicators (SMA, EMA, RSI, MACD, volume anomalies)
4. **Stores** data in AWS S3 using a medallion architecture (Bronze → Silver → Gold)
5. **Orchestrates** batch ETL with Apache Airflow (historical backfill, daily aggregation, data quality)
6. **Visualizes** insights via a Streamlit dashboard
7. **Queries** data via AWS Athena over a Glue Data Catalog

All infrastructure runs **locally via Docker Compose** (Kafka, Spark, Airflow, Streamlit) and uses **AWS free tier only** (S3, Glue, Athena). AWS resources are managed entirely through **CloudFormation**. CI/CD is handled by **GitHub Actions**.

## Architecture

```
Local (Docker Compose):
  Alpha Vantage API → Kafka Producer → Kafka Broker → Spark Streaming → S3 (Silver)
  Yahoo Finance     → Airflow DAGs   → Spark Batch  → S3 (Gold)
  Streamlit Dashboard ← reads from S3 Gold / local Parquet

AWS Cloud (Free Tier):
  S3 (bronze/ silver/ gold/) → Glue Catalog → Athena (SQL queries)
  Databricks Community Edition (notebooks, optional)
```

## Stock Watchlist

The pipeline processes a configurable list of 5-10 stocks:

```python
WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "JNJ"]
```

This is NOT an all-market scanner. The watchlist is defined in `src/config/watchlist.py`.

## Data Sources

- **Alpha Vantage API** (free tier, 25 req/day): Real-time quotes, intraday OHLCV, technical indicators, news sentiment
- **Yahoo Finance / yfinance** (free, no key): Historical daily OHLCV, fundamentals, company info, dividends

## Medallion Architecture (S3)

| Layer | Path | Format | Contents |
|-------|------|--------|----------|
| Bronze | `s3://bucket/bronze/` | JSON | Raw API responses, as-is |
| Silver | `s3://bucket/silver/` | Parquet, partitioned by date/ticker | Cleaned, deduplicated, typed, with computed indicators |
| Gold | `s3://bucket/gold/` | Parquet | Aggregated: daily summaries, sector rollups, signals, screening tables |

## Key Technical Indicators Computed

- **SMA** (Simple Moving Average): 20-day, 50-day, 200-day
- **EMA** (Exponential Moving Average): 12-day, 26-day
- **RSI** (Relative Strength Index): 14-period
- **MACD**: 12/26/9 standard
- **Bollinger Bands**: 20-period, 2 std dev
- **Volume anomaly**: flag when volume > 2× 20-period average
- **Golden Cross / Death Cross**: SMA-50 vs SMA-200 crossover detection

## Kill Switch / Demo Mode

The pipeline has a **kill switch** at every level:
- `RUN_PIPELINE` env var: set to `false` to disable the Kafka producer
- `MAX_ITERATIONS` env var: auto-stop after N polling cycles (e.g., `MAX_ITERATIONS=5` for demo)
- `docker compose down`: stops all local services instantly
- `make teardown`: destroys all AWS CloudFormation stacks
- GitHub Actions `teardown-infra.yaml`: manual-trigger-only workflow to destroy AWS resources

## Technology Stack

| Layer | Tool | Runs On |
|-------|------|---------|
| Ingestion | Apache Kafka (Confluent Docker images) | Local Docker |
| Stream Processing | Spark Structured Streaming 3.5 | Local Docker |
| Batch Processing | Spark (PySpark) | Local Docker |
| Orchestration | Apache Airflow 2.8+ | Local Docker |
| Cloud Storage | AWS S3 | AWS Free Tier |
| Data Catalog | AWS Glue | AWS Free Tier |
| SQL Query Engine | AWS Athena | AWS Free Tier |
| Analytics | Databricks Community Edition | Free |
| Dashboard | Streamlit | Local Docker |
| IaC | AWS CloudFormation | YAML templates |
| CI/CD | GitHub Actions | Free |
| Containerization | Docker + Docker Compose | Local |

## Project Structure

```
stock-market-pipeline/
├── cloudformation/          # ALL AWS resources as CloudFormation YAML
│   ├── 01-s3-datalake.yaml
│   ├── 02-glue-catalog.yaml
│   ├── 03-iam-roles.yaml
│   ├── 04-athena-workgroup.yaml
│   ├── deploy-all.sh
│   └── teardown-all.sh
├── .github/
│   └── workflows/           # CI/CD pipelines
│       ├── ci.yaml          # Lint + test + coverage on every PR
│       ├── deploy-infra.yaml
│       ├── build-images.yaml
│       └── teardown-infra.yaml
├── docker/
│   ├── docker-compose.yaml  # Full local stack
│   ├── kafka-producer/Dockerfile
│   ├── spark-jobs/Dockerfile
│   ├── airflow/Dockerfile
│   └── dashboard/Dockerfile
├── src/
│   ├── config/              # Settings from env vars, watchlist
│   ├── producers/           # Kafka producer (Alpha Vantage → Kafka)
│   ├── consumers/           # Spark Structured Streaming
│   ├── batch/               # Spark batch ETL (backfill, aggregation)
│   └── common/              # Shared schemas, S3 utils, indicator math
├── dags/                    # Airflow DAGs
├── dashboards/              # Streamlit app
├── tests/
│   ├── unit/                # Fast, hermetic, mocked — ≥80% coverage
│   └── integration/         # Compose modules, controlled IO
├── notebooks/               # Databricks exploration notebooks
├── scripts/                 # Automation scripts (setup, demo, seed)
└── Makefile                 # Single entry point for all operations
```

## Core Engineering Principles

- **DRY**: No duplication in production or test code. Extract shared utilities.
- **TDD**: Write a failing test first, then implement minimal code to pass.
- **12-Factor**: All config via env vars. No hardcoded secrets. Stateless processes. Logs to stdout.
- **Security**: Validate all API inputs. Secrets in `.env` (local) or GitHub Secrets (CI). Least-privilege IAM.
- **Coverage**: ≥80% on all new/changed code. Unit tests are fast, pure, use mocks for IO/network/time.
- **Modular design**: Small, testable units with clear interfaces. Single responsibility.
- **Documentation**: Update README.md with every meaningful change. Docstrings on all public functions.

## Code Style

- **Python 3.11+**
- **Formatter**: Black (line length 88)
- **Linter**: Ruff
- **Type checker**: MyPy (ignore-missing-imports)
- **Naming**: snake_case for functions/variables, PascalCase for classes
- **Imports**: stdlib → third-party → local, separated by blank lines
- **Docstrings**: Google-style on all public functions and classes

## How to Build — Phase Order

Build this project in the following order:

### Phase 1: Foundation
1. Project scaffolding (directory structure, Makefile, .env.example, .gitignore)
2. `src/config/settings.py` — all configuration from environment variables
3. `src/config/watchlist.py` — stock watchlist definition
4. `src/common/schemas.py` — Pydantic models for stock tick, OHLCV, indicators
5. `src/common/indicators.py` — pure functions for SMA, EMA, RSI, MACD, Bollinger Bands
6. Unit tests for schemas and indicators (TDD)

### Phase 2: Infrastructure
7. CloudFormation templates (S3, Glue, IAM, Athena)
8. `cloudformation/deploy-all.sh` and `teardown-all.sh`
9. GitHub Actions: `ci.yaml`, `deploy-infra.yaml`, `teardown-infra.yaml`

### Phase 3: Ingestion
10. Docker Compose (Kafka + Zookeeper)
11. `src/producers/stock_producer.py` — Kafka producer with kill switch
12. `scripts/create-kafka-topics.sh`
13. Unit tests for producer (mock API calls, mock Kafka)

### Phase 4: Stream Processing
14. Add Spark to Docker Compose
15. `src/consumers/spark_streaming.py` — consume from Kafka, compute indicators, write to S3
16. `src/common/s3_utils.py` — S3 read/write helpers
17. Unit tests for streaming logic (mock Spark session, mock S3)

### Phase 5: Batch Processing
18. `src/batch/historical_backfill.py` — yfinance → S3 Bronze
19. `src/batch/daily_aggregation.py` — Bronze → Silver → Gold transforms
20. `src/batch/fundamental_enrichment.py` — company fundamentals
21. Unit tests for all batch jobs

### Phase 6: Orchestration
22. Add Airflow to Docker Compose
23. `dags/daily_historical_backfill.py`
24. `dags/daily_batch_aggregation.py`
25. `dags/data_quality_checks.py`
26. `dags/fundamental_data_refresh.py`

### Phase 7: Visualization & Analytics
27. `dashboards/app.py` — Streamlit dashboard
28. Databricks notebooks for exploration
29. Athena query examples

### Phase 8: Polish
30. README.md (comprehensive)
31. Integration tests
32. CI/CD workflows finalized
33. Demo script (`scripts/run-demo.sh`)

## Important Constraints

- **Free resources only**: No AWS MWAA, no AWS MSK, no paid services
- **Local Docker for Kafka, Spark, Airflow**: These do NOT run on AWS
- **AWS is only for storage + catalog + query**: S3, Glue, Athena
- **Alpha Vantage free tier**: 25 requests/day — design producer to respect rate limits
- **Kill switch mandatory**: Every long-running process must be stoppable via env var or docker compose down
- **No hardcoded secrets**: API keys, AWS credentials — all from env vars
- **Parquet format**: Use Parquet for Silver and Gold layers (columnar, compressed, Athena-compatible)
- **Partitioning**: Silver and Gold data partitioned by `year/month` or `year/month/day`