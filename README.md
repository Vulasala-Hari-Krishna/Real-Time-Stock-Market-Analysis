# Real-Time Stock Market Analytics Pipeline

[![CI](https://github.com/Vulasala-Hari-Krishna/Real-Time-Stock-Market-Analysis/actions/workflows/ci.yaml/badge.svg)](https://github.com/Vulasala-Hari-Krishna/Real-Time-Stock-Market-Analysis/actions/workflows/ci.yaml)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An end-to-end **Lambda Architecture** stock market analytics platform that
ingests live quotes from Alpha Vantage, streams them through Kafka and Spark
Structured Streaming, stores data in a **medallion architecture** (bronze →
silver → gold) on **AWS S3**, orchestrates batch enrichment with **Airflow**,
and surfaces interactive dashboards — **live** and **historical** — via
**Streamlit**.  All containerised with Docker and deployed through
**CloudFormation**.

---

## Architecture (Lambda Architecture)

```
                           SPEED LAYER (real-time)
┌────────────────┐   ┌───────────┐   ┌──────────────────┐   ┌────────────────────┐
│  Alpha Vantage │──▶│   Kafka   │──▶│  Spark Structured │──▶│ S3 silver/          │
│  (quotes API)  │   │  Broker   │   │  Streaming        │   │ stock_ticks         │
└───────┬────────┘   └───────────┘   └──────────────────┘   │ (cleaned Parquet)   │
        │                                                    └────────┬───────────┘
        │  raw backup                                                 │
        └──────────▶ S3 bronze/                      ┌────────────────┤
                                                     │ Dashboard      │
                           BATCH LAYER               │ Live Data tab  │
┌──────────────────────────────────────────┐         └────────────────┘
│  Airflow DAGs (daily)                    │
│                                          │
│  ┌─────────────────┐  ┌───────────────┐  │
│  │ Tick Rollup     │  │ Daily         │  │   ┌────────────────────┐
│  │ ticks → daily   │─▶│ Aggregation   │──│──▶│ S3 gold/           │
│  │ OHLCV bars      │  │ + Enrichment  │  │   │ (indicators,       │
│  └─────────────────┘  └───────────────┘  │   │  signals, sector)  │
│                                          │   └────────┬───────────┘
└──────────────────────────────────────────┘            │
                                              ┌────────┴───────────┐
   One-time seed:                             │ Dashboard          │
   yfinance 5-year ──▶ S3 silver/historical   │ Historical tabs    │
                                              └────────────────────┘
```

---

## Tech Stack

| Layer             | Technology                         |
|-------------------|------------------------------------|
| Ingestion         | Alpha Vantage API, Kafka 7.5       |
| Stream Processing | Spark Structured Streaming 3.5     |
| Batch Processing  | PySpark 3.5                        |
| Orchestration     | Apache Airflow 2.8                 |
| Storage           | AWS S3 (medallion architecture)    |
| Catalog / Query   | AWS Glue, Amazon Athena            |
| Dashboard         | Streamlit 1.32, Plotly 5.19        |
| Notebooks         | Databricks (PySpark)               |
| Infrastructure    | AWS CloudFormation, Docker Compose |
| CI/CD             | GitHub Actions                     |
| Language          | Python 3.11                        |

---

## Prerequisites

| Tool        | Version | Purpose                              |
|-------------|---------|--------------------------------------|
| Docker      | 24+     | Containerised services               |
| Python      | 3.11+   | Local development & testing          |
| AWS CLI     | 2.x     | Deploy CloudFormation / S3 access    |
| API Key     | —       | Free Alpha Vantage key ([get one](https://www.alphavantage.co/support/#api-key)) |

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/Vulasala-Hari-Krishna/Real-Time-Stock-Market-Analysis.git
cd Real-Time-Stock-Market-Analysis

# 2. One-time setup (checks prerequisites, creates .env, builds images)
bash scripts/setup-local.sh

# 3. Fill in your keys in .env
#    ALPHA_VANTAGE_API_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# 4. Start all services
make start

# 5. Open the dashboard
#    http://localhost:8501   — Streamlit analytics dashboard
#    http://localhost:8080   — Spark Master UI
#    http://localhost:8081   — Airflow UI  (admin / admin)

# 6. Stop everything
make stop
```

---

## Environment Variables

| Variable                  | Default                       | Description                      |
|---------------------------|-------------------------------|----------------------------------|
| `ALPHA_VANTAGE_API_KEY`   | —                             | Alpha Vantage API key            |
| `AWS_ACCESS_KEY_ID`       | —                             | AWS IAM access key               |
| `AWS_SECRET_ACCESS_KEY`   | —                             | AWS IAM secret key               |
| `AWS_DEFAULT_REGION`      | `us-east-1`                   | AWS region (single source — see below) |
| `S3_BUCKET_NAME`          | `stock-market-datalake-bucket`| S3 data lake bucket              |
| `KAFKA_BROKER`            | `localhost:9092`              | Kafka bootstrap server           |
| `RUN_PIPELINE`            | `true`                        | Kill switch for the producer     |
| `MAX_ITERATIONS`          | `10`                          | Producer auto-stop limit         |
| `POLL_INTERVAL_SECONDS`   | `60`                          | Seconds between quote polls      |
| `ENVIRONMENT`             | `dev`                         | Environment name                 |

### Changing the AWS Region

All components read the region from a single source:

- **Local / Docker**: set `AWS_DEFAULT_REGION` in your `.env` file (e.g. `ap-south-1`).
  This flows to Python code (`settings.py`), shell scripts, and the dashboard automatically.
- **GitHub Actions**: when triggering the **Deploy** or **Teardown** workflow, enter the
  desired region in the `aws-region` input field. Alternatively, create a repository
  variable named `AWS_REGION` (Settings → Variables → Actions) so every run
  uses it by default.

---

## Running the Demo

```bash
# Automated demo — starts services, produces data, opens dashboard
make demo

# Or use the helper script
bash scripts/run-demo.sh
```

The demo runs the producer for 5 iterations, seeds historical data via
the initial backfill, and opens the Streamlit dashboard where you can
explore both live tick data and historical analytics.

---

## Deploying AWS Infrastructure

```bash
# Deploy all CloudFormation stacks (S3, Glue, IAM, Athena)
make deploy

# Validate templates first
make validate-cfn

# Tear down everything (cost protection!)
make teardown
```

> **Cost protection:** All resources use AWS free-tier eligible services.  
> Always run `make teardown` when you're done to avoid charges.

---

## Project Structure

```
Real-Time-Stock-Market-Analysis/
├── .github/
│   ├── instructions/          # Copilot coding instructions
│   └── workflows/             # GitHub Actions CI/CD
├── cloudformation/            # AWS CloudFormation templates
│   ├── 01-s3-datalake.yaml
│   ├── 02-glue-catalog.yaml
│   ├── 03-iam-roles.yaml
│   ├── 04-athena-workgroup.yaml
│   ├── deploy-all.sh
│   ├── teardown-all.sh
│   └── parameters/            # dev.json, prod.json
├── dags/                      # Airflow DAG definitions
│   ├── daily_tick_rollup.py       # Roll up real-time ticks → daily OHLCV bars
│   ├── daily_batch_aggregation.py # Indicators, signals, enrichment → gold
│   ├── data_quality_checks.py     # Freshness, completeness, null, schema
│   ├── fundamental_data_refresh.py# Weekly fundamentals refresh
│   └── initial_historical_backfill.py  # One-time 5-year seed (manual)
├── dashboards/                # Streamlit analytics dashboard
│   ├── app.py                 # Main app entry point
│   ├── data_loader.py         # S3 data reader + demo fallback
│   └── pages/
│       ├── live_data.py       # Real-time price board & intraday charts
│       ├── overview.py        # Market overview & signals
│       ├── stock_detail.py    # Individual stock deep-dive
│       └── sector_analysis.py # Sector heatmaps & correlations
├── docker/                    # Docker build contexts
│   ├── docker-compose.yaml    # All services orchestration
│   ├── airflow/               # Airflow image
│   ├── dashboard/             # Streamlit image
│   ├── kafka-producer/        # Producer image
│   └── spark-jobs/            # Spark jobs image
├── notebooks/                 # Databricks exploration notebooks
│   ├── 01_explore_bronze.py
│   ├── 02_silver_analysis.py
│   └── 03_gold_insights.py
├── scripts/                   # Automation helpers
│   ├── create-kafka-topics.sh
│   ├── run-demo.sh
│   ├── seed-historical-data.sh
│   └── setup-local.sh
├── src/                       # Application source code
│   ├── batch/                 # PySpark batch jobs
│   │   ├── tick_rollup.py         # Roll up ticks → daily OHLCV
│   │   ├── daily_aggregation.py   # Technical indicators & signals
│   │   ├── fundamental_enrichment.py # P/E, market cap enrichment
│   │   └── historical_backfill.py # One-time 5-year seed
│   ├── common/                # Shared utilities
│   │   ├── indicators.py      # Technical indicator functions
│   │   ├── s3_utils.py        # S3 read/write helpers
│   │   └── schemas.py         # Pydantic data models
│   ├── config/
│   │   ├── settings.py        # Pydantic env-var config
│   │   └── watchlist.py       # 10-stock watchlist
│   ├── consumers/
│   │   └── spark_streaming.py # Spark Structured Streaming
│   └── producers/
│       └── stock_producer.py  # Kafka quote producer
├── tests/
│   ├── conftest.py            # Shared fixtures
│   ├── integration/           # Integration tests
│   │   ├── test_kafka_spark_flow.py
│   │   └── test_s3_write_read.py
│   └── unit/                  # Unit tests (8 modules)
├── .env.example               # Environment variable template
├── LICENSE                    # MIT License
├── Makefile                   # Developer task runner
├── README.md                  # ← you are here
├── requirements.txt           # Production dependencies
└── requirements-dev.txt       # Dev/test dependencies
```

---

## Data Flow

### Speed Layer (real-time)

1. **Ingest** — `stock_producer.py` polls Alpha Vantage every 60 s, publishes
   JSON quotes to the `raw_stock_ticks` Kafka topic, and backs up raw data to
   S3 **bronze** layer.

2. **Stream** — `spark_streaming.py` reads from Kafka in micro-batches (every
   30 s), validates & cleans records, deduplicates, detects volume anomalies,
   and writes cleaned Parquet to `silver/stock_ticks`.

3. **Live Dashboard** — The **Live Data** tab reads directly from
   `silver/stock_ticks` and shows a real-time price board, intraday charts,
   today's movers, and pipeline health.

### Batch Layer (daily)

4. **Tick Rollup** — `tick_rollup.py` (06:00 UTC) reads real-time ticks from
   `silver/stock_ticks`, aggregates to daily OHLCV bars per symbol, deduplicates
   against existing data, and appends to `silver/historical`.

5. **Aggregation** — `daily_aggregation.py` (07:00 UTC) reads `silver/historical`,
   computes SMA, EMA, RSI, MACD, generates trading signals, builds sector
   rollups & correlations → **gold** layer.

6. **Enrichment** — `fundamental_enrichment.py` joins gold data with P/E,
   market cap, dividend yield from yfinance.

### One-Time Seed

7. **Historical Backfill** — `historical_backfill.py` downloads 5-year OHLCV
   history via yfinance, writes bronze JSON + silver Parquet.  Run once at
   project setup via the `initial_historical_backfill` DAG (manual trigger).
### Orchestration

8. **Airflow DAGs** — Five DAGs schedule all work:

   | DAG | Schedule | Purpose |
   |-----|----------|---------|
   | `initial_historical_backfill` | Manual (one-time) | Seed 5-year OHLCV history |
   | `daily_tick_rollup` | Daily 06:00 UTC | Ticks → daily OHLCV bars |
   | `daily_batch_aggregation` | Daily 07:00 UTC | Indicators + enrichment → gold |
   | `data_quality_checks` | Daily 08:00 UTC | Freshness, completeness, nulls |
   | `fundamental_data_refresh` | Weekly Sun 06:00 | Refresh company fundamentals |

### Serving Layer

9. **Dashboard** — Streamlit reads from both the speed and batch layers:
   - **Live Data** — real-time price board, intraday charts, volume monitor
   - **Market Overview** — watchlist table with colour-coded signals
   - **Stock Detail** — candlestick + SMA/RSI/volume charts
   - **Sector Analysis** — heatmaps & correlation matrices

---

## Key Insights Generated

| Insight                   | Description                                               |
|---------------------------|-----------------------------------------------------------|
| Real-Time Price Feed      | Live prices, intraday charts, today's movers via speed layer |
| Volume Anomalies          | Spikes > 2\u00d7 the 20-day average (both real-time and batch)  |
| Technical Signals         | SMA crossovers (golden/death cross), RSI overbought/oversold |
| MACD Momentum             | MACD line vs signal line divergence                       |
| Sector Performance        | Daily average returns per sector                          |
| Pairwise Correlations     | 30-day rolling correlation between all stock pairs        |
| Fundamental Screening     | Undervalued stocks (forward P/E below market average)     |
| Pipeline Health           | Live data freshness indicator on the dashboard            |

---

## Screenshots

> *Screenshots will be added after the first live run.*

| View                | Description                          |
|---------------------|--------------------------------------|
| Live Data           | ![live](docs/screenshots/live_data.png) |
| Market Overview     | ![overview](docs/screenshots/overview.png) |
| Stock Detail        | ![detail](docs/screenshots/stock_detail.png) |
| Sector Analysis     | ![sector](docs/screenshots/sector_analysis.png) |

---

## Testing

```bash
# Run all unit tests with coverage (≥80% required)
make test

# Verbose output
pytest tests/unit -v --cov=src --cov-report=term-missing --cov-fail-under=80

# Run integration tests (requires Docker services)
pytest tests/integration -v
```

The test suite covers 9 modules (260+ tests):
- Technical indicator calculations
- Pydantic schema validation
- S3 utility functions
- Kafka producer logic
- Spark streaming consumer
- PySpark batch jobs (backfill, aggregation, enrichment, tick rollup)

---

## CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yaml`) runs on every push
and pull request to `main`:

| Job                  | What it does                                         |
|----------------------|------------------------------------------------------|
| **Lint**             | ruff check, black --check, mypy                      |
| **Unit Tests**       | pytest with ≥80% coverage gate                       |
| **CFN Lint**         | Validates CloudFormation YAML                        |
| **Docker Build**     | Builds all Dockerfiles to verify no build errors     |

Additional manual-only workflows (triggered via GitHub Actions UI):
`build-images.yaml` (Docker image publishing),
`deploy-infra.yaml` (CloudFormation deployment), and
`teardown-infra.yaml` (stack teardown).

---

## Contributing

1. Fork the repo and create a feature branch.
2. Follow the coding conventions in `.github/instructions/`.
3. Write tests first (TDD) — maintain ≥80% coverage.
4. Run `make lint` and `make test` before pushing.
5. Open a pull request against `main`.

---

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE)
for details.