FROM python:3.11-slim AS base

LABEL maintainer="stock-market-pipeline"
LABEL description="Kafka producer for Alpha Vantage stock data"

RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --root-user-action=ignore \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    pydantic==2.6.4 \
    pydantic-settings==2.2.1 \
    boto3==1.34.62 \
    pandas==2.2.1 \
    pyarrow==15.0.2

COPY src/ src/

USER appuser

CMD ["python", "-m", "src.producers.stock_producer"]
