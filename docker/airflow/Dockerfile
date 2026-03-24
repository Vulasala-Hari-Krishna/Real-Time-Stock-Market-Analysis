FROM apache/airflow:2.8.1

LABEL maintainer="stock-market-pipeline"
LABEL description="Airflow with project dependencies for batch orchestration"

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
