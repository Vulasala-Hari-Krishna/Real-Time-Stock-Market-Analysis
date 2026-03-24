FROM apache/airflow:2.8.1-python3.11

LABEL maintainer="stock-market-pipeline"
LABEL description="Airflow with project dependencies for batch orchestration"

USER airflow

COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip uninstall -y aiobotocore && \
    pip install --no-cache-dir -r /tmp/requirements.txt
