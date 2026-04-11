FROM apache/airflow:2.8.1-python3.11

LABEL maintainer="stock-market-pipeline"
LABEL description="Airflow with project dependencies for batch orchestration"

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip uninstall -y aiobotocore && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Make spark-submit binary available on PATH
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
