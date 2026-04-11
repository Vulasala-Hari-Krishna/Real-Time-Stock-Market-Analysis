FROM python:3.11-slim

LABEL maintainer="stock-market-pipeline"
LABEL description="Spark 3.5.3 standalone cluster node with Python 3.11"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
        procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    | tar -xz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}"

RUN groupadd --gid 1000 spark && \
    useradd --uid 1000 --gid spark --shell /bin/bash --create-home spark && \
    chown -R spark:spark "${SPARK_HOME}"

USER spark
WORKDIR ${SPARK_HOME}
