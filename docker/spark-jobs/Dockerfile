FROM python:3.11-slim

LABEL maintainer="stock-market-pipeline"
LABEL description="Spark submit client with Python 3.11 for stock market streaming jobs"

# Install Java runtime required by Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN groupadd --gid 1000 sparkuser \
    && useradd --uid 1000 --gid sparkuser --shell /bin/bash --create-home sparkuser

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    boto3==1.34.62 \
    pydantic==2.6.4 \
    pydantic-settings==2.2.1 \
    pandas==2.2.1 \
    pyarrow==15.0.2

WORKDIR /app

COPY src/ src/

USER sparkuser

ENTRYPOINT ["spark-submit"]
