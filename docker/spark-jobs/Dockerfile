FROM apache/spark:3.5.3-python3

USER root

RUN pip install --no-cache-dir --root-user-action=ignore \
    boto3==1.34.62 \
    pydantic==2.6.4 \
    pydantic-settings==2.2.1 \
    pandas==2.2.1 \
    pyarrow==15.0.2

WORKDIR /app

COPY src/ src/

USER 185

ENTRYPOINT ["spark-submit"]
