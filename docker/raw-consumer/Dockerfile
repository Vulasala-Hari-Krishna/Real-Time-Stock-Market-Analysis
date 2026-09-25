FROM python:3.11-slim

RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid appuser --create-home appuser \
    && mkdir -p /var/lib/raw-landing \
    && chown appuser:appuser /var/lib/raw-landing

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    boto3==1.34.62 \
    pydantic==2.6.4 \
    pydantic-settings==2.2.1

COPY src/ src/

USER appuser

CMD ["python", "-m", "src.consumers.raw_landing"]