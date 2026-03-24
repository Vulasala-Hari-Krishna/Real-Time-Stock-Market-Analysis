FROM python:3.11-slim

LABEL maintainer="stock-market-pipeline"
LABEL description="Streamlit dashboard for stock market analytics"

RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

COPY dashboards/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY dashboards/ /app/dashboards/
COPY src/ /app/src/

USER appuser

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "/app/dashboards/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
