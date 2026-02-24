# =============================================================================
# Python Producer — Ingest Simulator
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Prometheus metrics port
EXPOSE 8765

CMD ["python", "-u", "producer.py"]
