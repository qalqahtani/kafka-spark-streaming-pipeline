# =============================================================================
# FastAPI — REST API
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

# Install system deps (curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Expose the API port and the Prometheus metrics port (both served by uvicorn)
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
