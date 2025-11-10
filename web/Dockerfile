# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS base
WORKDIR /app
COPY . /app

# Create the directory before switching to read-only at runtime
RUN mkdir -p /app/app/static


ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     PIP_DISABLE_PIP_VERSION_CHECK=1     PYTHONHASHSEED=0

ARG TZ=America/Los_Angeles
ENV TZ=${TZ} DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends tzdata ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Expose FastAPI on 5780
EXPOSE 5780

# Healthcheck: hit the /health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://127.0.0.1:5780/health || exit 1

# Run the server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5780"]
