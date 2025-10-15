# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG TZ=UTC
ENV TZ=${TZ}

RUN apt-get update && apt-get install -y --no-install-recommends tzdata ffmpeg ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -u 10001 -m appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY yuribot ./yuribot

RUN mkdir -p /app/data && chown -R appuser:appuser /app
VOLUME ["/app/data"]

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import importlib; importlib.import_module('yuribot'); print('ok')" || exit 1

# Run the bot
CMD ["python", "-m", "yuribot"]
