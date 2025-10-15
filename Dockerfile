# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG TZ=UTC
ENV TZ=${TZ}

RUN apt-get update && apt-get install -y --no-install-recommends tzdata ffmpeg locales ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -u 10001 -m appuser

RUN sed -i 's/# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 PYTHONIOENCODING=UTF-8

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
