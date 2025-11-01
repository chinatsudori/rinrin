# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Use noninteractive tzdata install to avoid prompts
ARG TZ=UTC
ENV TZ=${TZ} DEBIAN_FRONTEND=noninteractive

# System deps in one layer; clean up properly
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      tzdata ffmpeg locales ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Locales
RUN sed -i 's/# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
 && locale-gen
ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 PYTHONIOENCODING=UTF-8

# App user
RUN useradd -u 10001 -m appuser

WORKDIR /app

# Leverage BuildKit cache for faster pip installs
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy code with correct ownership in one go
COPY --chown=appuser:appuser yuribot ./yuribot

# Data dir owned by app user
RUN mkdir -p /app/data && chown -R appuser:appuser /app
VOLUME ["/app/data"]

USER appuser

# Lightweight healthcheck: import package only
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import importlib; importlib.import_module('yuribot'); print('ok')"

# Run the bot
CMD ["python", "-m", "yuribot"]
