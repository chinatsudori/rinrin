# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONHASHSEED=0

# Noninteractive tzdata to avoid prompts
ARG TZ=America/Los_Angeles
ENV TZ=${TZ} DEBIAN_FRONTEND=noninteractive

# OS deps (ffmpeg needed for yt-dlp/audio), clean afterwards
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      tzdata ffmpeg locales ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Locales
RUN sed -i 's/# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
 && locale-gen
ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 PYTHONIOENCODING=UTF-8

# Non-root user
RUN useradd -u 10001 -m appuser

WORKDIR /app

# Install deps with cache
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --force-reinstall "wavelink==2.6.3"

# Code (owned by app user)
COPY --chown=appuser:appuser yuribot ./yuribot

# Data dir
RUN mkdir -p /app/data && chown -R appuser:appuser /app
VOLUME ["/app/data"]

USER appuser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import importlib; importlib.import_module('yuribot'); print('ok')"

# Entrypoint
CMD ["python", "-m", "yuribot"]
