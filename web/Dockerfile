# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

# --- Runtime env ---
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONHASHSEED=0 \
    DEBIAN_FRONTEND=noninteractive

ARG TZ=America/Los_Angeles
ENV TZ=${TZ}

WORKDIR /app

# 1) Install OS deps (no wget needed if compose supplies the healthcheck)
RUN apt-get update \
 && apt-get install -y --no-install-recommends tzdata ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 2) Install Python deps first for better cache hits
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

# 3) Copy application code (once)
COPY app /app/app

# 4) Pre-create static dir so runtime can be read-only
RUN mkdir -p /app/app/static

# 5) Run as non-root
RUN useradd -u 10001 -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 5780

# 6) Do NOT define healthcheck here; compose already has one
# (If you insist, prefer a Python-based check that doesn't require wget/curl)

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5780", "--proxy-headers"]
