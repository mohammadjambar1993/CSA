# Use an official Python runtime as a parent image
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY env.conf .
COPY src/ .

ENV PYTHONUNBUFFERED 1

CMD ["python", "sync_redis_to_redis.py"] 