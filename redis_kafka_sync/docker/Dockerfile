FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY env.conf .

# Make scripts executable
RUN chmod +x /app/src/reactive_producer.py
RUN chmod +x /app/src/redis_stream_monitor.py

# Default command
CMD ["python", "/app/src/reactive_producer.py"]
