# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies that might be needed by Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir reduces image size
# Using virtualenvs is good practice but adds complexity; skipping for simplicity here
RUN pip install --no-cache-dir -r requirements.txt

# Copy the configuration file and source code into the container
COPY env.conf .
COPY src/ .

# Make port 1883 available to the world outside this container (if needed, usually not for client)
# EXPOSE 1883

# Define environment variable
ENV PYTHONUNBUFFERED 1

# Run sync_redis_with_mqtt.py when the container launches
CMD ["python", "sync_redis_with_mqtt.py"] 