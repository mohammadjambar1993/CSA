#!/usr/bin/env python3
"""Common test fixtures for EDN Service Bus tests."""

import asyncio
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# We don't need to define an event_loop fixture as pytest-asyncio will handle it
# based on the asyncio_default_fixture_loop_scope = function setting in pytest.ini


@pytest.fixture
def mock_redis_client():
    """Mock Redis client fixture."""
    redis_client = MagicMock()
    redis_client.setex = MagicMock()
    redis_client.get = MagicMock()
    return redis_client


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer fixture."""
    producer = MagicMock()
    producer.start = MagicMock()
    producer.stop = MagicMock()
    producer.send = MagicMock()
    producer.send_and_wait = MagicMock()
    return producer


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer fixture."""
    consumer = MagicMock()
    consumer.start = MagicMock()
    consumer.stop = MagicMock()
    return consumer


@pytest.fixture
def sample_message():
    """Sample Kafka message fixture."""
    message = MagicMock()
    message.key = b"device123:1633046400000:0001"
    message.value = b'{"data": "sample message content"}'
    message.topic = "test-topic"
    message.partition = 0
    message.offset = 1
    return message
