#!/usr/bin/env python3
"""
Unit tests for the ReactiveKafkaProducer class.
"""

import asyncio
import os
import sys
import unittest.mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import classes to test
from producer.reactive_producer import ReactiveKafkaProducer


@pytest.mark.asyncio
@patch("producer.reactive_producer.AIOKafkaProducer")
async def test_start(mock_producer_class):
    """Test starting the producer."""
    # Setup
    mock_producer = AsyncMock()
    mock_producer_class.return_value = mock_producer

    # Test
    producer = ReactiveKafkaProducer()
    await producer.start()

    # Verify
    mock_producer_class.assert_called_once()
    mock_producer.start.assert_called_once()
    assert producer.producer == mock_producer


@pytest.mark.asyncio
@patch("producer.reactive_producer.AIOKafkaProducer")
async def test_stop(mock_producer_class):
    """Test stopping the producer."""
    # Setup
    mock_producer = AsyncMock()
    mock_producer_class.return_value = mock_producer

    # Test
    producer = ReactiveKafkaProducer()
    await producer.start()
    await producer.stop()

    # Verify
    mock_producer.stop.assert_called_once()


@pytest.mark.asyncio
@patch("producer.reactive_producer.AIOKafkaProducer")
@patch("producer.reactive_producer.cache_message")
async def test_send_message_success(mock_cache, mock_producer_class):
    """Test sending a message successfully."""
    # Setup
    mock_producer = AsyncMock()
    mock_record_metadata = MagicMock()
    mock_record_metadata.topic = "test-topic"
    mock_record_metadata.partition = 0
    mock_record_metadata.offset = 123

    mock_producer.send_and_wait.return_value = mock_record_metadata
    mock_producer_class.return_value = mock_producer

    mock_cache.return_value = True

    # Test
    producer = ReactiveKafkaProducer()
    await producer.start()

    topic = "test-topic"
    message = b"test message"
    device_id = "vest.device.123"

    key = await producer.send_message(topic, message, device_id)

    # Verify
    assert key is not None
    assert key.startswith(device_id)

    # Check that send_and_wait was called with the right arguments
    mock_producer.send_and_wait.assert_called_once()
    call_args = mock_producer.send_and_wait.call_args[1]
    assert call_args["topic"] == topic
    assert call_args["key"] == key
    assert call_args["value"] == message

    # Check that cache_message was called with the right arguments
    mock_cache.assert_called_once_with(key, message)


@pytest.mark.asyncio
@patch("producer.reactive_producer.AIOKafkaProducer")
@patch("producer.reactive_producer.cache_message")
async def test_send_message_kafka_error(mock_cache, mock_producer_class):
    """Test handling Kafka errors when sending a message."""
    # Setup
    mock_producer = AsyncMock()
    mock_producer.send_and_wait.side_effect = Exception("Kafka error")
    mock_producer_class.return_value = mock_producer

    # Test
    producer = ReactiveKafkaProducer()
    await producer.start()

    topic = "test-topic"
    message = b"test message"
    device_id = "vest.device.123"

    key = await producer.send_message(topic, message, device_id)

    # Verify
    assert key is None
    mock_producer.send_and_wait.assert_called_once()
    mock_cache.assert_not_called()


@pytest.mark.asyncio
@patch("producer.reactive_producer.AIOKafkaProducer")
@patch("producer.reactive_producer.cache_message")
async def test_send_batch(mock_cache, mock_producer_class):
    """Test sending a batch of messages."""
    # Setup
    mock_producer = AsyncMock()
    mock_record_metadata = MagicMock()
    mock_record_metadata.topic = "test-topic"
    mock_record_metadata.partition = 0
    mock_record_metadata.offset = 123

    mock_producer.send_and_wait.return_value = mock_record_metadata
    mock_producer_class.return_value = mock_producer

    mock_cache.return_value = True

    # Test
    producer = ReactiveKafkaProducer()
    await producer.start()

    topic = "test-topic"
    device_id = "vest.device.123"
    count = 5

    successful = await producer.send_batch(topic, device_id, count)

    # Verify
    assert successful == count
    assert mock_producer.send_and_wait.call_count == count
    assert mock_cache.call_count == count


@pytest.mark.asyncio
@patch("producer.reactive_producer.os.path.isfile")
@patch("producer.reactive_producer.os.path.getsize")
@patch(
    "producer.reactive_producer.open",
    new_callable=unittest.mock.mock_open,
    read_data=b"test file content",
)
@patch("producer.reactive_producer.ReactiveKafkaProducer")
async def test_file_producer_success(
    mock_producer_class, mock_open, mock_getsize, mock_isfile
):
    """Test producing a file successfully."""
    # Setup
    mock_isfile.return_value = True
    mock_getsize.return_value = 1000  # Smaller than max request size

    mock_producer_instance = AsyncMock()
    mock_producer_class.return_value = mock_producer_instance
    mock_producer_instance.send_message.return_value = "test-key"

    # Test
    topic = "test-topic"
    file_path = "/path/to/test.txt"
    device_id = "test.device.123"

    from producer.reactive_producer import file_producer

    success = await file_producer(topic, file_path, device_id)

    # Verify
    assert success is True
    mock_isfile.assert_called_once_with(file_path)
    mock_getsize.assert_called_once_with(file_path)
    mock_open.assert_called_once_with(file_path, "rb")
    mock_producer_class.assert_called_once()
    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.send_message.assert_called_once_with(
        topic, b"test file content", device_id
    )
    mock_producer_instance.stop.assert_called_once()


@pytest.mark.asyncio
@patch("producer.reactive_producer.os.path.isfile")
async def test_file_producer_file_not_found(mock_isfile):
    """Test file_producer when the file doesn't exist."""
    # Setup
    mock_isfile.return_value = False

    # Test
    topic = "test-topic"
    file_path = "/path/to/nonexistent.txt"

    from producer.reactive_producer import file_producer

    success = await file_producer(topic, file_path)

    # Verify
    assert success is False
    mock_isfile.assert_called_once_with(file_path)


@pytest.mark.asyncio
@patch("producer.reactive_producer.os.path.isfile")
@patch("producer.reactive_producer.os.path.getsize")
async def test_file_producer_file_too_large(mock_getsize, mock_isfile):
    """Test file_producer when the file is too large."""
    # Setup
    mock_isfile.return_value = True

    # Set file size larger than max request size
    from producer.reactive_producer import KAFKA_MAX_REQUEST_SIZE

    mock_getsize.return_value = KAFKA_MAX_REQUEST_SIZE + 1

    # Test
    topic = "test-topic"
    file_path = "/path/to/large.txt"

    from producer.reactive_producer import file_producer

    success = await file_producer(topic, file_path)

    # Verify
    assert success is False
    mock_isfile.assert_called_once_with(file_path)
    mock_getsize.assert_called_once_with(file_path)


@pytest.mark.asyncio
@patch("producer.reactive_producer.os.path.isfile")
@patch("producer.reactive_producer.os.path.getsize")
@patch(
    "producer.reactive_producer.open",
    new_callable=unittest.mock.mock_open,
    read_data=b"test file content",
)
@patch("producer.reactive_producer.ReactiveKafkaProducer")
async def test_file_producer_error_during_send(
    mock_producer_class, mock_open, mock_getsize, mock_isfile
):
    """Test file_producer handling errors during message sending."""
    # Setup
    mock_isfile.return_value = True
    mock_getsize.return_value = 1000  # Smaller than max request size

    mock_producer_instance = AsyncMock()
    mock_producer_class.return_value = mock_producer_instance
    mock_producer_instance.send_message.return_value = None  # Indicates failure

    # Test
    topic = "test-topic"
    file_path = "/path/to/test.txt"

    from producer.reactive_producer import file_producer

    success = await file_producer(topic, file_path)

    # Verify
    assert success is False
    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.send_message.assert_called_once()
    mock_producer_instance.stop.assert_called_once()


@pytest.mark.asyncio
@patch("producer.reactive_producer.AIOKafkaProducer")
async def test_producer_with_default_device_id(mock_producer_class):
    """Test sending a message with a default device ID."""
    # Setup
    mock_producer = AsyncMock()
    mock_record_metadata = MagicMock()
    mock_producer.send_and_wait.return_value = mock_record_metadata
    mock_producer_class.return_value = mock_producer

    # Test
    producer = ReactiveKafkaProducer()
    await producer.start()

    topic = "test-topic"
    message = b"test message"
    # Not providing device_id

    with patch("producer.reactive_producer.cache_message") as mock_cache:
        key = await producer.send_message(topic, message)

    # Verify
    assert key is not None
    assert "unknown.device." in key
    mock_producer.send_and_wait.assert_called_once()
