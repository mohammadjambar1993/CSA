"""Unit tests for the ReactiveKafkaProducer class."""

import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.errors import KafkaError

# Import module to test
from producer.reactive_producer import ReactiveKafkaProducer


class TestReactiveKafkaProducer:
    """Test suite for ReactiveKafkaProducer class."""

    def setup_method(self):
        """Set up test environment before each test method."""
        self.bootstrap_servers = ["localhost:9092"]
        self.topic = "test-topic"
        self.redis_client = MagicMock()

        # Create a mock for AIOKafkaProducer
        self.aiokafka_producer_mock = MagicMock()
        self.aiokafka_producer_mock.start = AsyncMock()
        self.aiokafka_producer_mock.stop = AsyncMock()
        self.aiokafka_producer_mock.send = AsyncMock()
        self.aiokafka_producer_mock.send_and_wait = AsyncMock()

        # Patch the AIOKafkaProducer
        self.aiokafka_patcher = patch(
            "edn_service_bus.reactive.producer.AIOKafkaProducer",
            return_value=self.aiokafka_producer_mock,
        )
        self.mock_aiokafka = self.aiokafka_patcher.start()

        # Patch the cache_message function
        self.cache_message_patcher = patch(
            "edn_service_bus.reactive.producer.cache_message"
        )
        self.mock_cache_message = self.cache_message_patcher.start()

        # Patch the generate_message_key function
        self.generate_key_patcher = patch(
            "edn_service_bus.reactive.producer.generate_message_key",
            return_value="device123:1633046400000:0001",
        )
        self.mock_generate_key = self.generate_key_patcher.start()

        # Create the producer instance
        self.producer = ReactiveKafkaProducer(self.bootstrap_servers, self.redis_client)

    def teardown_method(self):
        """Tear down test environment after each test method."""
        self.aiokafka_patcher.stop()
        self.cache_message_patcher.stop()
        self.generate_key_patcher.stop()

    @pytest.mark.asyncio
    async def test_start(self):
        """Test that the producer starts correctly."""
        # Mock AIOKafkaProducer to avoid Kafka connection errors
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()

        with patch(
            "producer.reactive_producer.AIOKafkaProducer", return_value=mock_producer
        ):
            # Create a new producer instance for this test
            producer = ReactiveKafkaProducer(self.bootstrap_servers, self.redis_client)

            # Execute
            await producer.start()

            # Verify start was called
            mock_producer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop(self):
        """Test that the producer stops correctly."""
        # Mock AIOKafkaProducer to avoid Kafka connection errors
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()
        mock_producer.stop = AsyncMock()

        with patch(
            "producer.reactive_producer.AIOKafkaProducer", return_value=mock_producer
        ):
            # Create a new producer instance for this test
            producer = ReactiveKafkaProducer(self.bootstrap_servers, self.redis_client)

            # Start
            await producer.start()

            # Execute stop
            await producer.stop()

            # Verify stop was called
            mock_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_message_success(self):
        """Test that a message is sent and cached correctly."""
        # Setup
        message = b'{"data": "test message"}'  # Message should be bytes
        topic = self.topic
        device_id = "device123"
        expected_key = "device123:1633046400000:0001"

        # Mock AIOKafkaProducer to avoid Kafka connection errors
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        # Create a mock for record_metadata
        mock_metadata = MagicMock()
        mock_metadata.topic = topic
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        mock_producer.send_and_wait.return_value = mock_metadata

        # Mock cache message
        cache_mock = MagicMock(return_value=True)

        with patch(
            "producer.reactive_producer.AIOKafkaProducer", return_value=mock_producer
        ):
            with patch(
                "producer.reactive_producer.generate_message_key",
                return_value=expected_key,
            ):
                with patch("producer.reactive_producer.cache_message", cache_mock):
                    # Create a new producer instance for this test
                    producer = ReactiveKafkaProducer(
                        self.bootstrap_servers, self.redis_client
                    )

                    # Start the producer
                    await producer.start()

                    # Send the message
                    result = await producer.send_message(topic, message, device_id)

                    # Verify key was returned correctly
                    assert result == expected_key

                    # Verify producer.send_and_wait was called with correct parameters
                    mock_producer.send_and_wait.assert_called_once_with(
                        topic=topic, key=expected_key, value=message
                    )

                    # Verify cache_message was called
                    cache_mock.assert_called_once_with(expected_key, message)

    @pytest.mark.asyncio
    async def test_send_message_kafka_error(self):
        """Test that errors during message sending are handled correctly."""
        # Setup
        message = b'{"data": "test message"}'  # Message should be bytes
        topic = self.topic
        device_id = "device123"
        expected_key = "device123:1633046400000:0001"

        # Mock AIOKafkaProducer to avoid Kafka connection errors
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()
        mock_producer.send_and_wait = AsyncMock(side_effect=KafkaError("Kafka error"))

        # Mock cache message
        cache_mock = MagicMock(return_value=True)

        with patch(
            "producer.reactive_producer.AIOKafkaProducer", return_value=mock_producer
        ):
            with patch(
                "producer.reactive_producer.generate_message_key",
                return_value=expected_key,
            ):
                with patch("producer.reactive_producer.cache_message", cache_mock):
                    # Create a new producer instance for this test
                    producer = ReactiveKafkaProducer(
                        self.bootstrap_servers, self.redis_client
                    )

                    # Start the producer
                    await producer.start()

                    # Send the message - should return None on error
                    result = await producer.send_message(topic, message, device_id)

                    # Verify null result on error
                    assert result is None

                    # Verify producer.send_and_wait was called
                    mock_producer.send_and_wait.assert_called_once()

                    # Verify cache_message was NOT called after error
                    cache_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_batch(self):
        """Test that a batch of messages is sent correctly."""
        # Setup
        topic = self.topic
        device_id = "device123"
        count = 5  # The batch size

        # Mock AIOKafkaProducer to avoid Kafka connection errors
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()

        # Create a fake send_message that always succeeds
        successful_keys = [f"key_{i}" for i in range(count)]

        with patch(
            "producer.reactive_producer.AIOKafkaProducer", return_value=mock_producer
        ):
            with patch("asyncio.sleep", AsyncMock()):  # Mock sleep to speed up test
                # Create a new producer instance for this test
                producer = ReactiveKafkaProducer(
                    self.bootstrap_servers, self.redis_client
                )

                # Mock the send_message method
                producer.send_message = AsyncMock(side_effect=successful_keys)

                # Start the producer
                await producer.start()

                # Send the batch - parameters should match implementation order
                successful = await producer.send_batch(topic, device_id, count)

                # Verify correct number of successful messages
                assert successful == count

                # Verify send_message was called count times
                assert producer.send_message.call_count == count


# Helper class for async mocks
class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
