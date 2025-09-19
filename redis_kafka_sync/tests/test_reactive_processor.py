#!/usr/bin/env python3
"""
Integration tests for the ReactiveProcessor class.
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import modules to test
from processor.reactive_processor import ReactiveProcessor


class TestReactiveProcessorIntegration:
    """Integration tests for ReactiveProcessor."""

    @pytest.mark.asyncio
    @patch("processor.reactive_processor.AIOKafkaConsumer")
    @patch("processor.reactive_processor.AIOKafkaProducer")
    @patch("processor.reactive_processor.get_redis_client")
    async def test_processor_end_to_end(
        self, mock_get_redis, mock_producer_class, mock_consumer_class
    ):
        """Test the processing workflow from message consumption to production."""
        # Setup mocks
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        # Setup consumer mock
        mock_consumer = AsyncMock()
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()

        # Setup consumer to yield one message then exit
        message = MagicMock()
        message.key = "device123:2023-10-01T12:00:00Z:0001"
        message.value = b"test-message"
        message.topic = "input-topic"
        message.partition = 0
        message.offset = 100

        # Make the consumer's async iterator yield one message then raise StopAsyncIteration
        mock_consumer.__aiter__.return_value = self._mock_async_iterator([message])
        mock_consumer_class.return_value = mock_consumer

        # Setup producer mock
        mock_producer = AsyncMock()
        mock_producer.start = AsyncMock()
        mock_producer.stop = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        # Create a mock for the send_and_wait result
        mock_metadata = MagicMock()
        mock_metadata.topic = "output-topic"
        mock_metadata.partition = 0
        mock_metadata.offset = 200
        mock_producer.send_and_wait.return_value = mock_metadata

        mock_producer_class.return_value = mock_producer

        # Mock process function - simulate applying an algorithm
        def process_func(input_bytes):
            # In a real test, this would call mylibrary.algorithm.run(input_bytes)
            return b"PROCESSED:" + input_bytes

        # Create processor with mocked dependencies
        processor = ReactiveProcessor(
            input_topic="input-topic",
            output_topic="output-topic",
            consumer_group="test-group",
            process_func=process_func,
            bootstrap_servers="kafka:9092",
            redis_client=mock_redis,
        )

        # Important: Set the producer directly to make sure it's accessible
        processor.producer = mock_producer

        # Patch the _process_messages method to manually process our test message
        async def mock_process_messages():
            await processor._process_message(message)
            return True

        processor._process_messages = mock_process_messages

        # Execute - start processor and let it process one message
        with patch(
            "processor.reactive_processor.get_cached_message", return_value=None
        ):
            with patch("processor.reactive_processor.cache_message", return_value=True):
                with patch(
                    "processor.reactive_processor.parse_business_key",
                    return_value=("device123", None, None),
                ):
                    with patch(
                        "processor.reactive_processor.generate_message_key",
                        return_value="device123:2023-10-01T12:30:00Z:0001",
                    ):

                        # Start processor
                        await processor.start()

                        # Wait for processing to complete
                        # In a real test, we'd need some way to detect when processing is done
                        await asyncio.sleep(0.1)

                        # Stop processor
                        await processor.stop()

        # Verify calls
        mock_consumer.start.assert_called_once()
        mock_producer.start.assert_called_once()

        # Verify the producer was called with the processed message
        mock_producer.send_and_wait.assert_called_once_with(
            topic="output-topic",
            key="device123:2023-10-01T12:30:00Z:0001",
            value=b"PROCESSED:test-message",
        )

    @staticmethod
    async def _mock_async_iterator(items):
        """Helper method to create an async iterator from a list of items."""
        for item in items:
            yield item


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
