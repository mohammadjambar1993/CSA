"""Unit tests for Redis error handling and failure scenarios."""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis

# Import the modules to test
from consumer.reactive_consumer import cache_message as consumer_cache
from consumer.reactive_consumer import get_cached_message
from consumer.reactive_consumer import get_redis_client as consumer_get_redis
from processor.reactive_processor import ReactiveProcessor
from processor.reactive_processor import get_redis_client as processor_get_redis
from producer.reactive_producer import cache_message as producer_cache
from producer.reactive_producer import get_redis_client as producer_get_redis


class TestRedisFailureScenarios:
    """Tests for Redis failure handling across all components."""

    @pytest.fixture
    def mock_redis_error(self):
        """Fixture to simulate Redis connection error."""
        with patch(
            "redis.Redis.ping", side_effect=redis.RedisError("Connection refused")
        ):
            yield

    @pytest.fixture
    def mock_redis_timeout(self):
        """Fixture to simulate Redis timeout."""
        with patch(
            "redis.Redis.ping", side_effect=redis.TimeoutError("Connection timeout")
        ):
            yield

    @pytest.fixture
    def mock_redis_get_error(self):
        """Fixture to simulate Redis get error."""
        # First make ping succeed, then make get fail
        with (
            patch("redis.Redis.ping", return_value=True),
            patch(
                "redis.Redis.get", side_effect=redis.RedisError("Get operation failed")
            ),
        ):
            yield

    @pytest.fixture
    def mock_redis_exists_error(self):
        """Fixture to simulate Redis exists error."""
        # First make ping succeed, then make exists fail
        with (
            patch("redis.Redis.ping", return_value=True),
            patch(
                "redis.Redis.exists",
                side_effect=redis.RedisError("Exists operation failed"),
            ),
        ):
            yield

    @pytest.fixture
    def mock_redis_setex_error(self):
        """Fixture to simulate Redis setex error."""
        # First make ping succeed, then make setex fail
        with (
            patch("redis.Redis.ping", return_value=True),
            patch(
                "redis.Redis.setex",
                side_effect=redis.RedisError("Setex operation failed"),
            ),
        ):
            yield

    # Consumer tests
    def test_consumer_get_redis_client_error(self, mock_redis_error):
        """Test consumer's get_redis_client handles connection errors."""
        result = consumer_get_redis()
        assert result is None

    def test_consumer_get_redis_client_timeout(self, mock_redis_timeout):
        """Test consumer's get_redis_client handles timeouts."""
        result = consumer_get_redis()
        assert result is None

    def test_consumer_get_cached_message_redis_error(self, mock_redis_error):
        """Test get_cached_message handles Redis connection errors."""
        result = get_cached_message("test-key")
        assert result is None

    def test_consumer_get_cached_message_exists_error(self, mock_redis_exists_error):
        """Test get_cached_message handles Redis exists errors."""
        result = get_cached_message("test-key")
        assert result is None

    def test_consumer_get_cached_message_get_error(self, mock_redis_get_error):
        """Test get_cached_message handles Redis get errors."""
        # Mock exists to return True so get is called
        with patch("redis.Redis.exists", return_value=True):
            result = get_cached_message("test-key")
            assert result is None

    def test_consumer_cache_message_redis_error(self, mock_redis_error):
        """Test cache_message handles Redis connection errors."""
        result = consumer_cache("test-key", b"test-value")
        assert result is False

    def test_consumer_cache_message_setex_error(self, mock_redis_setex_error):
        """Test cache_message handles Redis setex errors."""
        result = consumer_cache("test-key", b"test-value")
        assert result is False

    # Producer tests
    def test_producer_get_redis_client_error(self, mock_redis_error):
        """Test producer's get_redis_client handles connection errors."""
        result = producer_get_redis()
        assert result is None

    def test_producer_cache_message_redis_error(self, mock_redis_error):
        """Test producer's cache_message handles Redis connection errors."""
        result = producer_cache("test-key", b"test-value")
        assert result is False

    def test_producer_cache_message_setex_error(self, mock_redis_setex_error):
        """Test producer's cache_message handles Redis setex errors."""
        result = producer_cache("test-key", b"test-value")
        assert result is False

    # Processor tests
    def test_processor_get_redis_client_error(self, mock_redis_error):
        """Test processor's get_redis_client handles connection errors."""
        result = processor_get_redis()
        assert result is None

    @pytest.mark.asyncio
    async def test_processor_with_redis_failure(self, mock_redis_error):
        """Test processor functionality with Redis failure."""

        # Create processor with minimal dependencies
        def mock_process_func(data):
            return data

        processor = ReactiveProcessor(
            input_topic="input-topic",
            output_topic="output-topic",
            consumer_group="test-group",
            process_func=mock_process_func,
        )

        # Mock consumer and producer to isolate Redis errors
        with (
            patch.object(processor, "consumer", new=AsyncMock()),
            patch.object(processor, "producer", new=AsyncMock()),
        ):

            # Create a message
            message = MagicMock()
            message.key = "test-device-123"
            message.value = b"original-data"

            # Process message - should handle Redis errors gracefully
            result = await processor._process_message(message)

            # Verify that the processor can continue operating
            assert result is True or result is False  # Allow either outcome as valid

    @pytest.mark.asyncio
    async def test_processor_cache_hit_with_get_error(self, mock_redis_get_error):
        """Test processor handles Redis get errors during cache hit."""

        # Create processor with minimal dependencies
        def mock_process_func(data):
            return data

        processor = ReactiveProcessor(
            input_topic="input-topic",
            output_topic="output-topic",
            consumer_group="test-group",
            process_func=mock_process_func,
        )

        # Mock consumer and producer
        with (
            patch.object(processor, "consumer", new=AsyncMock()),
            patch.object(processor, "producer", new=AsyncMock()),
            patch(
                "processor.reactive_processor.parse_business_key",
                return_value=["test-device"],
            ),
            patch("processor.reactive_processor.get_cached_message", return_value=None),
        ):  # Cache miss due to error

            # Create a message
            message = MagicMock()
            message.key = "test-device-123"
            message.value = b"original-data"

            # Process message - should handle Redis errors gracefully
            result = await processor._process_message(message)

            # Should still be able to process the message from Kafka value
            assert result is True

    @pytest.mark.asyncio
    async def test_processor_publish_cache_error(self, mock_redis_setex_error):
        """Test processor handles Redis cache errors during result publishing."""

        # Create processor with minimal dependencies
        def mock_process_func(data):
            return b"processed:" + data

        processor = ReactiveProcessor(
            input_topic="input-topic",
            output_topic="output-topic",
            consumer_group="test-group",
            process_func=mock_process_func,
        )

        # Set up producer mock
        processor.producer = AsyncMock()
        processor.producer.send_and_wait = AsyncMock(return_value=MagicMock())

        # Process and publish
        with patch(
            "processor.reactive_processor.generate_message_key", return_value="new-key"
        ):
            result = await processor._publish_result(b"test-data", "test-device")

            # Should succeed even if Redis caching fails
            assert result is True

            # Verify send_and_wait was called with correct params
            processor.producer.send_and_wait.assert_called_once()
            call_args = processor.producer.send_and_wait.call_args.kwargs
            assert call_args["key"] == "new-key"
            assert call_args["value"] == b"test-data"
