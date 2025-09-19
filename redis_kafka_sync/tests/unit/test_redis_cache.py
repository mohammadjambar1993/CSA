#!/usr/bin/env python3
"""
Unit tests for the Redis caching functionality in the reactive implementation.
"""

import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

import redis
from consumer.reactive_consumer import cache_message as consumer_cache_message
from consumer.reactive_consumer import get_cached_message
from consumer.reactive_consumer import get_redis_client as consumer_get_redis_client

# Import functions to test
from producer.reactive_producer import cache_message as producer_cache_message
from producer.reactive_producer import get_redis_client as producer_get_redis_client


class TestRedisCache(unittest.TestCase):
    """Test the Redis caching functionality."""

    @patch("producer.reactive_producer.get_redis_client")
    def test_producer_cache_message(self, mock_get_redis_client):
        """Test that the producer caches messages correctly."""
        # Setup
        mock_redis = MagicMock()
        mock_redis.setex.return_value = True
        mock_get_redis_client.return_value = mock_redis

        # Test successful cache
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        message = b"Test message content"
        ttl = 3600

        result = producer_cache_message(key, message, ttl)

        # Verify redis_client.setex was called with the right arguments
        mock_redis.setex.assert_called_once_with(key, ttl, message)
        self.assertTrue(result)

    @patch("producer.reactive_producer.get_redis_client")
    def test_producer_cache_message_error(self, mock_get_redis_client):
        """Test error handling in producer cache_message."""
        # Setup
        mock_redis = MagicMock()
        mock_redis.setex.side_effect = redis.RedisError("Redis error")
        mock_get_redis_client.return_value = mock_redis

        # Test error handling
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        message = b"Test message content"

        result = producer_cache_message(key, message)

        # Verify function returns False on error
        self.assertFalse(result)

    @patch("producer.reactive_producer.get_redis_client")
    def test_producer_cache_message_unavailable(self, mock_get_redis_client):
        """Test behavior when Redis is unavailable."""
        # Setup for unavailable Redis
        mock_get_redis_client.return_value = None

        # Test when Redis is unavailable
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        message = b"Test message content"

        result = producer_cache_message(key, message)

        # Verify function returns False when Redis is unavailable
        self.assertFalse(result)

    @patch("consumer.reactive_consumer.get_redis_client")
    def test_consumer_get_cached_message_hit(self, mock_get_redis_client):
        """Test retrieving a message from cache (cache hit)."""
        # Setup for cache hit
        mock_redis = MagicMock()
        mock_redis.exists.return_value = True
        mock_redis.get.return_value = b"Cached message content"
        mock_get_redis_client.return_value = mock_redis

        # Test cache hit
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        result = get_cached_message(key)

        # Verify redis calls and result
        mock_redis.exists.assert_called_once_with(key)
        mock_redis.get.assert_called_once_with(key)
        self.assertEqual(result, b"Cached message content")

    @patch("consumer.reactive_consumer.get_redis_client")
    def test_consumer_get_cached_message_miss(self, mock_get_redis_client):
        """Test retrieving a message from cache (cache miss)."""
        # Setup for cache miss
        mock_redis = MagicMock()
        mock_redis.exists.return_value = False
        mock_get_redis_client.return_value = mock_redis

        # Test cache miss
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        result = get_cached_message(key)

        # Verify redis calls and result
        mock_redis.exists.assert_called_once_with(key)
        mock_redis.get.assert_not_called()
        self.assertIsNone(result)

    @patch("consumer.reactive_consumer.get_redis_client")
    def test_consumer_get_cached_message_error(self, mock_get_redis_client):
        """Test error handling in get_cached_message."""
        # Setup for error
        mock_redis = MagicMock()
        mock_redis.exists.side_effect = redis.RedisError("Redis error")
        mock_get_redis_client.return_value = mock_redis

        # Test error handling
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        result = get_cached_message(key)

        # Verify function returns None on error
        self.assertIsNone(result)

    @patch("consumer.reactive_consumer.get_redis_client")
    def test_consumer_get_cached_message_unavailable(self, mock_get_redis_client):
        """Test behavior when Redis is unavailable."""
        # Setup for unavailable Redis
        mock_get_redis_client.return_value = None

        # Test when Redis is unavailable
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        result = get_cached_message(key)

        # Verify function returns None when Redis is unavailable
        self.assertIsNone(result)

    @patch("consumer.reactive_consumer.get_redis_client")
    def test_consumer_cache_message(self, mock_get_redis_client):
        """Test that the consumer caches messages correctly."""
        # Setup
        mock_redis = MagicMock()
        mock_redis.setex.return_value = True
        mock_get_redis_client.return_value = mock_redis

        # Test successful cache
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        message = b"Test message content"
        ttl = 3600

        result = consumer_cache_message(key, message, ttl)

        # Verify redis_client.setex was called with the right arguments
        mock_redis.setex.assert_called_once_with(key, ttl, message)
        self.assertTrue(result)

    @patch("consumer.reactive_consumer.get_redis_client")
    def test_consumer_cache_message_unavailable(self, mock_get_redis_client):
        """Test behavior when Redis is unavailable for caching."""
        # Setup for unavailable Redis
        mock_get_redis_client.return_value = None

        # Test when Redis is unavailable
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        message = b"Test message content"

        result = consumer_cache_message(key, message)

        # Verify function returns False when Redis is unavailable
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
