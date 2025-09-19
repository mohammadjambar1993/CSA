"""Unit tests for Redis caching functionality."""

import json
import os
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

# Import modules to test
from edn_service_bus.cache.redis_cache import cache_message, get_cached_message


class TestRedisCache:
    """Test suite for Redis caching functionality."""

    @patch("edn_service_bus.cache.redis_cache.encrypt_value")
    def test_producer_cache_message(self, mock_encrypt_value):
        """Test that the producer caches a message correctly."""
        # Setup
        redis_client = MagicMock()
        key = "device123:1633046400000:0001"
        value = json.dumps({"data": "test message"}).encode()
        ttl = 3600  # 1 hour

        # Mock encryption to return the same value
        mock_encrypt_value.return_value = value

        # Execute
        cache_message(redis_client, key, value, ttl)

        # Verify
        mock_encrypt_value.assert_called_once_with(value)
        redis_client.setex.assert_called_once_with(key, ttl, value)

    @patch("edn_service_bus.cache.redis_cache.encrypt_value")
    def test_producer_cache_message_error(self, mock_encrypt_value):
        """Test that errors in caching are handled correctly."""
        # Setup
        redis_client = MagicMock()
        redis_client.setex.side_effect = Exception("Redis error")
        key = "device123:1633046400000:0001"
        value = json.dumps({"data": "test message"}).encode()
        ttl = 3600  # 1 hour

        # Mock encryption to return the same value
        mock_encrypt_value.return_value = value

        # Execute and verify
        try:
            cache_message(redis_client, key, value, ttl)
            # Should not reach here
            assert False, "Expected an exception to be raised"
        except Exception as e:
            assert str(e) == "Redis error"

    @patch("edn_service_bus.cache.redis_cache.decrypt_value")
    def test_consumer_get_cached_message_hit(self, mock_decrypt_value):
        """Test retrieving a cached message when it exists."""
        # Setup
        redis_client = MagicMock()
        key = "device123:1633046400000:0001"
        encrypted_value = b"encrypted_data"
        expected_value = json.dumps({"data": "test message"}).encode()
        redis_client.get.return_value = encrypted_value

        # Mock decryption to return our expected value
        mock_decrypt_value.return_value = expected_value

        # Execute
        result = get_cached_message(redis_client, key)

        # Verify
        redis_client.get.assert_called_once_with(key)
        mock_decrypt_value.assert_called_once_with(encrypted_value)
        assert result == expected_value

    def test_consumer_get_cached_message_miss(self):
        """Test retrieving a cached message when it does not exist."""
        # Setup
        redis_client = MagicMock()
        key = "device123:1633046400000:0001"
        redis_client.get.return_value = None

        # Execute
        result = get_cached_message(redis_client, key)

        # Verify
        redis_client.get.assert_called_once_with(key)
        assert result is None

    def test_consumer_get_cached_message_error(self):
        """Test error handling when retrieving a cached message."""
        # Setup
        redis_client = MagicMock()
        key = "device123:1633046400000:0001"
        redis_client.get.side_effect = Exception("Redis error")

        # Execute and verify
        try:
            result = get_cached_message(redis_client, key)
            # Should not reach here
            assert False, "Expected an exception to be raised"
        except Exception as e:
            assert str(e) == "Redis error"

    @patch("edn_service_bus.cache.redis_cache.encrypt_value")
    def test_consumer_cache_message(self, mock_encrypt_value):
        """Test that the consumer caches a message correctly."""
        # Setup
        redis_client = MagicMock()
        key = "device123:1633046400000:0001"
        value = json.dumps({"data": "test message"}).encode()
        ttl = 3600  # 1 hour

        # Mock encryption to return the same value
        mock_encrypt_value.return_value = value

        # Execute
        cache_message(redis_client, key, value, ttl)

        # Verify
        mock_encrypt_value.assert_called_once_with(value)
        redis_client.setex.assert_called_once_with(key, ttl, value)


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
