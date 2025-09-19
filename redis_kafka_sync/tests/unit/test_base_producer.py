#!/usr/bin/env python3
"""
Unit tests for the base Kafka producer class.
"""

import os
import sys
import unittest
import uuid
from datetime import datetime
from unittest.mock import ANY, MagicMock, call, patch

# Import classes to test
from producer.producer import (
    generate_15mb_payload,
    generate_message_key,
    get_next_sequence,
    on_send_error,
    on_send_success,
    produce,
    produce_file,
    produce_message,
    produce_message_test,
)


class TestBaseProducer(unittest.TestCase):
    """Test the base Kafka producer functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock Redis for all tests
        self.redis_patcher = patch("producer.producer.redis_client")
        self.mock_redis = self.redis_patcher.start()

        # Reset sequence counter - need to directly access the dictionary from the producer module
        import producer.producer

        producer.producer.sequence_counters = {}

    def tearDown(self):
        """Tear down test fixtures."""
        self.redis_patcher.stop()

    def test_get_next_sequence(self):
        """Test getting the next sequence number for a device."""
        # Setup
        device_id = "device123"

        # Execute
        seq1 = get_next_sequence(device_id)
        seq2 = get_next_sequence(device_id)
        seq3 = get_next_sequence("other_device")

        # Verify
        self.assertEqual(seq1, 1)
        self.assertEqual(seq2, 2)
        self.assertEqual(seq3, 1)  # Different device starts at 1

    def test_generate_message_key_with_defaults(self):
        """Test generating a message key with default values."""
        # Setup
        device_id = "device123"

        # Execute
        with patch("producer.producer.datetime") as mock_datetime:
            mock_now = datetime(2024, 3, 22, 19, 30, 0)
            mock_datetime.utcnow.return_value = mock_now
            key = generate_message_key(device_id)

        # Verify
        self.assertTrue(key.startswith(f"{device_id}:"))
        self.assertTrue(":0001" in key)  # Default sequence

    def test_generate_message_key_with_custom_values(self):
        """Test generating a message key with custom values."""
        # Setup
        device_id = "device123"
        timestamp = datetime(2024, 3, 22, 19, 30, 0)
        sequence = 42

        # Execute
        key = generate_message_key(device_id, timestamp, sequence)

        # Verify
        expected_key = (
            f"{device_id}:{timestamp.isoformat().replace('+00:00', 'Z')}:{sequence:04d}"
        )
        self.assertEqual(key, expected_key)

    def test_on_send_success_with_device_id(self):
        """Test the success callback with a device ID."""
        # Setup
        metadata = MagicMock()
        metadata.topic = "test-topic"
        metadata.partition = 0
        metadata.offset = 123
        metadata.key = b"device123:2024-03-22T19:30:00.123Z:0042"

        topic = "test-topic"
        value = b"Test message content"
        device_id = "device123"

        # Execute
        with patch("builtins.print") as mock_print:
            on_send_success(metadata, topic, value, device_id)

        # Verify - in the actual implementation, setex is called multiple times
        # We check that all calls are made, not just one
        self.mock_redis.setex.assert_any_call(metadata.key.decode("utf-8"), ANY, value)
        self.mock_redis.setex.assert_any_call(
            f"kafka_index:{metadata.topic}:{metadata.partition}:{metadata.offset}",
            ANY,
            metadata.key.decode("utf-8"),
        )

    def test_on_send_success_without_device_id(self):
        """Test the success callback without a device ID."""
        # Setup
        metadata = MagicMock()
        metadata.topic = "test-topic"
        metadata.partition = 0
        metadata.offset = 123

        topic = "test-topic"
        value = b"Test message content"

        # Execute
        with patch("builtins.print") as mock_print:
            on_send_success(metadata, topic, value)

        # Verify
        key = f"{metadata.topic}:{metadata.partition}:{metadata.offset}"
        self.mock_redis.setex.assert_called_once_with(key, ANY, value)

    def test_on_send_error(self):
        """Test the error callback."""
        # Setup
        exception = Exception("Kafka error")

        # Execute
        with patch("builtins.print") as mock_print:
            on_send_error(exception)

        # Verify
        mock_print.assert_called_with("Failed to send message: Kafka error")

    @patch("producer.producer.KafkaProducer")
    def test_produce(self, mock_producer_class):
        """Test producing sample messages."""
        # Setup
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.bootstrap_connected.return_value = True

        topic = "test-topic"
        device_id = "device123"

        # Mock the future returned by send
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        # Execute
        with patch("producer.producer.time.sleep"):  # Mock sleep to avoid delay
            with patch(
                "producer.producer.generate_message_key",
                return_value="device123:2024-03-22T19:30:00.123Z:0001",
            ):
                produce(topic, device_id)

        # Verify
        mock_producer_class.assert_called_once()
        self.assertEqual(mock_producer.send.call_count, 10)  # 10 messages
        mock_producer.flush.assert_called()
        mock_producer.close.assert_called_once()

    @patch("producer.producer.KafkaProducer")
    def test_produce_message(self, mock_producer_class):
        """Test producing a single message."""
        # Setup
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.bootstrap_connected.return_value = True

        topic = "test-topic"
        message = "Test message"
        device_id = "device123"

        # Mock the future returned by send
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        # Execute
        with patch(
            "producer.producer.generate_message_key",
            return_value="device123:2024-03-22T19:30:00.123Z:0001",
        ):
            produce_message(topic, message, device_id)

        # Verify
        mock_producer_class.assert_called_once()
        mock_producer.send.assert_called_once()
        # Verify the message was encoded to bytes
        self.assertEqual(
            mock_producer.send.call_args[1]["value"], message.encode("utf-8")
        )
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()

    @patch("producer.producer.KafkaProducer")
    def test_produce_message_with_uuid(self, mock_producer_class):
        """Test producing a message with an auto-generated UUID."""
        # Setup
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.bootstrap_connected.return_value = True

        topic = "test-topic"
        message = "Test message"

        # Mock the future returned by send
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        # Execute
        with patch(
            "producer.producer.uuid.uuid4", return_value=MagicMock(hex="12345678abcdef")
        ):
            with patch(
                "producer.producer.generate_message_key",
                return_value="unknown.device.12345678:2024-03-22T19:30:00.123Z:0001",
            ):
                produce_message(topic, message)

        # Verify
        mock_producer_class.assert_called_once()
        mock_producer.send.assert_called_once()
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()

    def test_generate_15mb_payload(self):
        """Test generating a 15MB payload."""
        # Execute
        with patch(
            "producer.producer.os.urandom", return_value=b"x" * (15 * 1024 * 1024)
        ) as mock_urandom:
            payload = generate_15mb_payload()

        # Verify
        mock_urandom.assert_called_once_with(15 * 1024 * 1024)
        self.assertEqual(len(payload), 15 * 1024 * 1024)

    @patch("producer.producer.KafkaProducer")
    def test_produce_message_test(self, mock_producer_class):
        """Test producing a test message with a large payload."""
        # Setup
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        topic = "test-topic"

        # Mock generate_15mb_payload
        test_payload = b"x" * 1024  # Use a smaller payload for testing

        # Mock the future returned by send
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        # Execute
        with patch(
            "producer.producer.generate_15mb_payload", return_value=test_payload
        ):
            produce_message_test(topic)

        # Verify
        mock_producer_class.assert_called_once()
        mock_producer.send.assert_called_once()
        self.assertTrue(mock_producer.send.call_args[1]["value"].endswith(test_payload))
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()

    @patch("producer.producer.KafkaProducer")
    def test_produce_file(self, mock_producer_class):
        """Test producing a file."""
        # Setup
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.bootstrap_connected.return_value = True

        topic = "test-topic"
        file_path = "test_file.txt"
        file_content = b"Test file content"

        # Mock file operations
        mock_open = unittest.mock.mock_open(read_data=file_content)

        # Mock the future returned by send
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        # Execute with patch to avoid closing the producer twice
        with patch("producer.producer.os.path.isfile", return_value=True):
            with patch(
                "producer.producer.os.path.getsize", return_value=len(file_content)
            ):
                with patch("builtins.open", mock_open):
                    # Execute
                    produce_file(topic, file_path)

        # Verify
        mock_producer_class.assert_called_once()
        mock_producer.send.assert_called_once_with(topic, value=file_content)
        mock_producer.flush.assert_called()

    @patch("producer.producer.os.path.isfile")
    def test_produce_file_not_found(self, mock_isfile):
        """Test producing a file that doesn't exist."""
        # Setup
        topic = "test-topic"
        file_path = "nonexistent_file.txt"

        # Mock os.path.isfile to return False
        mock_isfile.return_value = False

        # Execute
        with patch("builtins.print") as mock_print:
            # The actual function might print an error instead of raising an exception
            produce_file(topic, file_path)

        # Verify
        mock_isfile.assert_called_once_with(file_path)
        mock_print.assert_any_call(
            f"Error in produce_file: File not found: {file_path}"
        )

    @patch("producer.producer.os.path.isfile")
    @patch("producer.producer.os.path.getsize")
    def test_produce_file_too_large(self, mock_getsize, mock_isfile):
        """Test producing a file that exceeds the maximum size."""
        # Setup
        topic = "test-topic"
        file_path = "large_file.txt"
        max_size = 20971520  # Default max request size

        # Mock os.path functions
        mock_isfile.return_value = True
        mock_getsize.return_value = max_size + 1

        # Execute
        with patch("builtins.print") as mock_print:
            # The actual function might print an error instead of raising an exception
            produce_file(topic, file_path)

        # Verify
        mock_isfile.assert_called_once_with(file_path)
        mock_getsize.assert_called_once_with(file_path)
        mock_print.assert_any_call(
            f"Error in produce_file: File size {max_size + 1} exceeds the maximum allowed size of {max_size} bytes"
        )


if __name__ == "__main__":
    unittest.main()
