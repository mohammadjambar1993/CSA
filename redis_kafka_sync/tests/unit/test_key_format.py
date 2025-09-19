#!/usr/bin/env python3
"""
Unit tests for the key format functions in the reactive implementation.
"""

import os
import re
import sys
import unittest
from datetime import datetime

from consumer.reactive_consumer import parse_business_key

# Import functions to test
from producer.reactive_producer import generate_message_key, get_next_sequence


class TestKeyFormat(unittest.TestCase):
    """Test the key format generation and parsing functions."""

    def test_generate_message_key_format(self):
        """Test that the generated key has the correct format."""
        device_id = "vest.device.123"
        timestamp = datetime.strptime(
            "2024-03-22T19:30:00.123Z", "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        sequence = 42

        key = generate_message_key(device_id, timestamp, sequence)

        # Key should be in format device_id:timestamp:sequence
        self.assertEqual(key, "vest.device.123:2024-03-22T19:30:00.123Z:0042")

    def test_generate_message_key_default_timestamp(self):
        """Test that a timestamp is generated when not provided."""
        device_id = "vest.device.123"
        sequence = 42

        key = generate_message_key(device_id, None, sequence)

        # Key should have the device_id and sequence, with a timestamp in the middle
        device_id_and_rest = key.split(":", 1)
        self.assertEqual(device_id_and_rest[0], device_id)

        # Get the sequence from the end of the key
        rest_and_sequence = device_id_and_rest[1].rsplit(":", 1)
        self.assertEqual(rest_and_sequence[1], f"{sequence:04d}")

        # Validate timestamp format (ISO 8601)
        timestamp_str = rest_and_sequence[0]
        timestamp_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z"
        self.assertTrue(re.match(timestamp_pattern, timestamp_str))

    def test_generate_message_key_default_sequence(self):
        """Test that a sequence is generated when not provided."""
        device_id = "vest.device.123"
        timestamp = datetime.strptime(
            "2024-03-22T19:30:00.123Z", "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        key = generate_message_key(device_id, timestamp, None)

        # Key should have the device_id and timestamp, with a sequence at the end
        device_id_and_rest = key.split(":", 1)
        self.assertEqual(device_id_and_rest[0], device_id)

        # Get the sequence from the end of the key
        rest_and_sequence = device_id_and_rest[1].rsplit(":", 1)

        # The middle part should be the timestamp
        self.assertEqual(rest_and_sequence[0], "2024-03-22T19:30:00.123Z")

        # Sequence should be a 4-digit number
        sequence_pattern = r"\d{4}"
        self.assertTrue(re.match(sequence_pattern, rest_and_sequence[1]))

    def test_get_next_sequence(self):
        """Test that get_next_sequence increments for a device."""
        device_id = "vest.device.test"

        # First call should return 1
        first = get_next_sequence(device_id)
        self.assertEqual(first, 1)

        # Second call should return 2
        second = get_next_sequence(device_id)
        self.assertEqual(second, 2)

        # Different device should start at 1
        other_device = "vest.device.other"
        other_first = get_next_sequence(other_device)
        self.assertEqual(other_first, 1)

    def test_parse_business_key_valid(self):
        """Test parsing a valid business key."""
        key = "vest.device.123:2024-03-22T19:30:00.123Z:0042"
        result = parse_business_key(key)

        self.assertIsNotNone(result)
        device_id, timestamp, sequence = result

        self.assertEqual(device_id, "vest.device.123")
        self.assertEqual(timestamp.year, 2024)
        self.assertEqual(timestamp.month, 3)
        self.assertEqual(timestamp.day, 22)
        self.assertEqual(timestamp.hour, 19)
        self.assertEqual(timestamp.minute, 30)
        self.assertEqual(timestamp.second, 0)
        self.assertEqual(timestamp.microsecond, 123000)  # Milliseconds to microseconds
        self.assertEqual(sequence, 42)

    def test_parse_business_key_invalid_format(self):
        """Test parsing an invalid business key."""
        # Missing components
        key1 = "vest.device.123:2024-03-22T19:30:00.123Z"
        self.assertIsNone(parse_business_key(key1))

        # Too many components
        key2 = "vest.device.123:2024-03-22T19:30:00.123Z:0042:extra"
        self.assertIsNone(parse_business_key(key2))

        # Invalid timestamp
        key3 = "vest.device.123:not-a-timestamp:0042"
        self.assertIsNone(parse_business_key(key3))

        # Invalid sequence
        key4 = "vest.device.123:2024-03-22T19:30:00.123Z:not-a-number"
        self.assertIsNone(parse_business_key(key4))


if __name__ == "__main__":
    unittest.main()
