"""Unit tests for key format generation and parsing functions."""

import os
import re
import sys
import time
from unittest.mock import patch

import pytest

# Import module to test
from edn_service_bus.utils.key_format import (
    generate_message_key,
    get_next_sequence,
    parse_business_key,
)


class TestKeyFormat:
    """Test suite for key format generation and parsing functions."""

    def test_generate_message_key_format(self):
        """Test the format of the generated message key."""
        device_id = "device123"
        timestamp = int(time.time() * 1000)
        sequence = "0001"

        key = generate_message_key(device_id, timestamp, sequence)

        # Key should be in format: device_id:timestamp:sequence
        assert key == f"{device_id}:{timestamp}:{sequence}"

    def test_generate_message_key_default_timestamp(self):
        """Test that timestamp is generated when not provided."""
        device_id = "device123"
        sequence = "0001"

        # Get current time for comparison
        current_time = int(time.time() * 1000)

        with patch("time.time", return_value=current_time / 1000):
            key = generate_message_key(device_id, sequence=sequence)

        # Extract timestamp from key
        key_parts = key.split(":")
        assert len(key_parts) == 3
        timestamp = int(key_parts[1])

        # The timestamp should be close to current time
        assert timestamp == current_time

    def test_generate_message_key_default_sequence(self):
        """Test that sequence is generated when not provided."""
        device_id = "device123"
        timestamp = int(time.time() * 1000)

        key = generate_message_key(device_id, timestamp)

        # Extract sequence from key
        key_parts = key.split(":")
        assert len(key_parts) == 3
        sequence = key_parts[2]

        # Sequence should be a 4-digit number
        assert re.match(r"^\d{4}$", sequence)

    def test_get_next_sequence(self):
        """Test the sequence generation functionality."""
        device_id = "device123"

        # First sequence should be 0001
        seq1 = get_next_sequence(device_id)
        assert seq1 == "0001"

        # Second sequence for the same device should be 0002
        seq2 = get_next_sequence(device_id)
        assert seq2 == "0002"

        # Different device should start from 0001
        different_device = "device456"
        seq3 = get_next_sequence(different_device)
        assert seq3 == "0001"

    def test_parse_business_key_valid(self):
        """Test parsing a valid business key."""
        key = "device123:1633046400000:0001"
        result = parse_business_key(key)

        assert result is not None
        assert result["device_id"] == "device123"
        assert result["timestamp"] == 1633046400000
        assert result["sequence"] == "0001"

    def test_parse_business_key_invalid_format(self):
        """Test parsing an invalid business key."""
        # Test with various invalid formats
        invalid_keys = [
            "",  # Empty string
            "invalid_key",  # No delimiters
            "device123:1633046400000",  # Missing sequence
            "device123::0001",  # Empty timestamp
            ":1633046400000:0001",  # Empty device_id
            "device123:abc:0001",  # Non-numeric timestamp
        ]

        for key in invalid_keys:
            result = parse_business_key(key)
            assert result is None, f"Expected None for invalid key: {key}"


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
