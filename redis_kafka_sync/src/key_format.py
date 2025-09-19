"""
Proxy module that redirects imports to the actual key format implementation.
"""

# Import necessary functions from the actual implementations
# We'll need to implement these functions in the actual codebase
# For now, we'll provide placeholder implementations

import datetime
import re
import time


def generate_message_key(device_id, timestamp=None, sequence=None):
    """
    Generate a message key in the format device_id:timestamp:sequence.

    Args:
        device_id: Unique device identifier
        timestamp: ISO 8601 timestamp or datetime object, defaults to current time
        sequence: 4-digit sequence number, defaults to 0001

    Returns:
        str: Formatted message key
    """
    if timestamp is None:
        # Use epoch time in milliseconds as timestamp
        timestamp = int(time.time() * 1000)
    elif isinstance(timestamp, datetime.datetime):
        # Convert datetime to epoch milliseconds
        timestamp = int(timestamp.timestamp() * 1000)

    if sequence is None:
        sequence = "0001"
    else:
        sequence = f"{int(sequence):04d}"

    return f"{device_id}:{timestamp}:{sequence}"


# Dictionary to store sequence counters for each device
sequence_counters = {}


def get_next_sequence(device_id):
    """
    Get the next sequence number for a given device.

    Args:
        device_id: Device identifier

    Returns:
        str: Next sequence number as 4-digit string
    """
    global sequence_counters  # noqa: F824

    if device_id not in sequence_counters:
        sequence_counters[device_id] = 0

    sequence_counters[device_id] += 1
    return f"{sequence_counters[device_id]:04d}"


def parse_business_key(key):
    """
    Parse a business key into its components.

    Args:
        key: Message key in the format device_id:timestamp:sequence

    Returns:
        dict: Dictionary with device_id, timestamp, and sequence or None if invalid
    """
    if not key or not isinstance(key, str):
        return None

    try:
        # Split by colon, expecting exactly 3 parts
        parts = key.split(":")
        if len(parts) != 3:
            return None

        device_id, timestamp_str, sequence_str = parts

        # Check for empty components
        if not device_id or not timestamp_str or not sequence_str:
            return None

        # Parse timestamp based on format
        try:
            # Assume it's in milliseconds epoch format
            timestamp = int(timestamp_str)
        except ValueError:
            # Invalid timestamp format
            return None

        # Return as dictionary to match test expectations
        return {
            "device_id": device_id,
            "timestamp": timestamp,
            "sequence": sequence_str,  # Keep as string to match test expectations
        }
    except Exception:
        # Any other parsing error
        return None


__all__ = ["generate_message_key", "get_next_sequence", "parse_business_key"]
