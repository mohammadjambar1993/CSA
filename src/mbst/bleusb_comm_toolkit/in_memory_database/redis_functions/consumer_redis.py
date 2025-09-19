"""
Redis Consumer Functions for MBST Toolkit.

This module provides functions to read data from Redis—either from key-value
storage or Redis Streams. It supports basic filtering by timestamp and characteristic.

Author: Sarah Gulzar
"""

import logging
import os
import sys
from datetime import datetime

import pytz
import redis
from dotenv import load_dotenv

from .connect_to_redis import connect_to_redis
from .constants import devices

# Set timezone for timestamp generation
local_tz = pytz.timezone("America/Toronto")

# Load environment variables from .env file
load_dotenv()

# Redis config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def consumer_redis_key(key, device="undefined"):
    """
    Retrieves a value from Redis using a given key.

    Args:
        key (str): The Redis key to fetch.
        device (str): The device name to identify the correct Redis DB.

    Returns:
        bytes or None: The retrieved value, or None if not found or on error.
    """
    try:
        client = connect_to_redis(db=devices[device]["redis_db"])

        value = client.get(key)
        if value is not None:
            logging.info(
                f"[consumer_redis_key] Data retrieved from Redis for key {key}"
            )
            return value
        else:
            logging.warning(f"[consumer_redis_key] No data found for key: {key}")
            return None
    except Exception as e:
        logging.error(f"[consumer_redis_key] Error in consumer redis key: {e}")
        return None


def consumer_redis_streams(
    filter=False,
    stream_key=None,
    start_ts=None,
    end_ts=None,
    device="undefined",
    device_id="0000",
    char="unknown",
):
    """
    Reads data from a Redis Stream and optionally filters it by characteristic.

    Args:
        filter (bool): Whether to filter entries by `char`.
        stream_key (str): Redis stream key (if None, it's constructed from device/device_id).
        start_ts (float): Start timestamp in ms (defaults to 100 minutes ago).
        end_ts (float): End timestamp in ms (defaults to now).
        device (str): Device type to derive DB and stream key.
        device_id (str): Device ID (board address).
        char (str): Characteristic to filter by (used only if `filter=True`).

    Returns:
        list[dict] or None: A list of decoded stream entries (filtered or not), or None on error.
    """
    try:
        client = connect_to_redis(db=devices[device]["redis_db"])

        # Default: last ~100 minutes
        if start_ts is None:
            end_ts = datetime.now(local_tz).timestamp() * 1000
            start_ts = end_ts - 8640000  # ~2.4 hours

        if stream_key is None:
            stream_key = f"stream:{device}:{device_id}"

        entries = client.xrange(
            stream_key, min=str(int(start_ts)), max=str(int(end_ts))
        )

        all_entries = []
        filtered_entries = []

        for entry_id, entry_data in entries:
            characteristic = entry_data[b"characteristic"].decode("utf-8")
            timestamp = entry_data[b"timestamp"].decode("utf-8")
            value = entry_data[b"value"].decode("utf-8")
            key = entry_data[b"key"].decode("utf-8")

            entry = {
                "id": entry_id.decode("utf-8"),
                "timestamp": timestamp,
                "characteristic": characteristic,
                "value": value,
                "key": key,
            }

            if filter and characteristic == char:
                filtered_entries.append(entry)
            elif not filter:
                all_entries.append(entry)

        return filtered_entries if filter else all_entries

    except Exception as e:
        logging.warning(
            f"[consumer_redis_streams] Error in Consumer Redis Streams: {e}"
        )
        return None
