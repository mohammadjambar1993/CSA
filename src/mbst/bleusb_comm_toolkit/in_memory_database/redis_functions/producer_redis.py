import logging
import os
import sys
from datetime import datetime

import pytz
import redis
from dotenv import load_dotenv

from .connect_to_redis import connect_to_redis
from .constants import devices

# Set timezone for timestamp formatting
local_tz = pytz.timezone("America/Toronto")

# Load Redis connection settings from environment variables
load_dotenv()
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def producer_redis_key(
    value, device="undefined", deviceId="0000", char="unknown", timezone=local_tz
):
    """
    Store a key-value pair in Redis using a generated key format.

    Args:
        value (str or bytes): The value to store in Redis.
        device (str): Name of the device type (e.g., pressure_bed).
        deviceId (str): Unique device address or ID.
        char (str): BLE characteristic name.
        timezone: Timezone object to generate timestamps.

    Returns:
        str: The generated Redis key used to store the value.
        None: If an error occurred during storage.
    """
    try:
        client = connect_to_redis(db=devices[device]["redis_db"])

        # Use epoch timestamp in milliseconds for uniqueness
        epoch_ts = str(int(datetime.now(timezone).timestamp() * 1000))
        key = f"{device}-{deviceId}-{char}-{epoch_ts}"

        client.set(key, value)

        logging.info(f"[producer_redis_key] Data stored in Redis with key: {key}")
        return key
    except Exception as e:
        logging.error(f"[producer_redis_key] Error in producer redis key: {e}")
        return None


def producer_redis_streams(
    value,
    device="undefined",
    device_id="0000",
    uid="unknown",  # user ID
    nn="unknown",  # nickname
    char="unknown",
    timezone=local_tz,
):
    """
    Append a structured record into a Redis Stream for a given device.

    Args:
        value (str): The actual data to store in Redis Stream.
        device (str): Name of the BLE/USB device (e.g., insole, pressure_bed).
        device_id (str): BLE address or COM port identifier of the device.
        uid (str): Unique identifier for the user.
        nn (str): Name or alias for the user.
        char (str): BLE characteristic from which the data originates.
        timezone: Timezone object to timestamp each record.

    Returns:
        str: The Redis stream key used.
        None: If an error occurred while storing data.
    """
    try:
        client = connect_to_redis(db=devices[device]["redis_db"])

        epoch_ts = str(int(datetime.now(timezone).timestamp() * 1000))
        key = f"{device}-{device_id}-{char}-{epoch_ts}"
        stream_key = f"stream:{device}:{device_id}"

        client.xadd(
            stream_key,
            {
                "characteristic": char,
                "timestamp": epoch_ts,
                "value": value,
                "key": key,
                "uid": uid,
                "nn": nn,
            },
        )

        # print(
        #     f"[producer_redis_streams] Data stored in Redis Stream: {stream_key} for user: {user_id} and nickname: {nick_name}"
        # )
        return stream_key

    except Exception as e:
        logging.error(f"[producer_redis_streams] Error in Producer Redis Streams: {e}")
        return None
