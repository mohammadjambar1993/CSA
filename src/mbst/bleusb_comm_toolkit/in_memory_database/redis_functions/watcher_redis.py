import os
import signal
import sys
import time
from typing import List, Union

import redis
from dotenv import load_dotenv

from mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.connect_to_redis import (
    connect_to_redis,
)
from mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.constants import (
    devices,
)

# Load environment variables from .env file
load_dotenv()

# Get Redis connection parameters from environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def create_stream_keys(device, device_id, char="undefined"):
    """Helper function to create stream keys based on device, device_id, and characteristic."""
    if isinstance(device, list) and isinstance(device_id, list):
        return [f"stream:{d}:{id}" for d, id in zip(device, device_id)]
    elif isinstance(device, str) and isinstance(device_id, str):
        return [f"stream:{device}:{device_id}"]
    else:
        raise ValueError(
            "device and device_id should either both be lists or both be single values."
        )


# Function to process and display entries
def process_entries(stream, messages, char=None):
    for entry_id, entry_data in messages:
        characteristic = entry_data[b"characteristic"].decode("utf-8")
        timestamp = entry_data[b"timestamp"].decode("utf-8")
        value = entry_data[b"value"].decode("utf-8")

        if char is None:
            # Print or further process the new data
            print(
                f"New Entry - Stream: {stream}, ID: {entry_id}, "
                f"Timestamp: {timestamp}, Characteristic: {characteristic}, Value: {value}"
            )
        else:
            if characteristic == char:
                print(
                    f"New Entry - Stream: {stream}, ID: {entry_id}, "
                    f"Timestamp: {timestamp}, Characteristic: {characteristic}, Value: {value}"
                )


def watch_streams(
    client,
    device: Union[str, List[str]],
    device_id: Union[str, List[str]],
    char: Union[str, List[str]],
    stream_key: Union[str, List[str]] = None,
):
    # Build the stream key(s)
    if stream_key is None:
        streams = create_stream_keys(device, device_id)
        print(streams)

    # Initialize the last_ids dictionary only once with the most recent entry in each stream
    last_ids = {
        key: "$" for key in streams
    }  # "$" starts listening from new entries onward

    # Graceful shutdown handler
    def signal_handler(sig, frame):
        print("\nGraceful shutdown.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C to exit

    while True:
        try:
            entries = client.xread(
                last_ids, block=1000
            )  # Block indefinitely until new data

            # Process each stream's messages
            for stream, messages in entries:
                process_entries(stream, messages, char)

                # Update last_id for each processed stream
                if messages:
                    last_ids[stream] = messages[-1][0]

        except redis.exceptions.ConnectionError:
            print("Error connecting to Redis. Retrying...")
            time.sleep(1)  # Reconnect on failure

        except KeyboardInterrupt:
            # This is the signal handling when Ctrl+C is pressed
            print("\nGracefully shutting down...")
            sys.exit(0)


if __name__ == "__main__":
    client = connect_to_redis(db=devices["undefined"]["redis_db"])

    watch_streams(
        client=client,
        device="undefined",  # Device(s) to watch
        device_id="0000",  # Device ID(s) to watch
        char="unknown",  # Characteristic(s)
    )
    watch_streams(
        client=client,
        device=["undefined", "pressure_mat", "skiin", "insole"],  # Device(s) to watch
        device_id=["0000", "0000", "0000", "0000"],  # Device ID(s) to watch
        char="undefined",  # Characteristic(s)
    )
