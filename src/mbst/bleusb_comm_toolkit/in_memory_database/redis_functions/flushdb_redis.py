import os
import sys

import redis
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Redis connection parameters from environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def flush_all():
    # Connect to Redis
    try:
        client = redis.StrictRedis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=False
        )
        client.ping()
        print(f"flushdb: Connected to Redis at {REDIS_HOST}:{REDIS_PORT}.")
        all_keys = client.keys("*")
        print("All keys:", all_keys)
        client.flushdb()
        all_keys = client.keys("*")
        print("Database flushed All keys:", all_keys)
    except redis.exceptions.ConnectionError as e:
        print(f"flushdb: Could not connect to Redis: {e}")
        sys.exit(1)


def flush_db(db=0):
    try:
        client = redis.StrictRedis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=False, db=db
        )
        client.ping()
        print(f"flushdb: Connected to Redis at {REDIS_HOST}:{REDIS_PORT}.")
        all_keys = client.keys("*")
        print("All keys:", all_keys)
        client.flushdb()
        all_keys = client.keys("*")
        print("Database flushed All keys:", all_keys)
    except redis.exceptions.ConnectionError as e:
        print(f"flushdb: Could not connect to Redis: {e}")
        sys.exit(1)


if __name__ == "__main__":
    confirm = input("Are you sure you want to flush Redis DB 2? (y/n): ")
    if confirm.lower() == "y":
        flush_db(db=2)

# flush_db(db=2)
