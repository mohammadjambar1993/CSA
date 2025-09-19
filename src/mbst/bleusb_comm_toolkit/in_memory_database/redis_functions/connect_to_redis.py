import os
import sys
from datetime import datetime

import pytz
import redis
from dotenv import load_dotenv

# Set Timezone
local_tz = pytz.timezone("America/Toronto")

# Load environment variables from .env file
load_dotenv()

# Get Redis connection parameters from environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def connect_to_redis(db=0):
    try:
        client = redis.StrictRedis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=False, db=db
        )
        client.ping()
        # time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        # print(
        #     f"[connect_to_redis] Connected to Redis at {REDIS_HOST}:{REDIS_PORT} for database {db} at {time}."
        # )
        return client
    except redis.exceptions.ConnectionError as e:
        print(f"[connect_to_redis] Error connecting to Redis: {e}")
        sys.exit(1)
