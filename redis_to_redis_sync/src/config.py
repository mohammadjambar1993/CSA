#!/usr/bin/env python3
"""
Redis to Redis Sync Configuration
-------------------------------
Configuration parameters for Redis to Redis synchronization.
"""

import os
from typing import Any, List


# Utility function to get environment variable with default
def get_env(key: str, default: Any) -> Any:
    return os.environ.get(key, default)


# Utility function to get boolean environment variable
def get_bool_env(key: str, default: bool) -> bool:
    val = get_env(key, str(default)).lower()
    return val in ("true", "1", "t", "y", "yes")


# Utility function to get integer environment variable
def get_int_env(key: str, default: int) -> int:
    try:
        return int(get_env(key, str(default)))
    except ValueError:
        return default


# Utility function to get float environment variable
def get_float_env(key: str, default: float) -> float:
    try:
        return float(get_env(key, str(default)))
    except ValueError:
        return default


# Utility function to get list environment variable
def get_list_env(key: str, default: str) -> List[str]:
    val = get_env(key, default)
    return [item.strip() for item in val.split(",") if item.strip()]


# Source Redis Configuration
SOURCE_REDIS_HOST = get_env("SOURCE_REDIS_HOST", "source-redis")
SOURCE_REDIS_PORT = get_int_env("SOURCE_REDIS_PORT", 6379)
SOURCE_REDIS_PASSWORD = get_env("SOURCE_REDIS_PASSWORD", None)
SOURCE_REDIS_DB = get_int_env("SOURCE_REDIS_DB", 0)

# Source Connection Management
SOURCE_REDIS_MAX_RETRIES = get_int_env("SOURCE_REDIS_MAX_RETRIES", 5)
SOURCE_REDIS_BACKOFF_BASE = get_float_env("SOURCE_REDIS_BACKOFF_BASE", 1.0)
SOURCE_REDIS_BACKOFF_MAX = get_float_env("SOURCE_REDIS_BACKOFF_MAX", 30.0)
SOURCE_REDIS_HEALTH_CHECK_INTERVAL = get_int_env(
    "SOURCE_REDIS_HEALTH_CHECK_INTERVAL", 60
)
SOURCE_REDIS_POOL_SIZE = get_int_env("SOURCE_REDIS_POOL_SIZE", 10)

# Target Redis Configuration
TARGET_REDIS_HOST = get_env("TARGET_REDIS_HOST", "target-redis")
TARGET_REDIS_PORT = get_int_env("TARGET_REDIS_PORT", 6379)
TARGET_REDIS_PASSWORD = get_env("TARGET_REDIS_PASSWORD", None)
TARGET_REDIS_DB = get_int_env("TARGET_REDIS_DB", 0)
TARGET_STREAM_PREFIX = get_env("TARGET_STREAM_PREFIX", "")

# Target Connection Management
TARGET_REDIS_MAX_RETRIES = get_int_env("TARGET_REDIS_MAX_RETRIES", 5)
TARGET_REDIS_BACKOFF_BASE = get_float_env("TARGET_REDIS_BACKOFF_BASE", 1.0)
TARGET_REDIS_BACKOFF_MAX = get_float_env("TARGET_REDIS_BACKOFF_MAX", 30.0)
TARGET_REDIS_HEALTH_CHECK_INTERVAL = get_int_env(
    "TARGET_REDIS_HEALTH_CHECK_INTERVAL", 60
)
TARGET_REDIS_POOL_SIZE = get_int_env("TARGET_REDIS_POOL_SIZE", 10)

# Redis DB Search Configuration (Source)
REDIS_DB_SEARCH_RANGE = get_env("REDIS_DB_SEARCH_RANGE", "0-15")

# Parse database range
REDIS_DBS_TO_SEARCH = []
if REDIS_DB_SEARCH_RANGE:
    try:
        parts = REDIS_DB_SEARCH_RANGE.split(",")
        for part in parts:
            if "-" in part:
                start, end = map(int, part.split("-"))
                REDIS_DBS_TO_SEARCH.extend(range(start, end + 1))
            else:
                REDIS_DBS_TO_SEARCH.append(int(part))
        REDIS_DBS_TO_SEARCH = sorted(list(set(REDIS_DBS_TO_SEARCH)))
    except ValueError:
        print(
            f"Invalid REDIS_DB_SEARCH_RANGE format: {REDIS_DB_SEARCH_RANGE}. Using default DB {SOURCE_REDIS_DB}"
        )
        REDIS_DBS_TO_SEARCH = [SOURCE_REDIS_DB]
else:
    REDIS_DBS_TO_SEARCH = [SOURCE_REDIS_DB]

# Stream Processing Configuration (Source)
REDIS_STREAMS_ENABLED = get_bool_env("REDIS_STREAMS_ENABLED", True)
REDIS_STREAMS = get_list_env(
    "REDIS_STREAMS", ""
)  # Default empty, rely on discovery unless specified
STREAM_CONSUMER_GROUP = get_env("STREAM_CONSUMER_GROUP", "redis-sync-group")
STREAM_CONSUMER_NAME = get_env("STREAM_CONSUMER_NAME", "redis-sync-consumer")
STREAM_BATCH_SIZE = get_int_env("STREAM_BATCH_SIZE", 100)
STREAM_POLL_INTERVAL = get_float_env("STREAM_POLL_INTERVAL", 1.0)  # Seconds

# Stream Auto-Discovery (Source)
STREAM_AUTO_DISCOVERY = get_bool_env("STREAM_AUTO_DISCOVERY", True)
STREAM_DISCOVERY_PATTERNS = get_list_env(
    "STREAM_DISCOVERY_PATTERNS", "*-stream,device:*:stream,biosignal-*"
)
STREAM_DISCOVERY_INTERVAL = get_int_env("STREAM_DISCOVERY_INTERVAL", 10)
STREAM_AUTO_CREATE_CONSUMER_GROUPS = get_bool_env(
    "STREAM_AUTO_CREATE_CONSUMER_GROUPS", True
)

# Logging Configuration
LOG_LEVEL = get_env("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = get_env(
    "LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
