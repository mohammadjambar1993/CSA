#!/usr/bin/env python3
"""
Settings Module
-------------
Handles configuration parameters for Redis to PostgreSQL synchronization.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

logger = logging.getLogger("redis-pg-sync")

# Default configuration path
DEFAULT_CONFIG_PATH = "/app/env.conf"


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


def configure(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from the specified path or use environment variables.

    Args:
        config_path: Path to the configuration file. If None, uses DEFAULT_CONFIG_PATH.

    Returns:
        Dict containing all configuration values
    """
    # Load environment variables from config file if it exists
    if config_path:
        config_file = Path(config_path)
        if config_file.exists():
            load_dotenv(config_file)
            logger.info(f"Loaded configuration from {config_path}")
        else:
            logger.warning(
                f"Configuration file {config_path} not found, using environment variables"
            )
    else:
        load_dotenv(DEFAULT_CONFIG_PATH, verbose=True)

    # Create a dictionary of all configuration values
    config = {
        # Redis Configuration
        "REDIS_HOST": REDIS_HOST,
        "REDIS_PORT": REDIS_PORT,
        "REDIS_PASSWORD": REDIS_PASSWORD,
        "REDIS_DB": REDIS_DB,
        "REDIS_POOL_SIZE": REDIS_POOL_SIZE,
        "REDIS_TIME_TO_LIVE": REDIS_TIME_TO_LIVE,
        "REDIS_MAX_RETRIES": REDIS_MAX_RETRIES,
        "REDIS_BACKOFF_BASE": REDIS_BACKOFF_BASE,
        "REDIS_BACKOFF_MAX": REDIS_BACKOFF_MAX,
        "REDIS_HEALTH_CHECK_INTERVAL": REDIS_HEALTH_CHECK_INTERVAL,
        "REDIS_DB_SEARCH_RANGE": REDIS_DB_SEARCH_RANGE,
        "REDIS_DBS_TO_SEARCH": REDIS_DBS_TO_SEARCH,
        "REDIS_STREAMS_ENABLED": REDIS_STREAMS_ENABLED,
        "REDIS_STREAMS": REDIS_STREAMS,
        "REDIS_RATE_LIMIT": REDIS_RATE_LIMIT,
        # Stream Configuration
        "STREAM_CONSUMER_GROUP": STREAM_CONSUMER_GROUP,
        "STREAM_CONSUMER_NAME": STREAM_CONSUMER_NAME,
        "STREAM_BATCH_SIZE": STREAM_BATCH_SIZE,
        "STREAM_POLL_INTERVAL": STREAM_POLL_INTERVAL,
        "STREAM_AUTO_DISCOVERY": STREAM_AUTO_DISCOVERY,
        "STREAM_DISCOVERY_PATTERNS": STREAM_DISCOVERY_PATTERNS,
        "STREAM_DISCOVERY_INTERVAL": STREAM_DISCOVERY_INTERVAL,
        "STREAM_AUTO_CREATE_CONSUMER_GROUPS": STREAM_AUTO_CREATE_CONSUMER_GROUPS,
        "STREAM_CONFLICT_OVERWRITE": STREAM_CONFLICT_OVERWRITE,
        # Key-Value Configuration
        "KEY_VALUE_AUTO_DISCOVERY": KEY_VALUE_AUTO_DISCOVERY,
        "KEY_VALUE_PATTERNS": KEY_VALUE_PATTERNS,
        # PostgreSQL Configuration
        "PG_HOST": PG_HOST,
        "PG_PORT": PG_PORT,
        "PG_DATABASE": PG_DATABASE,
        "PG_USER": PG_USER,
        "PG_PASSWORD": PG_PASSWORD,
        "PG_SCHEMA": PG_SCHEMA,
        "PG_POOL_SIZE": PG_POOL_SIZE,
        "PG_MAX_QUERIES": PG_MAX_QUERIES,
        "PG_QUERY_TIMEOUT": PG_QUERY_TIMEOUT,
        "PG_MAX_RETRIES": PG_MAX_RETRIES,
        "PG_BACKOFF_BASE": PG_BACKOFF_BASE,
        "PG_BACKOFF_MAX": PG_BACKOFF_MAX,
        "PG_HEALTH_CHECK_INTERVAL": PG_HEALTH_CHECK_INTERVAL,
        # Table Names
        "STREAM_TABLE_NAME": STREAM_TABLE_NAME,
        "KEY_VALUE_TABLE_NAME": KEY_VALUE_TABLE_NAME,
        # Synchronization Configuration
        "SYNC_INTERVAL": SYNC_INTERVAL,
        "MONITOR_INTERVAL": MONITOR_INTERVAL,
        # Logging Configuration
        "LOG_LEVEL": LOG_LEVEL,
        "LOG_FORMAT": LOG_FORMAT,
    }

    return config


# Redis Configuration
REDIS_HOST = get_env("REDIS_HOST", "redis-cache")
REDIS_PORT = get_int_env("REDIS_PORT", 6379)
REDIS_PASSWORD = get_env("REDIS_PASSWORD", None)
REDIS_DB = get_int_env("REDIS_DB", 0)
REDIS_POOL_SIZE = get_int_env("REDIS_POOL_SIZE", 10)
REDIS_TIME_TO_LIVE = get_int_env("REDIS_TIME_TO_LIVE", 3600)

# Redis Connection Management
REDIS_MAX_RETRIES = get_int_env("REDIS_MAX_RETRIES", 5)
REDIS_BACKOFF_BASE = get_float_env("REDIS_BACKOFF_BASE", 1.0)
REDIS_BACKOFF_MAX = get_float_env("REDIS_BACKOFF_MAX", 30.0)
REDIS_HEALTH_CHECK_INTERVAL = get_int_env("REDIS_HEALTH_CHECK_INTERVAL", 60)

# Redis DB Search Configuration
REDIS_DB_SEARCH_RANGE = get_env("REDIS_DB_SEARCH_RANGE", "0-15")
SOURCE_REDIS_DB = get_int_env("SOURCE_REDIS_DB", 0)

# Parse database range
REDIS_DBS_TO_SEARCH: List[int] = []
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

# Redis Stream Configuration
REDIS_STREAMS_ENABLED = get_bool_env("REDIS_STREAMS_ENABLED", True)
REDIS_STREAMS = get_list_env("REDIS_STREAMS", "biosignal-stream")
STREAM_CONSUMER_GROUP = get_env("STREAM_CONSUMER_GROUP", "sync-group")
STREAM_CONSUMER_NAME = get_env("STREAM_CONSUMER_NAME", "sync-consumer")
STREAM_BATCH_SIZE = get_int_env("STREAM_BATCH_SIZE", 100)
STREAM_POLL_INTERVAL = get_float_env("STREAM_POLL_INTERVAL", 1.0)
STREAM_AUTO_DISCOVERY = get_bool_env("STREAM_AUTO_DISCOVERY", True)
STREAM_DISCOVERY_PATTERNS = get_list_env(
    "STREAM_DISCOVERY_PATTERNS", "*-stream,device:*:stream,biosignal-*"
)
STREAM_DISCOVERY_INTERVAL = get_int_env("STREAM_DISCOVERY_INTERVAL", 10)
STREAM_AUTO_CREATE_CONSUMER_GROUPS = get_bool_env(
    "STREAM_AUTO_CREATE_CONSUMER_GROUPS", True
)
# Controls how stream message conflicts are handled
STREAM_CONFLICT_OVERWRITE = get_bool_env("STREAM_CONFLICT_OVERWRITE", False)

# Redis Rate Limiting and Backoff
REDIS_RATE_LIMIT = int(os.getenv("REDIS_RATE_LIMIT", "100"))  # Operations per second

# Key-Value Configuration
KEY_VALUE_AUTO_DISCOVERY = get_bool_env("KEY_VALUE_AUTO_DISCOVERY", True)
KEY_VALUE_PATTERNS = get_list_env("KEY_VALUE_PATTERNS", "*")

# PostgreSQL Configuration
PG_HOST = get_env("POSTGRES_HOST", "remote-postgres-server")
PG_PORT = get_int_env("POSTGRES_PORT", 5432)
PG_DATABASE = get_env("POSTGRES_DB", "edn")
PG_USER = get_env("POSTGRES_USER", "postgres")
PG_PASSWORD = get_env("POSTGRES_PASSWORD", "postgres")
PG_SCHEMA = get_env("POSTGRES_SCHEMA", "public")
PG_POOL_SIZE = get_int_env("POSTGRES_POOL_SIZE", 10)
PG_MAX_QUERIES = get_int_env("POSTGRES_MAX_QUERIES", 50000)
PG_QUERY_TIMEOUT = get_float_env("POSTGRES_QUERY_TIMEOUT", 30.0)

# PostgreSQL Connection Management
PG_MAX_RETRIES = get_int_env("PG_MAX_RETRIES", 5)
PG_BACKOFF_BASE = get_float_env("PG_BACKOFF_BASE", 1.0)
PG_BACKOFF_MAX = get_float_env("PG_BACKOFF_MAX", 30.0)
PG_HEALTH_CHECK_INTERVAL = get_int_env("PG_HEALTH_CHECK_INTERVAL", 60)

# Table Names
STREAM_TABLE_NAME = get_env("STREAM_TABLE_NAME", "redis_streams")
KEY_VALUE_TABLE_NAME = get_env("KEY_VALUE_TABLE_NAME", "redis_key_values")

# Synchronization Configuration
SYNC_INTERVAL = get_float_env("SYNC_INTERVAL", 5.0)
MONITOR_INTERVAL = get_int_env("MONITOR_INTERVAL", 5)

# Logging Configuration
LOG_LEVEL = get_env("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = get_env(
    "LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
