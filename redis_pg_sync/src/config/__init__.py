"""
Configuration Package
-------------------
Contains settings and configuration management for Redis to PostgreSQL synchronization.
"""

from .settings import (
    PG_DATABASE,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_POOL_SIZE,
    REDIS_PORT,
    REDIS_STREAMS,
    REDIS_STREAMS_ENABLED,
    configure,
)

__all__ = [
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_PASSWORD",
    "REDIS_DB",
    "REDIS_POOL_SIZE",
    "PG_HOST",
    "PG_PORT",
    "PG_DATABASE",
    "PG_USER",
    "PG_PASSWORD",
    "REDIS_STREAMS",
    "REDIS_STREAMS_ENABLED",
    "configure",
]
