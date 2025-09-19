#!/usr/bin/env python3
"""
Exceptions Module
---------------
Custom exceptions for Redis to PostgreSQL synchronization.
"""

import logging
from typing import Any, Callable, Coroutine, Optional, TypeVar, Union

import redis.asyncio as aioredis

logger = logging.getLogger("redis-pg-sync")

T = TypeVar("T")


class RedisError(Exception):
    """Base exception for Redis errors."""

    pass


class PostgresError(Exception):
    """Base exception for PostgreSQL errors."""

    pass


class ConnectionError(Exception):
    """Exception for connection errors."""

    pass


class RedisConnectionError(ConnectionError, RedisError):
    """Exception for Redis connection errors."""

    pass


class PostgresConnectionError(ConnectionError, PostgresError):
    """Exception for PostgreSQL connection errors."""

    pass


class SyncError(Exception):
    """Exception for synchronization errors."""

    pass


class ConfigurationError(Exception):
    """Exception for configuration errors."""

    pass


async def handle_redis_error(
    operation_desc: str, coro: Callable[..., Coroutine[Any, Any, T]], *args, **kwargs
) -> Optional[T]:
    """
    Helper to execute a Redis coroutine and handle common errors.

    Args:
        operation_desc: Description of the operation being performed
        coro: The coroutine to execute
        *args: Positional arguments for the coroutine
        **kwargs: Keyword arguments for the coroutine

    Returns:
        The result of the coroutine if successful, None if an error occurred
    """
    try:
        return await coro(*args, **kwargs)
    except aioredis.RedisError as e:
        logger.error(f"Redis error during '{operation_desc}': {e}")
        raise RedisError(f"Error during '{operation_desc}': {e}") from e
    except Exception as e:
        logger.error(f"Unexpected error during '{operation_desc}': {e}")
        raise SyncError(f"Unexpected error during '{operation_desc}': {e}") from e


def is_busygroup_error(error: Union[Exception, str]) -> bool:
    """
    Check if an error is a BUSYGROUP error from Redis.

    Args:
        error: The error to check

    Returns:
        True if the error is a BUSYGROUP error, False otherwise
    """
    error_str = str(error)
    return isinstance(error, aioredis.ResponseError) and "BUSYGROUP" in error_str


def is_nogroup_error(error: Union[Exception, str]) -> bool:
    """
    Check if an error is a NOGROUP error from Redis.

    Args:
        error: The error to check

    Returns:
        True if the error is a NOGROUP error, False otherwise
    """
    error_str = str(error)
    return isinstance(error, aioredis.ResponseError) and "NOGROUP" in error_str
