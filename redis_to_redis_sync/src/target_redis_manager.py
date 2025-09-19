#!/usr/bin/env python3
"""
Target Redis Manager
-------------------
Handles target Redis connection and writing.
"""

import asyncio
import logging
import time  # Import time for health checks
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis

from .config import (  # TARGET_STREAM_MAXLEN - Removed as it wasn't implemented/used
    TARGET_REDIS_BACKOFF_BASE,
    TARGET_REDIS_BACKOFF_MAX,
    TARGET_REDIS_DB,
    TARGET_REDIS_HEALTH_CHECK_INTERVAL,
    TARGET_REDIS_HOST,
    TARGET_REDIS_MAX_RETRIES,
    TARGET_REDIS_PASSWORD,
    TARGET_REDIS_POOL_SIZE,
    TARGET_REDIS_PORT,
)

# Corrected import path assuming error_handling.py exists
from .error_handling import handle_redis_error

logger = logging.getLogger("redis-to-redis-sync")


class TargetRedisManager:
    """Manages target Redis connection and writing."""

    def __init__(self):
        """Initialize the target Redis manager."""
        self.target_redis_pool: Optional[aioredis.ConnectionPool] = None
        self._lock = asyncio.Lock()
        self._connection_attempts = 0
        self._last_health_check = 0.0

    async def _check_connection_health(self, client: aioredis.Redis) -> bool:
        """Perform a health check on the target Redis connection."""
        try:
            if not await client.ping():
                logger.warning("Target Redis ping failed.")
                return False
            # Optional: Add more checks like checking pool stats if needed
            return True
        except Exception as e:
            logger.error(f"Target Redis health check failed: {e}")
            return False

    async def connect(self) -> bool:
        """Establish connection to Target Redis with retry logic and health checks."""
        async with self._lock:
            # Validate pool size
            if TARGET_REDIS_POOL_SIZE < 1:
                logger.error(
                    f"Invalid TARGET_REDIS_POOL_SIZE: {TARGET_REDIS_POOL_SIZE}. Must be >= 1."
                )
                return False

            # Check health if pool exists and interval has passed
            current_time = time.time()
            if self.target_redis_pool is not None:
                if (
                    current_time - self._last_health_check
                    < TARGET_REDIS_HEALTH_CHECK_INTERVAL
                ):
                    # Check health even if interval hasn't passed, but don't force reconnect yet
                    # This ensures we don't use a dead connection within the interval
                    temp_client = aioredis.Redis(connection_pool=self.target_redis_pool)
                    if await self._check_connection_health(temp_client):
                        await handle_redis_error(
                            "closing temporary target health check client",
                            temp_client.close,
                        )
                        return True
                    else:
                        logger.warning(
                            "Target Redis connection check failed within interval. Forcing reconnect attempt."
                        )
                        await self.close()  # Close existing pool
                else:
                    # Interval passed, perform health check
                    temp_client = aioredis.Redis(connection_pool=self.target_redis_pool)
                    is_healthy = await self._check_connection_health(temp_client)
                    await handle_redis_error(
                        "closing temporary target health check client after interval",
                        temp_client.close,
                    )
                    if is_healthy:
                        self._last_health_check = current_time
                        return True
                    else:
                        logger.warning(
                            "Target Redis connection unhealthy after interval. Attempting to reconnect."
                        )
                        await self.close()

            # Attempt to create a new connection pool with retries
            self._connection_attempts = 0
            while self._connection_attempts < TARGET_REDIS_MAX_RETRIES:
                try:
                    # Calculate backoff delay
                    delay = min(
                        TARGET_REDIS_BACKOFF_BASE * (2**self._connection_attempts),
                        TARGET_REDIS_BACKOFF_MAX,
                    )

                    pool = aioredis.ConnectionPool(
                        host=TARGET_REDIS_HOST,
                        port=TARGET_REDIS_PORT,
                        db=TARGET_REDIS_DB,
                        password=(
                            TARGET_REDIS_PASSWORD if TARGET_REDIS_PASSWORD else None
                        ),
                        socket_timeout=5,
                        decode_responses=True,
                        max_connections=TARGET_REDIS_POOL_SIZE,
                    )
                    # Test connection immediately after creation
                    test_client = aioredis.Redis(connection_pool=pool)
                    if await self._check_connection_health(test_client):
                        self.target_redis_pool = pool  # Assign the pool only on success
                        self._last_health_check = time.time()
                        self._connection_attempts = 0  # Reset attempts
                        logger.info(
                            f"Created target Redis connection pool for DB {TARGET_REDIS_DB}"
                        )
                        await handle_redis_error(
                            "closing target test client after successful connect",
                            test_client.close,
                        )
                        return True
                    else:
                        await handle_redis_error(
                            f"closing failed target pool DB {TARGET_REDIS_DB}",
                            test_client.close,
                        )
                        await handle_redis_error(
                            f"disconnecting failed target pool DB {TARGET_REDIS_DB}",
                            pool.disconnect,
                        )
                        raise ConnectionError(
                            "Newly created target pool failed health check."
                        )

                except Exception as e:
                    logger.error(
                        f"Target connection attempt {self._connection_attempts + 1} failed: {e}"
                    )
                    self._connection_attempts += 1
                    if self._connection_attempts < TARGET_REDIS_MAX_RETRIES:
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"Max connection attempts reached for target Redis DB {TARGET_REDIS_DB}"
                        )
                        self.target_redis_pool = None  # Ensure pool is None
                        return False
            return False  # Should not be reached

    async def write_messages(
        self, target_key: str, messages: List[Tuple[str, Dict[str, Any]]]
    ) -> List[str]:
        """Write messages to target Redis and return successfully written message IDs."""
        # Ensure connection is active before proceeding
        if not await self.connect() or self.target_redis_pool is None:
            logger.error(
                "Target Redis connection not available, cannot write messages."
            )
            return []

        # Acquire connection from the pool
        target_client = aioredis.Redis(connection_pool=self.target_redis_pool)
        written_ids = []
        pipeline = target_client.pipeline(
            transaction=False
        )  # Use pipeline without transaction
        added_to_pipeline = False

        for entry_id, data in messages:
            try:
                # Pipeline XADD on target
                # TARGET_STREAM_MAXLEN is removed, add back if needed
                pipeline.xadd(target_key, data, id=entry_id)  # Use original ID
                written_ids.append(entry_id)  # Track IDs intended for writing
                added_to_pipeline = True
            except Exception as e:
                logger.error(
                    f"Error adding entry {entry_id} for stream {target_key} to target pipeline: {e}"
                )

        if not added_to_pipeline:
            logger.warning(
                f"No valid messages could be added to pipeline for target stream {target_key}"
            )
            await handle_redis_error(
                f"closing target client after empty pipeline", target_client.close
            )
            return []

        # Execute pipeline on target using helper
        pipeline_result = await handle_redis_error(
            f"executing write pipeline for target stream {target_key}", pipeline.execute
        )

        # Close the acquired client connection
        await handle_redis_error(
            f"closing target client after pipeline execution", target_client.close
        )

        if pipeline_result is not None:
            # Check results if needed, though xadd in pipeline usually returns True/False or raises error
            logger.debug(
                f"Executed pipeline for {len(written_ids)} messages to target stream {target_key}. Result: {pipeline_result}"
            )
            # Assuming success if no error was caught by handle_redis_error
            return written_ids
        else:
            logger.error(f"Pipeline execution failed for target stream {target_key}")
            return []

    async def close(self):
        """Close target Redis connections."""
        async with self._lock:
            if self.target_redis_pool:
                await handle_redis_error(
                    "closing target Redis pool", self.target_redis_pool.disconnect
                )
            self.target_redis_pool = None
            self._connection_attempts = 0
            self._last_health_check = 0.0
            logger.info("Target Redis connection resources released.")
