#!/usr/bin/env python3
"""
Source Redis Manager
-------------------
Handles source Redis connections, discovery, reading, and acking.
"""

import asyncio
import fnmatch
import logging
import re  # Import re for parsing
import time  # Import time for health checks
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from config import (
    REDIS_DBS_TO_SEARCH,
    SOURCE_REDIS_BACKOFF_BASE,
    SOURCE_REDIS_BACKOFF_MAX,
    SOURCE_REDIS_DB,
    SOURCE_REDIS_HEALTH_CHECK_INTERVAL,
    SOURCE_REDIS_HOST,
    SOURCE_REDIS_MAX_RETRIES,
    SOURCE_REDIS_PASSWORD,
    SOURCE_REDIS_POOL_SIZE,
    SOURCE_REDIS_PORT,
    STREAM_AUTO_CREATE_CONSUMER_GROUPS,
    STREAM_AUTO_DISCOVERY,
    STREAM_BATCH_SIZE,
    STREAM_CONSUMER_GROUP,
    STREAM_CONSUMER_NAME,
    STREAM_DISCOVERY_PATTERNS,
    STREAM_POLL_INTERVAL,
)
from error_handling import handle_redis_error, is_busygroup_error, is_nogroup_error

logger = logging.getLogger("redis-to-redis-sync")


class SourceRedisManager:
    """Manages source Redis connections, discovery, reading, and acking."""

    def __init__(self):
        """Initialize the source Redis manager."""
        self.source_redis_pools: Dict[int, Optional[aioredis.Redis]] = {}
        self._lock = asyncio.Lock()
        self._connection_attempts: Dict[int, int] = {}
        self._last_health_check: Dict[int, float] = {}

    async def _check_connection_health(
        self, client: aioredis.Redis, db_num: int
    ) -> bool:
        """Perform a health check on a Redis connection."""
        try:
            if not await client.ping():
                logger.warning(f"Source Redis DB {db_num} ping failed.")
                return False
            # Optional: Add more checks like checking pool stats if needed
            return True
        except Exception as e:
            logger.error(f"Source Redis DB {db_num} health check failed: {e}")
            return False

    async def _get_source_redis_pool(self, db: int) -> Optional[aioredis.Redis]:
        """Get or create a Redis connection pool for a specific source DB with retry logic."""
        async with self._lock:  # Ensure thread safety for pool creation/check
            # Validate pool size configuration
            if SOURCE_REDIS_POOL_SIZE < 1:
                logger.error(
                    f"Invalid SOURCE_REDIS_POOL_SIZE: {SOURCE_REDIS_POOL_SIZE}. Must be >= 1."
                )
                return None

            # Check health if pool exists and interval has passed
            current_time = time.time()
            if (
                db in self.source_redis_pools
                and self.source_redis_pools[db] is not None
            ):
                if (
                    current_time - self._last_health_check.get(db, 0)
                    < SOURCE_REDIS_HEALTH_CHECK_INTERVAL
                ):
                    return self.source_redis_pools[db]  # Return existing healthy pool
                else:
                    # Interval passed, perform health check
                    is_healthy = await self._check_connection_health(
                        self.source_redis_pools[db], db
                    )
                    if is_healthy:
                        self._last_health_check[db] = current_time
                        return self.source_redis_pools[db]
                    else:
                        logger.warning(
                            f"Source Redis DB {db} connection unhealthy. Attempting to reconnect."
                        )
                        await handle_redis_error(
                            f"closing unhealthy source pool DB {db}",
                            self.source_redis_pools[db].close,
                        )
                        self.source_redis_pools[db] = (
                            None  # Mark as None to force recreation
                        )

            # Initialize attempts counter for this DB if needed
            if db not in self._connection_attempts:
                self._connection_attempts[db] = 0

            # Attempt to create a new connection pool with retries
            while self._connection_attempts[db] < SOURCE_REDIS_MAX_RETRIES:
                try:
                    # Calculate backoff delay
                    delay = min(
                        SOURCE_REDIS_BACKOFF_BASE
                        * (2 ** self._connection_attempts[db]),
                        SOURCE_REDIS_BACKOFF_MAX,
                    )

                    pool = aioredis.ConnectionPool(
                        host=SOURCE_REDIS_HOST,
                        port=SOURCE_REDIS_PORT,
                        db=db,
                        password=(
                            SOURCE_REDIS_PASSWORD if SOURCE_REDIS_PASSWORD else None
                        ),
                        socket_timeout=5,
                        decode_responses=True,
                        max_connections=SOURCE_REDIS_POOL_SIZE,
                    )
                    client = aioredis.Redis(connection_pool=pool)

                    # Test connection immediately after creation
                    if await self._check_connection_health(client, db):
                        self.source_redis_pools[db] = client
                        self._last_health_check[db] = time.time()
                        self._connection_attempts[db] = 0  # Reset attempts on success
                        logger.info(f"Created source Redis connection pool for DB {db}")
                        return client
                    else:
                        # Close the faulty client/pool immediately
                        await handle_redis_error(
                            f"closing failed source pool DB {db}", client.close
                        )
                        raise ConnectionError(
                            f"Newly created pool for DB {db} failed health check."
                        )

                except Exception as e:
                    logger.error(
                        f"Source connection attempt {self._connection_attempts[db] + 1} for DB {db} failed: {e}"
                    )
                    self._connection_attempts[db] += 1
                    if self._connection_attempts[db] < SOURCE_REDIS_MAX_RETRIES:
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"Max connection attempts reached for source Redis DB {db}"
                        )
                        return None  # Failed to connect after retries
            return None  # Should not be reached

    async def discover_streams(self) -> List[Tuple[str, int, str]]:
        """Discover streams on the source Redis using error helpers."""
        if not STREAM_AUTO_DISCOVERY:
            return []

        logger.info(f"Starting stream discovery on source Redis...")
        discovered_streams_info = []  # List of (prefixed_key, db_num, original_key)

        for db_num in REDIS_DBS_TO_SEARCH:
            source_client = await self._get_source_redis_pool(db_num)
            if not source_client:
                logger.warning(
                    f"Skipping stream discovery in source DB {db_num} - connection failed"
                )
                continue

            try:
                cursor = "0"
                while True:
                    # Use helper for scan
                    scan_result = await handle_redis_error(
                        f"scanning keys in source DB {db_num}",
                        source_client.scan,
                        cursor=cursor,
                        match="*",
                        count=200,
                    )
                    if scan_result is None:
                        break  # Scan error
                    cursor, keys = scan_result

                    type_tasks = {
                        key: handle_redis_error(
                            f"getting type for key {key}", source_client.type, key
                        )
                        for key in keys
                    }
                    results = await asyncio.gather(*type_tasks.values())
                    key_types = dict(zip(type_tasks.keys(), results))

                    for key, key_type in key_types.items():
                        if key_type == "stream":
                            if any(
                                fnmatch.fnmatch(key, pattern)
                                for pattern in STREAM_DISCOVERY_PATTERNS
                            ):
                                prefixed_key = (
                                    f"db{db_num}:{key}"
                                    if db_num != SOURCE_REDIS_DB
                                    else key
                                )
                                if not any(
                                    info[0] == prefixed_key
                                    for info in discovered_streams_info
                                ):
                                    logger.info(
                                        f"Discovered stream: {key} in source DB {db_num} (prefixed: {prefixed_key})"
                                    )
                                    discovered_streams_info.append(
                                        (prefixed_key, db_num, key)
                                    )

                    if cursor == "0":
                        break  # Scan finished for this DB
            except Exception as e:
                logger.error(
                    f"Error during stream discovery processing in source DB {db_num}: {e}"
                )

        return discovered_streams_info

    async def _create_consumer_group(self, db_num: int, stream_key: str):
        """Create consumer group on source using error helpers."""
        source_client = await self._get_source_redis_pool(db_num)
        if not source_client:
            return

        exists = await handle_redis_error(
            f"checking existence of source stream {stream_key} in DB {db_num}",
            source_client.exists,
            stream_key,
        )
        if exists is None:
            return

        if not exists and STREAM_AUTO_CREATE_CONSUMER_GROUPS:
            # Only try to create stream if auto-create groups is enabled
            # Assume if groups aren't auto-created, streams might not be either
            await handle_redis_error(
                f"creating source stream {stream_key} in DB {db_num} for group creation",
                source_client.xadd,
                stream_key,
                {"init": "init"},
                maxlen=1,
            )
            logger.info(
                f"Stream {stream_key} did not exist in DB {db_num}, created it with an init message."
            )
            exists = True  # Assume creation worked or next step will handle it

        if exists and STREAM_AUTO_CREATE_CONSUMER_GROUPS:
            try:
                await source_client.xgroup_create(
                    stream_key, STREAM_CONSUMER_GROUP, id="0", mkstream=True
                )
                logger.info(
                    f"Ensured consumer group {STREAM_CONSUMER_GROUP} exists for source stream {stream_key} in DB {db_num}"
                )
            except aioredis.ResponseError as e:
                if is_busygroup_error(e):
                    logger.debug(
                        f"Group {STREAM_CONSUMER_GROUP} already exists for source stream {stream_key} in DB {db_num}"
                    )
                else:
                    logger.error(
                        f"Redis error creating group for source {stream_key} in DB {db_num}: {e}"
                    )
            except Exception as e:
                logger.error(
                    f"Unexpected error creating group for source {stream_key} in DB {db_num}: {e}"
                )
        elif not STREAM_AUTO_CREATE_CONSUMER_GROUPS:
            logger.debug(
                f"Skipping consumer group creation for {stream_key} in DB {db_num} as STREAM_AUTO_CREATE_CONSUMER_GROUPS is false."
            )

    async def setup_initial_consumer_groups(self, streams_to_process: List[str]):
        """Setup consumer groups for initial streams using helpers."""
        if not streams_to_process or not STREAM_AUTO_CREATE_CONSUMER_GROUPS:
            logger.info(
                "Skipping initial consumer group setup (no streams listed or auto-create disabled)."
            )
            return

        logger.info(f"Setting up initial consumer groups for: {streams_to_process}")
        setup_tasks = []
        for stream_name in streams_to_process:
            db_num, original_key = self._parse_stream_name(stream_name)
            if original_key:
                setup_tasks.append(self._create_consumer_group(db_num, original_key))
        if setup_tasks:
            await asyncio.gather(*setup_tasks)

    def _parse_stream_name(self, stream_name: str) -> Tuple[int, Optional[str]]:
        """Parses a potentially prefixed stream name into (db_num, original_key) using regex."""
        pattern = r"^db(\d+):(.+)$"
        match = re.match(pattern, stream_name)
        if match:
            try:
                db_num = int(match.group(1))
                original_key = match.group(2)
                return db_num, original_key
            except ValueError:
                logger.error(f"Invalid DB number format in stream name: {stream_name}")
                return SOURCE_REDIS_DB, None  # Indicate parsing failure
        else:
            # Assume it's in the default DB if no prefix matches
            return SOURCE_REDIS_DB, stream_name

    async def read_stream_messages(
        self, stream_name: str
    ) -> Optional[Tuple[str, int, str, List[Tuple[str, Dict[str, Any]]]]]:
        """Read messages from a source stream.
        Returns tuple: (prefixed_stream_name, db_num, original_key, messages) or None on error/no messages.
        """
        db_num, original_key = self._parse_stream_name(stream_name)
        if not original_key:
            logger.error(f"Could not parse stream name for reading: {stream_name}")
            return None

        source_client = await self._get_source_redis_pool(db_num)
        if not source_client:
            # Error already logged by _get_source_redis_pool
            return None

        try:
            result = await source_client.xreadgroup(
                STREAM_CONSUMER_GROUP,
                STREAM_CONSUMER_NAME,
                {original_key: ">"},  # Use the original key for XREADGROUP
                count=STREAM_BATCH_SIZE,
                block=int(STREAM_POLL_INTERVAL * 1000),
            )

            if not result:
                return None  # No new messages

            for stream_data in result:
                # stream_data format: (stream_key_bytes, list_of_message_tuples)
                actual_stream_key = stream_data[
                    0
                ]  # This is the key from Redis, should match original_key
                messages = stream_data[1]
                if not messages:
                    continue
                logger.debug(
                    f"Read {len(messages)} messages from {actual_stream_key} in DB {db_num}"
                )
                # Return the original *prefixed* name, db, original key, and messages
                return stream_name, db_num, original_key, messages

        except aioredis.ResponseError as e:
            if is_nogroup_error(e):
                logger.warning(
                    f"Consumer group '{STREAM_CONSUMER_GROUP}' does not exist for source stream {stream_name}. Attempting to create."
                )
                await self._create_consumer_group(db_num, original_key)
            else:
                logger.error(
                    f"Redis response error processing source stream {stream_name}: {e}"
                )
        except Exception as e:
            logger.error(
                f"Unexpected error processing source stream {stream_name}: {e}",
                exc_info=True,
            )

        return None

    async def ack_messages(self, stream_name: str, message_ids: List[str]) -> bool:
        """Acknowledge messages in the source stream."""
        db_num, original_key = self._parse_stream_name(stream_name)
        if not original_key:
            return False

        source_client = await self._get_source_redis_pool(db_num)
        if not source_client:
            return False

        if not message_ids:
            logger.warning(f"Attempted to ACK empty message list for {stream_name}")
            return False

        ack_result = await handle_redis_error(
            f"acknowledging {len(message_ids)} messages in source stream {original_key} (DB {db_num})",
            source_client.xack,
            original_key,
            STREAM_CONSUMER_GROUP,
            *message_ids,
        )

        if ack_result is None:
            logger.critical(
                f"CRITICAL: Source Redis XACK FAILED for {len(message_ids)} IDs in {original_key} (DB {db_num}). Messages WILL be reprocessed."
            )
            # TODO: Increment a metric counter for 'xack_failures' here.
            # TODO: Trigger an alert based on 'xack_failures'.
            return False
        elif ack_result == 0:
            logger.warning(
                f"Source Redis XACK returned 0 for {len(message_ids)} IDs in {original_key} (DB {db_num}). Possibly already ACKed or wrong IDs?"
            )
            # TODO: Increment a metric counter for 'xack_zero_acks' here (optional).
            # Treat as success technically, as Redis didn't error, but log warning
            return True
        else:
            logger.debug(
                f"Acknowledged {ack_result}/{len(message_ids)} messages in source stream {original_key} (DB {db_num})"
            )
            # TODO: Increment a metric counter for 'xack_successes' here (optional).
            return True

    async def close(self):
        """Close all source Redis connections."""
        async with self._lock:
            logger.info(
                f"Closing {len(self.source_redis_pools)} source Redis connection pools."
            )
            close_tasks = []
            for db, pool in self.source_redis_pools.items():
                if pool:
                    close_tasks.append(
                        handle_redis_error(
                            f"closing source Redis pool for DB {db}", pool.close
                        )
                    )

            if close_tasks:
                await asyncio.gather(*close_tasks)

            self.source_redis_pools.clear()
            self._connection_attempts.clear()
            self._last_health_check.clear()
