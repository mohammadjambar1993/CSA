"""
Redis Service for Redis MQTT Synchronization.

This module provides Redis operations for the Redis MQTT Synchronization system.
It handles connections to Redis servers, with proper null-safety and async/await patterns.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as redis
from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import RedisError

from ..config import settings
from ..utils.rate_limiter import RateLimiter
from .interfaces import RedisServiceInterface

logger = logging.getLogger("redis-mqtt-sync")


class RedisService(RedisServiceInterface):
    """
    Redis service implementation.

    Handles connections to Redis servers across multiple databases with proper null-safety
    and async operations. Supports discovering and managing Redis streams.
    """

    def __init__(self) -> None:
        """Initialize Redis service with empty client and pool collections."""
        self._pools: Dict[int, Optional[ConnectionPool]] = {}
        self._clients: Dict[int, Optional[Redis]] = {}
        self._rate_limiter = RateLimiter(settings.REDIS_RATE_LIMIT, time_unit=1.0)

    async def connect(self, db: int = 0) -> bool:
        """
        Connect to Redis server.

        Handles connection establishment with proper error handling and
        reconnection logic. Safely manages potentially null connections.

        Args:
            db: The Redis database number to connect to

        Returns:
            True if connection successful, False otherwise
        """
        # Check if already connected to this db
        if db in self._clients and self._clients[db] is not None:
            client = self._clients[db]
            if client is not None:  # This helps the type checker
                try:
                    # Test connection with a simple ping
                    await client.ping()
                    return True
                except RedisError:
                    # Connection is broken, clean it up
                    logger.warning(
                        f"Redis connection to DB {db} is broken, reconnecting..."
                    )
                    await self._close_connection(db)
                    return False

            else:
                return False

        # Create a new connection
        retries = 0
        while retries <= settings.REDIS_MAX_RETRIES:
            try:
                # Create connection pool if it doesn't exist
                if db not in self._pools:
                    self._pools[db] = redis.ConnectionPool(
                        host=settings.REDIS_HOST,
                        port=settings.REDIS_PORT,
                        db=db,
                        password=settings.REDIS_PASSWORD,
                        username=settings.REDIS_USERNAME,
                        ssl=settings.REDIS_SSL,
                        max_connections=settings.REDIS_POOL_SIZE,
                        decode_responses=True,
                    )

                # Create client using the pool
                self._clients[db] = redis.Redis(
                    connection_pool=self._pools[db],
                )

                # Test connection
                client = self._clients[db]
                if client is not None:
                    await client.ping()
                    logger.info(
                        f"Connected to Redis server at {settings.REDIS_HOST}:{settings.REDIS_PORT}, DB {db}"
                    )
                    return True
                return False
            except RedisError as e:
                retries += 1
                logger.warning(
                    f"Redis connection attempt {retries}/{settings.REDIS_MAX_RETRIES} failed: {e}"
                )
                if retries <= settings.REDIS_MAX_RETRIES:
                    await asyncio.sleep(settings.REDIS_RETRY_INTERVAL)
                    continue
                logger.error(
                    f"Failed to connect to Redis after {settings.REDIS_MAX_RETRIES} retries"
                )
                return False
        return False

    async def _close_connection(self, db: int) -> None:
        """
        Close a specific Redis connection.

        Safely closes Redis client and pool resources for a given database,
        with proper null handling and async/await patterns.

        Args:
            db: The Redis database number to disconnect from
        """
        if db in self._clients:
            client = self._clients[db]
            if client is not None:
                try:
                    await client.close()
                except Exception as e:
                    logger.warning(f"Error closing Redis connection for DB {db}: {e}")
            self._clients[db] = None

        if db in self._pools:
            pool = self._pools[db]
            if pool is not None:
                try:
                    await pool.disconnect()
                except Exception as e:
                    logger.warning(f"Error closing Redis pool for DB {db}: {e}")

            self._pools[db] = None

    async def disconnect(self) -> None:
        """Disconnect from all Redis servers."""
        for db in list(self._clients.keys()):
            await self._close_connection(db)

        logger.info("Disconnected from all Redis servers")

    async def discover_streams(self) -> List[Tuple[str, int, int]]:
        """
        Discover Redis streams across all configured databases.

        Safely queries all configured Redis databases, handling potentially null
        connections and using proper async/await patterns.

        Returns:
            List of tuples with (stream_key, db_number, length)
        """
        await self._rate_limiter.acquire()
        discovered_streams = []

        for db in settings.REDIS_DBS_TO_SEARCH:
            if not await self.connect(db):
                logger.warning(
                    f"Skipping stream discovery for unreachable Redis DB {db}"
                )
                continue

            try:
                # Get all keys (this can be expensive on large databases)
                client = self._clients[db]
                if client is not None:
                    all_keys = await client.keys("*")

                    for key in all_keys:
                        try:
                            # Check if the key is a stream
                            client = self._clients[
                                db
                            ]  # Get client reference again in case it changed
                            if client is not None:
                                key_type = await client.type(key)
                                if key_type.lower() == "stream":
                                    # Get stream length
                                    client = self._clients[
                                        db
                                    ]  # Get client reference again
                                    if client is not None:
                                        length = await client.xlen(key)
                                        discovered_streams.append((key, db, length))
                        except RedisError as e:
                            logger.warning(
                                f"Error checking key type for {key} in DB {db}: {e}"
                            )
            except RedisError as e:
                logger.error(f"Error discovering streams in Redis DB {db}: {e}")

        logger.info(f"Discovered {len(discovered_streams)} Redis streams")
        return discovered_streams

    async def setup_initial_consumer_groups(self, streams: List[str]) -> bool:
        """
        Create consumer groups for streams if they don't exist.

        Handles potentially null Redis connections and uses proper async/await patterns
        when interacting with Redis to set up consumer groups.

        Args:
            streams: List of stream keys to create consumer groups for

        Returns:
            True if all operations completed (some may have been skipped if groups exist)
        """
        if not streams:
            logger.warning("No streams provided for consumer group setup")
            return True

        success = True
        group_name = settings.STREAM_GROUP

        for stream_key in streams:
            for db in settings.REDIS_DBS_TO_SEARCH:
                if not await self.connect(db):
                    continue

                try:
                    # Check if the stream exists in this database
                    client = self._clients[db]
                    if client is not None:
                        key_type = await client.type(stream_key)
                        if key_type.lower() != "stream":
                            continue

                        # Check if the consumer group already exists
                        groups = await client.xinfo_groups(stream_key)
                        group_exists = any(g.get("name") == group_name for g in groups)

                        if not group_exists:
                            # Create consumer group starting from the earliest entry
                            await client.xgroup_create(
                                stream_key, group_name, id="0", mkstream=True
                            )
                            logger.info(
                                f"Created consumer group '{group_name}' for stream '{stream_key}' in DB {db}"
                            )
                except RedisError as e:
                    logger.error(
                        f"Error setting up consumer group for stream '{stream_key}' in DB {db}: {e}"
                    )
                    success = False

        return success

    async def read_stream_messages(
        self, stream_key: str
    ) -> Optional[Tuple[str, List[Tuple[str, Dict[str, Any]]]]]:
        """
        Read messages from a Redis stream using a consumer group.

        Safely handles potentially null Redis connections and uses proper async/await
        patterns when interacting with Redis streams.

        Args:
            stream_key: The key of the stream to read from

        Returns:
            A tuple containing (stream_key, [(entry_id, data), ...]) if successful,
            None otherwise
        """
        await self._rate_limiter.acquire()

        # Try all configured databases
        for db in settings.REDIS_DBS_TO_SEARCH:
            if not await self.connect(db):
                continue

            try:
                # Check if the stream exists in this database
                client = self._clients[db]
                if client is not None:
                    key_type = await client.type(stream_key)
                    if key_type.lower() != "stream":
                        continue

                    # Read messages from the stream
                    response = await client.xreadgroup(
                        groupname=settings.STREAM_GROUP,
                        consumername=settings.STREAM_CONSUMER,
                        streams={stream_key: ">"},
                        count=settings.STREAM_BATCH_SIZE,
                        block=settings.STREAM_READ_TIMEOUT_MS,
                    )

                    if response:
                        # Process the response
                        for stream_data in response:
                            if stream_data[0] == stream_key and stream_data[1]:
                                # Format: [(entry_id, {field: value, ...}), ...]
                                result = [
                                    (entry_id, message)
                                    for entry_id, message in stream_data[1]
                                ]
                                return (stream_key, result)
            except RedisError as e:
                logger.error(
                    f"Error reading from stream '{stream_key}' in DB {db}: {e}"
                )

        # Stream not found in any database or no messages available
        return None

    async def ack_messages(self, stream_key: str, entry_ids: List[str]) -> int:
        """
        Acknowledge messages from a stream.

        Safely handles potentially null Redis connections and uses proper async/await
        patterns when interacting with Redis streams.

        Args:
            stream_key: The key of the stream to acknowledge messages from
            entry_ids: List of message IDs to acknowledge

        Returns:
            Number of messages successfully acknowledged
        """
        if not entry_ids:
            return 0

        await self._rate_limiter.acquire()

        # Try all configured databases
        for db in settings.REDIS_DBS_TO_SEARCH:
            if not await self.connect(db):
                continue

            try:
                # Check if the stream exists in this database
                client = self._clients[db]
                if client is not None:
                    key_type = await client.type(stream_key)
                    if key_type.lower() != "stream":
                        continue

                    # Acknowledge messages
                    ack_count = await client.xack(
                        stream_key, settings.STREAM_GROUP, *entry_ids
                    )
                    logger.debug(
                        f"Acknowledged {ack_count}/{len(entry_ids)} messages from stream '{stream_key}' in DB {db}"
                    )
                    return int(ack_count)  # Ensure we return an int
            except RedisError as e:
                logger.error(
                    f"Error acknowledging messages from stream '{stream_key}' in DB {db}: {e}"
                )

        # Stream not found in any database
        return 0
