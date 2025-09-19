#!/usr/bin/env python3
"""
Redis Service
------------
Handles Redis connections, discovery, reading, and acking.
"""

import asyncio
import fnmatch
import logging
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import redis.asyncio as aioredis
from redis.asyncio.client import Redis

from ..config import settings
from ..core.exceptions import (
    RedisConnectionError,
    RedisError,
    is_busygroup_error,
    is_nogroup_error,
)
from ..core.types import (
    AckResult,
    ConnectionResult,
    MessageResult,
    OperationResult,
    RedisConsumerGroup,
    RedisMessage,
    RedisMessageID,
    RedisStreamOffset,
    SyncResult,
    ValidationResult,
)
from .interfaces import RedisServiceInterface

logger = logging.getLogger("redis-pg-sync")


class RedisService(RedisServiceInterface):
    """Service for Redis operations."""

    def __init__(self):
        """Initialize the Redis service."""
        self.redis_pools: Dict[int, Optional[Redis]] = (
            {}
        )  # Dictionary to hold connection pools for each DB
        self._lock = asyncio.Lock()
        self._connection_attempts: Dict[int, int] = (
            {}
        )  # Track connection attempts per DB
        self._last_health_check: Dict[int, float] = (
            {}
        )  # Track last health check time per DB
        self._health_check_interval = settings.REDIS_HEALTH_CHECK_INTERVAL

    async def connect(self, db_num: int) -> ConnectionResult:
        """
        Establish connection to Redis with retry logic and health checks for a specific DB.

        Args:
            db_num: Redis database number

        Returns:
            ConnectionResult indicating success or failure
        """
        pool = await self._get_redis_pool(db_num)
        return {
            "success": pool is not None,
            "error": None if pool is not None else "Failed to connect to Redis",
            "data": None,
        }

    async def _get_redis_pool(self, db: int) -> Optional[Redis]:
        """
        Get or create a Redis connection pool for a specific DB with retry logic.

        Args:
            db: Redis database number

        Returns:
            Redis client or None if connection failed
        """
        async with self._lock:
            # Validate pool size
            if settings.REDIS_POOL_SIZE < 1:
                logger.error(
                    f"Invalid Redis pool size: {settings.REDIS_POOL_SIZE}. Must be at least 1."
                )
                return None

            # Check if we need to perform a health check
            current_time = time.time()
            if db in self._last_health_check:
                if (
                    current_time - self._last_health_check[db]
                    < self._health_check_interval
                ):
                    return self.redis_pools.get(db)

            # Initialize connection attempts counter if not exists
            if db not in self._connection_attempts:
                self._connection_attempts[db] = 0

            # If pool exists, verify it's healthy
            if db in self.redis_pools and self.redis_pools[db] is not None:
                try:
                    redis_client = self.redis_pools[db]
                    assert redis_client is not None  # Help type checker understand
                    if await self._check_connection_health(redis_client):
                        self._last_health_check[db] = current_time
                        return self.redis_pools[db]
                    else:
                        logger.warning(
                            f"Unhealthy connection detected for DB {db}, attempting to reconnect"
                        )
                        redis_client = self.redis_pools[db]
                        assert redis_client is not None  # Help type checker
                        await redis_client.close()
                        self.redis_pools[db] = None
                except Exception as e:
                    logger.error(f"Health check failed for DB {db}: {e}")
                    self.redis_pools[db] = None

            # Attempt to create new connection with exponential backoff
            while self._connection_attempts[db] < settings.REDIS_MAX_RETRIES:
                try:
                    # Calculate backoff delay
                    delay = min(
                        settings.REDIS_BACKOFF_BASE
                        * (2 ** self._connection_attempts[db]),
                        settings.REDIS_BACKOFF_MAX,
                    )

                    # Create a new connection pool
                    pool = aioredis.ConnectionPool(
                        host=settings.REDIS_HOST,
                        port=settings.REDIS_PORT,
                        db=db,
                        password=(
                            settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None
                        ),
                        socket_timeout=5,
                        decode_responses=True,
                        max_connections=settings.REDIS_POOL_SIZE,
                    )
                    client = aioredis.Redis(connection_pool=pool)

                    # Test connection
                    if await self._check_connection_health(client):
                        self.redis_pools[db] = client
                        self._last_health_check[db] = current_time
                        self._connection_attempts[db] = 0
                        logger.info(f"Created Redis connection pool for DB {db}")
                        return cast(Redis, client)  # Explicitly cast to Redis

                except Exception as e:
                    logger.error(
                        f"Connection attempt {self._connection_attempts[db] + 1} failed for DB {db}: {e}"
                    )

                self._connection_attempts[db] += 1
                if self._connection_attempts[db] < settings.REDIS_MAX_RETRIES:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Max connection attempts reached for DB {db}")
                    return None

            return None  # Explicitly return None to match return type

    async def _check_connection_health(self, client: Redis) -> bool:
        """
        Perform a health check on a Redis connection.

        Args:
            client: Redis client to check

        Returns:
            True if healthy, False otherwise
        """
        try:
            # Check if connection is responsive
            if not await client.ping():
                return False

            # Check if connection pool is healthy
            pool = client.connection_pool
            if pool is None:
                return False

            # Check if we can perform a basic operation
            await client.time()
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def _parse_stream_name(self, stream_name: str) -> Tuple[int, Optional[str]]:
        """
        Parse a potentially prefixed stream name into (db_num, original_key).

        Args:
            stream_name: Stream name to parse (possibly prefixed with db{number}:)

        Returns:
            Tuple of (db_num, original_key)
        """
        # Pattern for matching db{number}:stream_name format
        pattern = r"^db(\d+):(.+)$"
        match = re.match(pattern, stream_name)

        if match:
            try:
                db_num = int(match.group(1))
                original_key = match.group(2)
                return db_num, original_key
            except ValueError:
                logger.error(f"Invalid DB number format in stream name: {stream_name}")
                return settings.REDIS_DB, None
        else:
            return settings.REDIS_DB, stream_name

    async def discover_streams(self) -> List[Dict[str, Any]]:
        """
        Discover Redis streams using error helpers.

        Returns:
            List of dictionaries containing stream information with keys:
            - display_name: str
            - db_num: int
            - stream_key: str
        """
        if not settings.STREAM_AUTO_DISCOVERY:
            return []

        logger.info("Starting Redis stream auto-discovery...")
        discovered_streams_info: List[Dict[str, Any]] = []

        for db_num in settings.REDIS_DBS_TO_SEARCH:
            redis_client = await self._get_redis_pool(db_num)
            if not redis_client:
                logger.warning(
                    f"Cannot discover streams in DB {db_num}: Redis connection failed"
                )
                continue

            try:
                # Scan for keys matching the patterns
                for pattern in settings.STREAM_DISCOVERY_PATTERNS:
                    keys = await self.scan_keys(db_num, pattern)

                    for key in keys:
                        try:
                            # Check if the key is a stream
                            stream_info = await redis_client.xinfo_stream(key)
                            if stream_info:
                                # Create a display name that includes the DB number
                                display_name = f"db{db_num}:{key}"
                                discovered_streams_info.append(
                                    {
                                        "display_name": display_name,
                                        "db_num": db_num,
                                        "stream_key": key,
                                    }
                                )
                                logger.debug(f"Discovered stream: {display_name}")
                        except aioredis.ResponseError as e:
                            # Skip if not a stream
                            if "WRONGTYPE" in str(e):
                                continue
                            logger.warning(
                                f"Error checking if key {key} is a stream: {e}"
                            )

            except Exception as e:
                logger.error(f"Error during stream discovery in DB {db_num}: {e}")

        logger.info(f"Discovered {len(discovered_streams_info)} streams")
        return discovered_streams_info

    async def _create_consumer_group(self, db_num: int, stream_key: str) -> bool:
        """
        Create a consumer group for a stream if it doesn't already exist.

        Args:
            db_num: Redis database number
            stream_key: Stream key to create consumer group for

        Returns:
            True if successful or already exists, False otherwise
        """
        if not settings.STREAM_AUTO_CREATE_CONSUMER_GROUPS:
            return False

        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(
                f"Cannot create consumer group for {stream_key}: Redis connection failed"
            )
            return False

        try:
            # Try to create the consumer group
            await redis_client.xgroup_create(
                stream_key, settings.STREAM_CONSUMER_GROUP, id="0-0", mkstream=True
            )
            logger.info(
                f"Created consumer group {settings.STREAM_CONSUMER_GROUP} for stream {stream_key} in DB {db_num}"
            )
            return True
        except aioredis.ResponseError as e:
            # Group already exists
            if is_busygroup_error(e):
                logger.debug(
                    f"Consumer group {settings.STREAM_CONSUMER_GROUP} already exists for stream {stream_key}"
                )
                return True
            else:
                logger.error(
                    f"Error creating consumer group for stream {stream_key}: {e}"
                )
                return False
        except Exception as e:
            logger.error(
                f"Unexpected error creating consumer group for stream {stream_key}: {e}"
            )
            return False

    async def setup_consumer_groups(
        self, streams_to_process: List[str]
    ) -> OperationResult:
        """
        Set up consumer groups for streams.

        Args:
            streams_to_process: List of stream names to process

        Returns:
            OperationResult indicating success or failure of the operation
        """
        success = True
        errors = []

        for stream_name in streams_to_process:
            db_num, stream_key = self._parse_stream_name(stream_name)
            if stream_key:
                result = await self._create_consumer_group(db_num, stream_key)
                if not result:
                    success = False
                    errors.append(
                        f"Failed to create consumer group for stream {stream_name}"
                    )

        return {
            "success": success,
            "error": None if success else "; ".join(errors),
            "data": None,
        }

    async def read_stream_messages(
        self,
        stream_name: str,
        count: int = 10,
        block: int = 0,
        last_id: RedisStreamOffset = ">",
    ) -> MessageResult:
        """
        Read messages from a stream.

        Args:
            stream_name: Name of the stream to read from
            count: Number of messages to read
            block: Time to block in milliseconds
            last_id: ID or timestamp to start reading from

        Returns:
            MessageResult containing the read messages
        """
        db_num, stream_key = self._parse_stream_name(stream_name)
        if not stream_key:
            logger.error(f"Invalid stream name: {stream_name}")
            return {
                "success": False,
                "error": f"Invalid stream name: {stream_name}",
                "data": None,
                "message_id": None,
                "stream": None,
                "messages": [],
            }

        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(f"Cannot read stream {stream_name}: Redis connection failed")
            return {
                "success": False,
                "error": f"Cannot read stream {stream_name}: Redis connection failed",
                "data": None,
                "message_id": None,
                "stream": None,
                "messages": [],
            }

        try:
            # Create the consumer group if it doesn't exist
            if not await self._create_consumer_group(db_num, stream_key):
                return {
                    "success": False,
                    "error": f"Failed to create consumer group for stream {stream_name}",
                    "data": None,
                    "message_id": None,
                    "stream": None,
                    "messages": [],
                }

            # Read messages from the stream
            messages = await redis_client.xreadgroup(
                settings.STREAM_CONSUMER_GROUP,
                settings.STREAM_CONSUMER_NAME,
                {stream_key: last_id},
                count=count,
                block=block,
            )

            if not messages:
                return {
                    "success": True,
                    "error": None,
                    "data": None,
                    "message_id": None,
                    "stream": stream_key,
                    "messages": [],
                }

            # Process the messages
            parsed_messages: List[Tuple[str, Dict[str, Any]]] = []
            latest_message_id = None
            for _, message_list in messages:
                for message_id, message_data in message_list:
                    parsed_messages.append((message_id, message_data))
                    if latest_message_id is None or message_id > latest_message_id:
                        latest_message_id = message_id

            return {
                "success": True,
                "error": None,
                "data": None,
                "message_id": latest_message_id,
                "stream": stream_key,
                "messages": parsed_messages,
            }

        except aioredis.ResponseError as e:
            # Handle special cases
            if is_nogroup_error(e):
                # Group doesn't exist, create it
                success = await self._create_consumer_group(db_num, stream_key)
                if success:
                    # Try reading again
                    return await self.read_stream_messages(
                        stream_name, count, block, last_id
                    )
                return {
                    "success": False,
                    "error": f"Failed to create consumer group for stream {stream_name}",
                    "data": None,
                    "message_id": None,
                    "stream": None,
                    "messages": [],
                }
            else:
                logger.error(f"Redis error reading stream {stream_name}: {e}")
                return {
                    "success": False,
                    "error": f"Redis error reading stream {stream_name}: {e}",
                    "data": None,
                    "message_id": None,
                    "stream": None,
                    "messages": [],
                }
        except Exception as e:
            logger.error(f"Unexpected error reading stream {stream_name}: {e}")
            return {
                "success": False,
                "error": f"Unexpected error reading stream {stream_name}: {e}",
                "data": None,
                "message_id": None,
                "stream": None,
                "messages": [],
            }

    async def ack_messages(self, stream_key: str, message_ids: List[str]) -> AckResult:
        """
        Acknowledge messages in a stream.

        Args:
            stream_key: Stream key to acknowledge in
            message_ids: List of message IDs to acknowledge

        Returns:
            AckResult containing acknowledgment statistics
        """
        if not message_ids:
            return {"success": True, "error": None, "data": None, "count": 0}

        # Parse the stream key to get the DB number
        db_num, original_key = self._parse_stream_name(stream_key)
        if not original_key:
            logger.error(f"Invalid stream key: {stream_key}")
            return {
                "success": False,
                "error": f"Invalid stream key: {stream_key}",
                "data": None,
                "count": 0,
            }

        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(f"Cannot ack messages: Redis connection failed")
            return {
                "success": False,
                "error": "Cannot ack messages: Redis connection failed",
                "data": None,
                "count": 0,
            }

        try:
            # Acknowledge the messages
            ack_count = await redis_client.xack(
                original_key, settings.STREAM_CONSUMER_GROUP, *message_ids
            )
            logger.debug(
                f"Acknowledged {ack_count}/{len(message_ids)} messages in stream {stream_key}"
            )
            return {
                "success": ack_count > 0,
                "error": (
                    None
                    if ack_count > 0
                    else f"Failed to acknowledge any messages in stream {stream_key}"
                ),
                "data": None,
                "count": ack_count,
            }
        except Exception as e:
            logger.error(f"Error acknowledging messages in stream {stream_key}: {e}")
            return {
                "success": False,
                "error": f"Error acknowledging messages in stream {stream_key}: {e}",
                "data": None,
                "count": 0,
            }

    async def read_key_value(self, key: str, db_num: int) -> Optional[Dict[str, Any]]:
        """
        Read a key-value pair from Redis.

        Args:
            key: Key to read
            db_num: Database number to read from

        Returns:
            Dictionary containing key and value, or None if error
        """
        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(f"Cannot read key: Redis connection failed")
            return None

        try:
            # First check the type of the key
            key_type = await redis_client.type(key)

            # Only proceed if it's a string
            if key_type != "string":
                logger.debug(f"Skipping non-string key {key} of type {key_type}")
                return None

            # Get the value
            value = await redis_client.get(key)
            if value is None:
                return None
            return {"key": key, "value": value, "type": key_type}
        except Exception as e:
            logger.error(f"Error reading key {key} from DB {db_num}: {e}")
            return None

    async def scan_keys(
        self, db_num: int, pattern: str = "*", count: int = 100
    ) -> List[str]:
        """
        Scan keys in Redis.

        Args:
            db_num: Database number to scan
            pattern: Pattern to match keys against
            count: Number of keys to return per scan

        Returns:
            List of keys
        """
        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(f"Cannot scan keys: Redis connection failed")
            return []

        try:
            # Scan for keys matching the pattern
            keys = []
            cursor = 0
            while True:
                cursor, partial_keys = await redis_client.scan(cursor, pattern, count)
                keys.extend(partial_keys)
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            logger.error(
                f"Error scanning keys in DB {db_num} with pattern {pattern}: {e}"
            )
            return []

    async def close(self) -> None:
        """Close Redis connections."""
        for db_num, redis_client in self.redis_pools.items():
            if redis_client:
                try:
                    await redis_client.close()
                    logger.info(f"Closed Redis connection for DB {db_num}")
                except Exception as e:
                    logger.error(f"Error closing Redis connection for DB {db_num}: {e}")

        # Clear connection pools
        self.redis_pools.clear()
        self._connection_attempts.clear()
        self._last_health_check.clear()

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        await self.close()

    async def ping(self) -> bool:
        """
        Ping the Redis server to check connection.

        Returns:
            bool: True if connection is healthy, False otherwise
        """
        try:
            # Try to ping any available pool
            for db_num, redis_client in self.redis_pools.items():
                if redis_client:
                    if await redis_client.ping():
                        return True
            return False
        except Exception as e:
            logger.error(f"Redis ping failed: {e}")
            return False

    async def write_messages(
        self, key: str, messages: List[Tuple[RedisMessageID, RedisMessage]]
    ) -> List[RedisMessageID]:
        """
        Write messages to a Redis stream.

        Args:
            key: Stream key to write to
            messages: List of (message_id, fields) tuples to write

        Returns:
            List of successfully written message IDs
        """
        # Extract db_num from key if it contains a database prefix
        db_num = 0  # Default to DB 0
        if ":" in key:
            try:
                db_num = int(key.split(":")[0])
                key = key.split(":", 1)[1]
            except (ValueError, IndexError):
                pass

        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(
                f"Cannot write messages: Redis connection failed for DB {db_num}"
            )
            return []

        try:
            # Use pipeline for better performance
            pipe = redis_client.pipeline()
            for message_id, fields in messages:
                # Ensure fields are of proper types for Redis
                redis_fields: Dict[
                    Union[bytes, bytearray, memoryview, str, int, float],
                    Union[bytes, bytearray, memoryview, str, int, float],
                ] = {}

                for k, v in fields.items():
                    # Convert keys and values to appropriate types
                    redis_fields[k] = v

                pipe.xadd(key, redis_fields, id=message_id)
            results = await pipe.execute()

            # Filter out None results (failed writes)
            successful_ids = [
                message_id
                for message_id, result in zip([msg[0] for msg in messages], results)
                if result is not None
            ]

            logger.debug(
                f"Wrote {len(successful_ids)}/{len(messages)} messages to stream {key}"
            )
            return successful_ids
        except Exception as e:
            logger.error(f"Failed to write messages to stream {key}: {e}")
            return []

    async def validate_stream(self, stream_key: str) -> ValidationResult:
        """
        Validate a Redis stream's configuration and state.

        Args:
            stream_key: Stream key to validate

        Returns:
            ValidationResult indicating if stream is valid
        """
        # Extract db_num from key if it contains a database prefix
        db_num = 0  # Default to DB 0
        if ":" in stream_key:
            try:
                db_num = int(stream_key.split(":")[0])
                stream_key = stream_key.split(":", 1)[1]
            except (ValueError, IndexError):
                pass

        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(
                f"Cannot validate stream: Redis connection failed for DB {db_num}"
            )
            return {
                "success": False,
                "error": f"Redis connection failed for DB {db_num}",
                "data": None,
                "is_valid": False,
                "details": {
                    "reason": "Redis connection failed",
                    "db_num": db_num,
                    "stream_key": stream_key,
                },
            }

        try:
            # Check if stream exists
            stream_info = await redis_client.xinfo_stream(stream_key)
            if not stream_info:
                return {
                    "success": False,
                    "error": f"Stream {stream_key} does not exist",
                    "data": None,
                    "is_valid": False,
                    "details": {
                        "reason": "Stream does not exist",
                        "stream_key": stream_key,
                        "db_num": db_num,
                    },
                }

            # Check if stream has messages
            length = stream_info.get("length", 0)
            if length == 0:
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "length": 0,
                        "groups": [],
                        "first_id": None,
                        "last_id": None,
                    },
                    "is_valid": True,
                    "details": {
                        "reason": "Stream exists but is empty",
                        "stream_key": stream_key,
                        "db_num": db_num,
                        "length": 0,
                    },
                }

            # Get first and last message IDs
            first_entry = stream_info.get("first-entry", [None, None])
            last_entry = stream_info.get("last-entry", [None, None])
            first_id = first_entry[0] if first_entry else None
            last_id = last_entry[0] if last_entry else None

            # Get consumer groups
            groups = []
            try:
                group_info = await redis_client.xinfo_groups(stream_key)
                for group in group_info:
                    groups.append(
                        {
                            "name": group["name"],
                            "consumers": group["consumers"],
                            "pending": group["pending"],
                            "last_delivered_id": group["last-delivered-id"],
                        }
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to get consumer groups for stream {stream_key}: {e}"
                )

            return {
                "success": True,
                "error": None,
                "data": {
                    "length": length,
                    "groups": groups,
                    "first_id": first_id,
                    "last_id": last_id,
                },
                "is_valid": True,
                "details": {
                    "reason": "Stream is valid",
                    "stream_key": stream_key,
                    "db_num": db_num,
                    "length": length,
                    "group_count": len(groups),
                    "first_id": first_id,
                    "last_id": last_id,
                },
            }
        except Exception as e:
            logger.error(f"Failed to validate stream {stream_key}: {e}")
            return {
                "success": False,
                "error": f"Failed to validate stream: {e}",
                "data": None,
                "is_valid": False,
                "details": {
                    "reason": "Validation failed",
                    "error": str(e),
                    "stream_key": stream_key,
                    "db_num": db_num,
                },
            }

    async def sync_stream(
        self, source_stream: str, target_stream: str, consumer_group: RedisConsumerGroup
    ) -> SyncResult:
        """
        Synchronize messages from source stream to target stream.

        Args:
            source_stream: Source stream key
            target_stream: Target stream key
            consumer_group: Consumer group information

        Returns:
            SyncResult containing synchronization statistics
        """
        # Extract db_num from source_stream if it contains a database prefix
        db_num = 0  # Default to DB 0
        if ":" in source_stream:
            try:
                db_num = int(source_stream.split(":")[0])
                source_stream = source_stream.split(":", 1)[1]
            except (ValueError, IndexError):
                pass

        redis_client = await self._get_redis_pool(db_num)
        if not redis_client:
            logger.error(f"Cannot sync stream: Redis connection failed for DB {db_num}")
            return {
                "success": False,
                "error": f"Redis connection failed for DB {db_num}",
                "data": None,
                "synced_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
            }

        try:
            # Get group_name and consumer_name from consumer_group
            # Handle both dictionary-based and tuple-based consumer_group
            if isinstance(consumer_group, dict):
                group_name = consumer_group.get("group_name")
                consumer_name = consumer_group.get("consumer_name")
            else:
                # Assuming it's a tuple or a sequence with at least 2 elements
                group_name = (
                    consumer_group[0]
                    if len(consumer_group) > 0
                    else settings.STREAM_CONSUMER_GROUP
                )
                consumer_name = (
                    consumer_group[1]
                    if len(consumer_group) > 1
                    else settings.STREAM_CONSUMER_NAME
                )

            # Read messages from source stream
            messages = await redis_client.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={source_stream: ">"},
                count=100,  # Read in batches
            )

            if not messages:
                return {
                    "success": True,
                    "error": None,
                    "data": None,
                    "synced_count": 0,
                    "failed_count": 0,
                    "skipped_count": 0,
                }

            # Write messages to target stream
            successful_ids = []
            error_count = 0
            skipped_count = 0

            for stream, stream_messages in messages:
                for message_id, fields in stream_messages:
                    try:
                        # Write to target stream
                        result = await redis_client.xadd(
                            target_stream, fields, id=message_id
                        )
                        if result:
                            successful_ids.append(message_id)
                        else:
                            skipped_count += 1
                    except Exception as e:
                        logger.error(
                            f"Failed to write message {message_id} to target stream: {e}"
                        )
                        error_count += 1

            # Acknowledge successfully synced messages
            if successful_ids:
                await redis_client.xack(source_stream, group_name, *successful_ids)

            return {
                "success": True,
                "error": None,
                "data": None,
                "synced_count": len(successful_ids),
                "failed_count": error_count,
                "skipped_count": skipped_count,
            }
        except Exception as e:
            logger.error(
                f"Failed to sync stream {source_stream} to {target_stream}: {e}"
            )
            return {
                "success": False,
                "error": f"Failed to sync stream: {e}",
                "data": None,
                "synced_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
            }
