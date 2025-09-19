import asyncio
import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# Configuration from environment (defaults provided)
# Source Redis (local instance where data originates) configuration
SOURCE_REDIS_HOST = os.getenv("SOURCE_REDIS_HOST", "localhost")
SOURCE_REDIS_PORT = int(os.getenv("SOURCE_REDIS_PORT", 6379))
SOURCE_REDIS_PASSWORD = os.getenv("SOURCE_REDIS_PASSWORD", "")
SOURCE_REDIS_DB_SEARCH_RANGE = os.getenv("SOURCE_REDIS_DB_SEARCH_RANGE", "0-15")

# Target Redis (remote instance for caching before Kafka) configuration
TARGET_REDIS_HOST = os.getenv("TARGET_REDIS_HOST", "redis-cache")
TARGET_REDIS_PORT = int(os.getenv("TARGET_REDIS_PORT", 6379))
TARGET_REDIS_PASSWORD = os.getenv("TARGET_REDIS_PASSWORD", "")
TARGET_REDIS_TIME_TO_LIVE = int(os.getenv("TARGET_REDIS_TIME_TO_LIVE", 3600))
TARGET_REDIS_POOL_SIZE = int(os.getenv("TARGET_REDIS_POOL_SIZE", 10))
TARGET_REDIS_DB = int(os.getenv("TARGET_REDIS_DB", 0))  # Default DB for target

# Common Redis configuration
KAFKA_SYNC_PREFIX = "kafka_sync:"
UNSYNCED_KEYS_SET = "unsynced_keys_set"


def parse_db_range(range_str: str) -> List[int]:
    """Parses a comma-separated list/range string into a list of DB numbers."""
    db_list = []
    if not range_str:
        return [0]  # Default to DB 0 if empty string
    for part in range_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = map(int, part.split("-"))
                if start <= end:
                    db_list.extend(range(start, end + 1))
                else:
                    logger.warning(
                        f"Invalid DB range (start > end): {part} in DB_SEARCH_RANGE"
                    )
            except ValueError:
                logger.warning(
                    f"Invalid DB range format: {part} in SOURCE_REDIS_DB_SEARCH_RANGE"
                )
        else:
            try:
                db_list.append(int(part))
            except ValueError:
                logger.warning(
                    f"Invalid DB number: {part} in SOURCE_REDIS_DB_SEARCH_RANGE"
                )
    return sorted(list(set(db_list)))  # Use list(set(...)) for unique elements


SOURCE_REDIS_DBS_TO_SEARCH = parse_db_range(SOURCE_REDIS_DB_SEARCH_RANGE)


async def handle_redis_error(operation_desc: str, coro, *args, **kwargs):
    """Helper to execute a Redis coroutine and handle common errors."""
    try:
        return await coro(*args, **kwargs)
    except (aioredis.RedisError, ConnectionError, asyncio.TimeoutError) as e:
        logger.error(f"Redis error during '{operation_desc}': {e}")
    except Exception as e:
        logger.error(f"Unexpected error during '{operation_desc}': {e}")
    return None  # Indicate failure


class RedisManager:
    """
    Manages Redis connection pools and operations for caching and sync tracking.
    Handles both source_redis (local data source) and target_redis (remote cache).
    """

    def __init__(self):
        # Dictionary of Redis clients for source_redis (local), keyed by DB number
        self.source_redis_pools: Dict[int, aioredis.Redis] = {}
        # Dictionary of Redis clients for target_redis (remote), keyed by DB number
        self.target_redis_pools: Dict[int, aioredis.Redis] = {}
        # DBs to search for streams in source_redis
        self.source_dbs_to_search = SOURCE_REDIS_DBS_TO_SEARCH
        logger.info(
            f"RedisManager initialized. Will search source DBs: {self.source_dbs_to_search}"
        )
        logger.info(
            f"Target Redis configured at {TARGET_REDIS_HOST}:{TARGET_REDIS_PORT} DB {TARGET_REDIS_DB}"
        )

    async def get_source_redis_client(self, db: int = 0) -> Optional[aioredis.Redis]:
        """
        Get a source_redis client from the pool for a specific DB, creating the pool if needed.
        Source Redis is the local instance where data originates (e.g., streams).
        """
        if db not in self.source_redis_pools or self.source_redis_pools[db] is None:
            logger.info(f"Creating source Redis connection pool for DB {db}...")
            try:
                pool = aioredis.ConnectionPool(
                    host=SOURCE_REDIS_HOST,
                    port=SOURCE_REDIS_PORT,
                    db=db,
                    password=SOURCE_REDIS_PASSWORD if SOURCE_REDIS_PASSWORD else None,
                    socket_timeout=5,
                    decode_responses=False,  # Keep as bytes for flexibility
                    max_connections=10,  # Default pool size for source
                )
                client = aioredis.Redis(connection_pool=pool)
                # Test connection
                if (
                    await handle_redis_error(
                        f"pinging source Redis DB {db}", client.ping
                    )
                    is None
                ):
                    await client.close()  # Clean up client if ping failed
                    raise ConnectionError(f"Ping failed for source Redis DB {db}")
                self.source_redis_pools[db] = client
                logger.info(
                    f"Successfully created and connected source Redis pool for DB {db}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to create source Redis connection pool for DB {db}: {e}"
                )
                self.source_redis_pools[db] = None  # Mark as failed
                return None

        # Check if pool exists but might have failed previously
        if self.source_redis_pools.get(db) is None:
            logger.warning(
                f"Source Redis pool for DB {db} previously failed to initialize."
            )
            return None

        return self.source_redis_pools[db]

    async def get_target_redis_client(
        self, db: int = TARGET_REDIS_DB
    ) -> Optional[aioredis.Redis]:
        """
        Get a target_redis client from the pool for a specific DB, creating the pool if needed.
        Target Redis is the remote instance used for caching data before Kafka sync.
        """
        if db not in self.target_redis_pools or self.target_redis_pools[db] is None:
            logger.info(f"Creating target Redis connection pool for DB {db}...")
            try:
                pool = aioredis.ConnectionPool(
                    host=TARGET_REDIS_HOST,
                    port=TARGET_REDIS_PORT,
                    db=db,
                    password=TARGET_REDIS_PASSWORD if TARGET_REDIS_PASSWORD else None,
                    socket_timeout=5,
                    decode_responses=False,  # Keep as bytes for Kafka/flexibility
                    max_connections=TARGET_REDIS_POOL_SIZE,
                )
                client = aioredis.Redis(connection_pool=pool)
                # Test connection
                if (
                    await handle_redis_error(
                        f"pinging target Redis DB {db}", client.ping
                    )
                    is None
                ):
                    await client.close()  # Clean up client if ping failed
                    raise ConnectionError(f"Ping failed for target Redis DB {db}")
                self.target_redis_pools[db] = client
                logger.info(
                    f"Successfully created and connected target Redis pool for DB {db}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to create target Redis connection pool for DB {db}: {e}"
                )
                self.target_redis_pools[db] = None  # Mark as failed
                return None

        # Check if pool exists but might have failed previously
        if self.target_redis_pools.get(db) is None:
            logger.warning(
                f"Target Redis pool for DB {db} previously failed to initialize."
            )
            return None

        return self.target_redis_pools[db]

    async def close_all_pools(self):
        """Close all managed Redis connection pools."""
        logger.info("Closing all Redis connection pools...")

        # Close source Redis pools
        for db, client in self.source_redis_pools.items():
            if client:
                try:
                    await client.close()
                    logger.debug(f"Closed source Redis pool for DB {db}")
                except Exception as e:
                    logger.error(f"Error closing source Redis pool for DB {db}: {e}")
        self.source_redis_pools.clear()

        # Close target Redis pools
        for db, client in self.target_redis_pools.items():
            if client:
                try:
                    await client.close()
                    logger.debug(f"Closed target Redis pool for DB {db}")
                except Exception as e:
                    logger.error(f"Error closing target Redis pool for DB {db}: {e}")
        self.target_redis_pools.clear()

        logger.info("Finished closing all Redis pools.")

    async def cache_message(
        self,
        key: str,
        message: bytes,
        db: int = TARGET_REDIS_DB,
        ttl: int = TARGET_REDIS_TIME_TO_LIVE,
    ) -> bool:
        """
        Cache a message (bytes) in target Redis and add the key to the unsynced_keys_set.
        Uses a transaction to ensure both operations succeed or fail together.
        """
        target_redis = await self.get_target_redis_client(db)
        if not target_redis:
            logger.error(
                f"Failed to get target Redis client for DB {db} to cache message {key}"
            )
            return False

        try:
            # Use a transaction to ensure both operations happen atomically
            tr = target_redis.pipeline(transaction=True)
            # Set the message with TTL
            tr.set(key, message, ex=ttl)
            # Add the key to the unsynced keys set
            tr.sadd(UNSYNCED_KEYS_SET, key)
            # Execute the transaction
            results = await handle_redis_error(
                f"transaction to cache message and track {key} in target Redis DB {db}",
                tr.execute,
            )
            if results is None:
                return False
            logger.debug(
                f"Cached message with key: {key} in target Redis DB {db} and added to unsynced set"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to cache message and track {key} in target Redis DB {db}: {e}"
            )
            return False

    async def get_message(self, key: str, db: int = TARGET_REDIS_DB) -> Optional[bytes]:
        """Retrieve a message (bytes) from target Redis."""
        target_redis = await self.get_target_redis_client(db)
        if not target_redis:
            logger.error(
                f"Failed to get target Redis client for DB {db} to retrieve key {key}"
            )
            return None

        return await handle_redis_error(
            f"getting value for key {key} from target Redis DB {db}",
            target_redis.get,
            key,
        )

    async def mark_as_synced(
        self,
        key: str,
        db: int = TARGET_REDIS_DB,
        ttl: Optional[int] = TARGET_REDIS_TIME_TO_LIVE,
    ) -> bool:
        """
        Mark a message key as synced in target Redis and remove it from the unsynced keys set.
        Uses a transaction to ensure both operations succeed or fail together.
        """
        target_redis = await self.get_target_redis_client(db)
        if not target_redis:
            logger.error(
                f"Failed to get target Redis client for DB {db} to mark sync for key {key}"
            )
            return False

        sync_key = f"{KAFKA_SYNC_PREFIX}{key}"

        try:
            # Use a transaction to ensure both operations happen atomically
            tr = target_redis.pipeline(transaction=True)
            # Set the sync marker with TTL (if provided)
            if ttl is not None:
                tr.set(sync_key, "1", ex=ttl)
            else:
                tr.set(sync_key, "1")  # No TTL, persist indefinitely
            # Remove the key from the unsynced keys set
            tr.srem(UNSYNCED_KEYS_SET, key)
            # Execute the transaction
            results = await handle_redis_error(
                f"transaction to mark {key} as synced and remove from unsynced set in target Redis DB {db}",
                tr.execute,
            )
            if results is None:
                return False
            logger.debug(
                f"Marked key {key} as synced in target Redis DB {db} (sync key: {sync_key}) with TTL {ttl} and removed from unsynced set"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to mark {key} as synced and remove from unsynced set in target Redis DB {db}: {e}"
            )
            return False

    async def is_synced(self, key: str, db: int = TARGET_REDIS_DB) -> bool:
        """Check if a message key is marked as synced in target Redis."""
        target_redis = await self.get_target_redis_client(db)
        if not target_redis:
            logger.error(
                f"Failed to get target Redis client for DB {db} to check sync status for key {key}"
            )
            return False  # Assume not synced if Redis fails

        sync_key = f"{KAFKA_SYNC_PREFIX}{key}"
        result = await handle_redis_error(
            f"checking sync status for key {key} in target Redis DB {db} (sync key: {sync_key})",
            target_redis.exists,
            sync_key,
        )
        return bool(result)  # Returns True if exists, False if not exists or error

    async def get_unsynced_keys(self) -> List[Tuple[int, str]]:
        """
        Get unsynced keys from target Redis using the dedicated unsynced_keys_set for each DB.
        This is much more efficient than scanning the entire keyspace.

        Returns a list of tuples: [(db_num, key_str)].
        """
        unsynced_keys_found: List[Tuple[int, str]] = []

        # For target Redis, we only check the configured target DB
        target_db = TARGET_REDIS_DB
        logger.info(f"Getting unsynced keys from target Redis DB {target_db}")

        target_redis = await self.get_target_redis_client(target_db)
        if not target_redis:
            logger.warning(
                f"Skipping target Redis DB {target_db} for unsynced check - cannot get client."
            )
            return unsynced_keys_found

        logger.debug(
            f"Getting members of unsynced keys set in target Redis DB {target_db}..."
        )
        set_members = await handle_redis_error(
            f"getting unsynced keys from set in target Redis DB {target_db}",
            target_redis.smembers,
            UNSYNCED_KEYS_SET,
        )

        if set_members is None:
            logger.error(
                f"Failed to retrieve unsynced keys set from target Redis DB {target_db}"
            )
            return unsynced_keys_found

        # Decode key bytes and add to results
        for member in set_members:
            try:
                key_str = member.decode("utf-8")
                unsynced_keys_found.append((target_db, key_str))
            except UnicodeDecodeError:
                logger.warning(
                    f"Skipping non-UTF8 key in unsynced set, target Redis DB {target_db}: {member[:50]}..."
                )
                # Optionally, consider removing invalid keys from the set
                # await target_redis.srem(UNSYNCED_KEYS_SET, member)

        logger.info(
            f"Found {len(unsynced_keys_found)} unsynced keys in target Redis DB {target_db}"
        )
        return unsynced_keys_found

    async def rebuild_unsynced_keys_set(self, db: int = TARGET_REDIS_DB) -> int:
        """
        Rebuild the unsynced keys set in target Redis by scanning all keys in the DB.
        Useful for recovery if the unsynced tracking gets out of sync with reality.

        Returns:
            Number of unsynced keys found and added to the set.
        """
        target_redis = await self.get_target_redis_client(db)
        if not target_redis:
            logger.error(
                f"Failed to get target Redis client for DB {db} to rebuild unsynced keys set"
            )
            return 0

        logger.info(f"Rebuilding unsynced keys set for target Redis DB {db}...")
        scan_count = 500  # Keys to fetch per SCAN call
        batch_size = 1000  # Max keys to process in a single pipeline
        unsynced_count = 0
        added_to_set = 0

        try:
            # First, clear the existing set (optional - could skip to append instead)
            await handle_redis_error(
                f"deleting existing unsynced keys set in target Redis DB {db}",
                target_redis.delete,
                UNSYNCED_KEYS_SET,
            )

            # Scan all keys in the DB
            cursor = b"0"
            while True:
                scan_result = await handle_redis_error(
                    f"scanning keys in target Redis DB {db} (cursor: {cursor.decode()})",
                    target_redis.scan,
                    cursor=cursor,
                    match="*",
                    count=scan_count,
                )
                if scan_result is None:
                    logger.error(
                        f"SCAN command failed for target Redis DB {db}. Aborting rebuild."
                    )
                    break

                cursor, keys_bytes = scan_result
                if not keys_bytes:
                    if cursor == b"0":
                        break  # Scan finished
                    else:
                        continue  # Empty batch, but more keys exist

                # Filter out sync keys and internal/temporary keys
                potential_content_keys = []
                for k_bytes in keys_bytes:
                    try:
                        k_str = k_bytes.decode("utf-8")
                        if not k_str.startswith(
                            KAFKA_SYNC_PREFIX
                        ) and not k_str.startswith(
                            ("__", "_:", "stream:", UNSYNCED_KEYS_SET)
                        ):
                            potential_content_keys.append(k_bytes)
                    except UnicodeDecodeError:
                        logger.warning(
                            f"Ignoring key in target Redis DB {db} with non-UTF8 bytes: {k_bytes[:50]}..."
                        )

                if not potential_content_keys:
                    if cursor == b"0":
                        break  # Scan finished
                    else:
                        continue  # No content keys in this batch, continue scan

                # Process the found keys in smaller sub-batches
                for i in range(0, len(potential_content_keys), batch_size):
                    batch = potential_content_keys[i : i + batch_size]
                    unsynced_count += len(batch)

                    # Check which keys already have sync markers
                    sync_keys = [
                        f"{KAFKA_SYNC_PREFIX}{k.decode('utf-8')}".encode("utf-8")
                        for k in batch
                    ]
                    sync_values = await handle_redis_error(
                        f"checking batch sync status for {len(sync_keys)} keys in target Redis DB {db}",
                        target_redis.mget,
                        sync_keys,
                    )

                    if sync_values is None:
                        logger.warning(
                            f"MGET check failed during rebuild in target Redis DB {db}. Skipping this batch."
                        )
                        continue

                    # Prepare pipeline to add unsynced keys to the set
                    pipe = target_redis.pipeline(transaction=False)
                    added_in_batch = 0

                    for j, sync_value in enumerate(sync_values):
                        if (
                            sync_value is None
                        ):  # No sync marker exists -> key is unsynced
                            pipe.sadd(UNSYNCED_KEYS_SET, batch[j])
                            added_in_batch += 1

                    if added_in_batch > 0:
                        # Execute the pipeline to add keys to the set
                        results = await handle_redis_error(
                            f"adding {added_in_batch} unsynced keys to set in target Redis DB {db}",
                            pipe.execute,
                        )
                        if results is not None:
                            added_to_set += added_in_batch

                if cursor == b"0":
                    break  # Scan finished

            logger.info(
                f"Rebuilt unsynced keys set for target Redis DB {db}: {added_to_set} unsynced keys added out of {unsynced_count} potential content keys"
            )
            return added_to_set

        except Exception as e:
            logger.error(
                f"Error rebuilding unsynced keys set for target Redis DB {db}: {e}"
            )
            return 0

    # Source Redis stream methods - for reading data from source Redis

    async def get_stream_keys(
        self, pattern: str = "*-stream", db: int = 0
    ) -> List[str]:
        """
        Get stream keys from source Redis matching the given pattern.
        This is used to discover Redis streams for processing.
        """
        source_redis = await self.get_source_redis_client(db)
        if not source_redis:
            logger.error(
                f"Failed to get source Redis client for DB {db} to find streams"
            )
            return []

        logger.debug(
            f"Scanning for streams in source Redis DB {db} with pattern: {pattern}"
        )
        keys = []
        cursor = b"0"

        while True:
            scan_result = await handle_redis_error(
                f"scanning for streams in source Redis DB {db} (cursor: {cursor.decode()})",
                source_redis.scan,
                cursor=cursor,
                match=pattern,
                count=500,
            )

            if scan_result is None:
                logger.error(
                    f"SCAN command failed for source Redis DB {db}. Aborting stream discovery."
                )
                break

            cursor, keys_bytes = scan_result
            if keys_bytes:
                for k_bytes in keys_bytes:
                    try:
                        key_str = k_bytes.decode("utf-8")
                        # Verify it's actually a stream
                        key_type = await handle_redis_error(
                            f"checking type of key {key_str} in source Redis DB {db}",
                            source_redis.type,
                            k_bytes,
                        )

                        if key_type == b"stream":
                            keys.append(key_str)
                    except UnicodeDecodeError:
                        logger.warning(
                            f"Ignoring non-UTF8 key in source Redis DB {db}: {k_bytes[:50]}..."
                        )

            if cursor == b"0":
                break

        logger.debug(f"Found {len(keys)} stream keys in source Redis DB {db}")
        return keys

    async def read_stream_messages(
        self,
        stream: str,
        count: int = 10,
        block: int = 0,
        last_id: str = "0-0",
        db: int = 0,
    ) -> List[Tuple[bytes, Dict[bytes, bytes]]]:
        """
        Read messages from a stream in source Redis.

        Args:
            stream: The stream key to read from
            count: Maximum number of messages to read
            block: Milliseconds to block waiting for messages (0 = indefinitely)
            last_id: ID to start reading from ('0-0' = from beginning, '$' = only new)
            db: Source Redis DB number

        Returns:
            List of (message_id, {field: value}) tuples
        """
        source_redis = await self.get_source_redis_client(db)
        if not source_redis:
            logger.error(
                f"Failed to get source Redis client for DB {db} to read stream {stream}"
            )
            return []

        logger.debug(
            f"Reading from stream {stream} in source Redis DB {db}, last_id: {last_id}"
        )

        try:
            # For XREAD, streams is a dict mapping stream names to last IDs
            streams = {stream: last_id}
            result = await handle_redis_error(
                f"reading from stream {stream} in source Redis DB {db}",
                source_redis.xread,
                streams=streams,
                count=count,
                block=block,
            )

            if result is None:
                return []

            # XREAD returns [[stream_name, [(msg_id, msg_data), ...]], ...]
            messages = []
            if result and result[0] and result[0][1]:
                messages = result[0][1]  # Get just the messages from the first stream

            logger.debug(
                f"Read {len(messages)} messages from stream {stream} in source Redis DB {db}"
            )
            return messages
        except Exception as e:
            logger.error(
                f"Error reading from stream {stream} in source Redis DB {db}: {e}"
            )
            return []

    async def acknowledge_stream_message(
        self, stream: str, message_id: bytes, db: int = 0
    ) -> bool:
        """
        Acknowledge a message in a source Redis stream (XACK command).
        This is used when processing streams with consumer groups.

        Returns True if acknowledged successfully, False otherwise.
        """
        source_redis = await self.get_source_redis_client(db)
        if not source_redis:
            logger.error(
                f"Failed to get source Redis client for DB {db} to acknowledge message in stream {stream}"
            )
            return False

        try:
            # For basic streams without consumer groups, we can use XDEL to remove the message
            result = await handle_redis_error(
                f"deleting message {message_id} from stream {stream} in source Redis DB {db}",
                source_redis.xdel,
                stream,
                message_id,
            )

            success = result is not None and result > 0
            if success:
                logger.debug(
                    f"Successfully removed message {message_id} from stream {stream} in source Redis DB {db}"
                )
            else:
                logger.warning(
                    f"Failed to remove message {message_id} from stream {stream} in source Redis DB {db}"
                )

            return success
        except Exception as e:
            logger.error(
                f"Error acknowledging message in stream {stream} in source Redis DB {db}: {e}"
            )
            return False
