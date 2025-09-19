#!/usr/bin/env python3
"""
Sync Orchestrator
---------------
Orchestrates the Redis to PostgreSQL synchronization process.
"""

import asyncio
import fnmatch
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from ..config import settings
from ..services.interfaces import PostgresServiceInterface, RedisServiceInterface
from ..services.postgres_service import PostgresService
from ..services.redis_service import RedisService
from ..utils.metrics import Metrics
from ..utils.rate_limiter import RateLimiter
from .types import MessageResult

logger = logging.getLogger("redis-pg-sync")


class SyncOrchestrator:
    """
    Orchestrates the Redis to PostgreSQL synchronization process.

    This class coordinates the synchronization of data between Redis and
    PostgreSQL, implementing rate limiting, exponential backoff, and
    comprehensive metrics collection.

    Attributes:
        redis_service: Service for Redis operations
        postgres_service: Service for PostgreSQL operations
        is_running: Indicates if the service is running
        streams_to_process: List of streams to process
        processed_keys: Set of processed key-value pairs
        metrics: Metrics collection instance
        rate_limiter: Rate limiter instance
        mode: Operation mode ("sync" or "monitor")
    """

    def __init__(
        self,
        redis_service: Optional[RedisServiceInterface] = None,
        postgres_service: Optional[PostgresServiceInterface] = None,
        mode: str = "sync",
    ):
        """
        Initialize the orchestrator.

        Args:
            redis_service: Service for Redis operations. If None, creates a new instance.
            postgres_service: Service for PostgreSQL operations. If None, creates a new instance.
            mode: Operation mode ("sync" or "monitor")
        """
        self.redis_service = redis_service or RedisService()
        self.postgres_service = postgres_service or PostgresService()
        self.is_running = False
        self.streams_to_process = list(settings.REDIS_STREAMS)
        self.processed_keys: Set[str] = set()
        self.metrics = Metrics()
        self.rate_limiter = RateLimiter(settings.REDIS_RATE_LIMIT)
        self.mode = mode
        self._validate_config()

    def _validate_config(self):
        """
        Validate configuration parameters.

        Checks the configuration parameters for validity and issues
        warnings or raises errors as appropriate.

        Raises:
            ValueError: If critical configuration parameters are invalid
        """
        if not settings.REDIS_STREAMS and not settings.STREAM_AUTO_DISCOVERY:
            logger.warning("No streams configured and auto-discovery is disabled")

        if not settings.KEY_VALUE_PATTERNS and settings.KEY_VALUE_AUTO_DISCOVERY:
            logger.warning(
                "Key-value auto-discovery enabled but no patterns configured"
            )

        if settings.SYNC_INTERVAL < 0.1:
            logger.warning("Sync interval is very small, may impact performance")

        if settings.REDIS_RATE_LIMIT < 1:
            raise ValueError("Redis rate limit must be at least 1")

    async def _with_backoff(self, func, *args, **kwargs):
        """
        Execute function with exponential backoff.

        This method implements exponential backoff for retrying failed
        operations. It uses the rate limiter to control operation rate
        and tracks metrics for operations and retries.

        Args:
            func: The function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The result of the function execution
        """
        # Apply rate limiting
        await self.rate_limiter.acquire()
        self.metrics.increment_redis_ops()

        retry_count = 0
        while retry_count < settings.REDIS_MAX_RETRIES:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Increment retries metric
                self.metrics.increment_retries()
                retry_count += 1

                # Log error
                if retry_count < settings.REDIS_MAX_RETRIES:
                    backoff = min(
                        settings.REDIS_BACKOFF_BASE * (2 ** (retry_count - 1)),
                        settings.REDIS_BACKOFF_MAX,
                    )
                    logger.warning(
                        f"Operation failed (attempt {retry_count}/{settings.REDIS_MAX_RETRIES}): {e}. "
                        f"Retrying in {backoff:.2f} seconds..."
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.error(
                        f"Operation failed after {settings.REDIS_MAX_RETRIES} attempts: {e}"
                    )
                    self.metrics.increment_errors()
                    return None
        return None

    async def process_streams(self):
        """
        Process streams from Redis to PostgreSQL.

        This method discovers streams if auto-discovery is enabled,
        sets up consumer groups, and processes messages from each stream.
        """
        logger.debug("Processing streams...")

        # Discover streams if auto-discovery is enabled
        if settings.STREAM_AUTO_DISCOVERY:
            discovered_streams = await self.redis_service.discover_streams()
            for display_name, db_num, stream_key in discovered_streams:
                if display_name not in self.streams_to_process:
                    logger.info(f"Discovered new stream: {display_name}")
                    self.streams_to_process.append(display_name)
                    self.metrics.stream_discovery_count += 1

        # Set up consumer groups
        await self.redis_service.setup_consumer_groups(self.streams_to_process)

        # Process each stream
        for stream_name in self.streams_to_process:
            await self._process_single_stream(stream_name)

    async def _process_single_stream(self, stream_name: str):
        """
        Process a single stream from Redis to PostgreSQL.

        This method reads messages from a stream, inserts them into
        PostgreSQL, and acknowledges the messages in Redis.

        Args:
            stream_name: Name of the stream to process
        """
        # Read messages from the stream
        result: MessageResult = await self._with_backoff(
            self.redis_service.read_stream_messages, stream_name
        )

        if not result:
            return

        logger.info(f"Processing stream: {stream_name}, messages: {result}")
        stream_key = result["message_id"]
        messages = result["messages"]

        if not messages:
            return
        # Insert messages into PostgreSQL
        message_tuples = [
            (stream_key, msg_id, msg_data) for msg_id, msg_data in messages
        ]
        success = await self.postgres_service.insert_stream_messages(message_tuples)

        if success:
            # Acknowledge messages in Redis
            message_ids = [msg_id for msg_id, _ in messages]
            await self._with_backoff(
                self.redis_service.ack_messages, stream_key, message_ids
            )
            self.metrics.messages_processed += len(messages)
            self.metrics.update_sync_time()
            logger.info(f"Processed {len(messages)} messages from stream {stream_name}")

    async def process_key_values(self):
        """
        Process key-value pairs from Redis to PostgreSQL.

        This method scans keys in Redis, reads their values, and
        upserts them into PostgreSQL.
        """
        if not settings.KEY_VALUE_AUTO_DISCOVERY:
            logger.debug("Key-value auto-discovery is disabled")
            return

        # Get key patterns to scan for
        patterns = settings.KEY_VALUE_PATTERNS
        if not patterns:
            logger.warning("No key patterns configured for auto-discovery")
            return

        # Process each database
        for db_num in settings.REDIS_DBS_TO_SEARCH:
            # Scan keys matching patterns
            all_matching_keys = []
            for pattern in patterns:
                keys = await self._with_backoff(
                    self.redis_service.scan_keys, db_num, pattern
                )
                if keys:
                    all_matching_keys.extend(keys)

            if not all_matching_keys:
                continue

            # Read and upsert key-value pairs
            key_values = []
            for key in all_matching_keys:
                # Skip already processed keys
                cache_key = f"db{db_num}:{key}"
                if cache_key in self.processed_keys:
                    continue

                # Read key-value pair
                result = await self._with_backoff(
                    self.redis_service.read_key_value, key, db_num
                )
                if result:
                    key, value = result
                    # Convert to the expected type: List[Tuple[str, Any]]
                    key_values.append((str(key), value))
                    self.processed_keys.add(cache_key)

            # Upsert key-value pairs into PostgreSQL
            if key_values:
                success = await self.postgres_service.upsert_key_values(key_values)
                if success:
                    self.metrics.key_values_processed += len(key_values)
                    self.metrics.update_sync_time()
                    logger.info(
                        f"Processed {len(key_values)} key-value pairs from DB {db_num}"
                    )

    async def run(self):
        """
        Run the orchestrator in the configured mode.

        This method enters a loop that processes streams and/or key-value
        pairs based on the configured mode. The loop continues until
        is_running is set to False.
        """
        logger.info(f"Starting Redis to PostgreSQL orchestrator in {self.mode} mode")

        # Setup PostgreSQL schema
        if not await self.postgres_service.setup_schema():
            logger.error("Failed to setup PostgreSQL schema. Exiting.")
            return

        # Set running flag
        self.is_running = True

        try:
            while self.is_running:
                # Process streams in both modes
                if settings.REDIS_STREAMS_ENABLED:
                    await self.process_streams()

                # Process key-value pairs only in sync mode
                if self.mode == "sync" and settings.KEY_VALUE_AUTO_DISCOVERY:
                    await self.process_key_values()

                # Wait for next sync cycle
                if self.is_running:
                    interval = (
                        settings.SYNC_INTERVAL
                        if self.mode == "sync"
                        else settings.MONITOR_INTERVAL
                    )
                    await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Orchestrator task cancelled")
            self.is_running = False
        except Exception as e:
            logger.error(f"Unexpected error in orchestrator: {e}", exc_info=True)
            self.is_running = False
            raise
        finally:
            # Final log of metrics
            metrics_dict = self.metrics.as_dict()
            logger.info(f"Final metrics: {metrics_dict}")
            await self.stop()

    async def stop(self):
        """
        Stop the orchestrator and clean up resources.

        This method sets is_running to False, closes Redis and PostgreSQL
        connections, and performs any necessary cleanup.
        """
        logger.info("Stopping orchestrator...")
        self.is_running = False

        # Close connections
        if hasattr(self, "redis_service"):
            await self.redis_service.close()
        if hasattr(self, "postgres_service"):
            await self.postgres_service.close()

        logger.info("Orchestrator stopped")
