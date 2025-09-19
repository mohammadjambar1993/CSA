#!/usr/bin/env python3
"""
Redis to Redis Orchestrator
--------------------------
Orchestrates the Redis to Redis synchronization process.
"""

import asyncio
import logging
from typing import List, Optional

# Import specific errors if needed for more granular handling
import redis.asyncio as aioredis
from config import (
    REDIS_STREAMS,
    STREAM_AUTO_DISCOVERY,
    STREAM_DISCOVERY_INTERVAL,
    TARGET_STREAM_PREFIX,
)
from source_redis_manager import SourceRedisManager
from target_redis_manager import TargetRedisManager

logger = logging.getLogger("redis-to-redis-sync")


class RedisToRedisOrchestrator:
    """Orchestrates the Redis to Redis synchronization process."""

    def __init__(self):
        """Initialize the orchestrator."""
        self.source_manager = SourceRedisManager()
        self.target_manager = TargetRedisManager()
        self.is_running = False
        self._current_streams = set(REDIS_STREAMS)  # Use a set for faster lookups
        self._discovery_task: Optional[asyncio.Task] = None
        self._processing_tasks: List[asyncio.Task] = []

    async def _discover_streams_periodically(self):
        """Task to periodically discover new streams."""
        logger.info("Starting periodic stream discovery task...")
        while self.is_running:
            try:
                discovered_streams_info = await self.source_manager.discover_streams()
                newly_discovered = False
                for prefixed_key, db_num, original_key in discovered_streams_info:
                    if prefixed_key not in self._current_streams:
                        self._current_streams.add(prefixed_key)
                        logger.info(
                            f"Discovered new stream: {original_key} in DB {db_num} (prefixed: {prefixed_key}). Added to processing list."
                        )
                        # Ensure consumer group exists before processing starts
                        await self.source_manager._create_consumer_group(
                            db_num, original_key
                        )
                        newly_discovered = True

                if newly_discovered:
                    logger.info(
                        f"Current streams being processed: {sorted(list(self._current_streams))}"
                    )

            except asyncio.CancelledError:
                logger.info("Stream discovery task cancelled.")
                break
            except Exception as e:
                logger.error(
                    f"Error during periodic stream discovery: {e}", exc_info=True
                )

            try:
                await asyncio.sleep(STREAM_DISCOVERY_INTERVAL)
            except asyncio.CancelledError:
                logger.info("Stream discovery sleep cancelled.")
                break
        logger.info("Stopped periodic stream discovery task.")

    async def _process_single_stream(self, stream_name: str):
        """Process a single Redis stream. Runs continuously until service stops."""
        logger.info(f"Starting processing task for stream: {stream_name}")
        while self.is_running:
            try:
                # Read messages from source - now returns more info
                read_result = await self.source_manager.read_stream_messages(
                    stream_name
                )

                if read_result:
                    prefixed_stream_name, db_num, original_key, messages = read_result
                    if not messages:
                        # No messages read, short sleep before next attempt
                        await asyncio.sleep(0.1)
                        continue

                    # Determine target stream name
                    target_key = (
                        f"{TARGET_STREAM_PREFIX}{original_key}"
                        if TARGET_STREAM_PREFIX
                        else original_key
                    )

                    # Write messages to target
                    written_ids = await self.target_manager.write_messages(
                        target_key, messages
                    )

                    # Acknowledge messages in source ONLY if write succeeded
                    if written_ids:  # Check if list is not empty
                        # Use the *prefixed* name which was returned by read_stream_messages
                        await self.source_manager.ack_messages(
                            prefixed_stream_name, written_ids
                        )
                    else:
                        logger.warning(
                            f"Write to target for {target_key} failed or returned no IDs. Source messages for {prefixed_stream_name} will not be ACKed."
                        )
                        # Optional: Add delay here if writes are failing frequently
                        await asyncio.sleep(1)
                else:
                    # No messages or error during read (error already logged by manager)
                    # Sleep briefly before trying again
                    await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                logger.info(f"Processing task for stream {stream_name} cancelled.")
                break  # Exit loop cleanly on cancellation
            except Exception as e:
                # Log unexpected errors in the stream processing loop
                logger.error(
                    f"Unexpected error processing stream {stream_name}: {e}",
                    exc_info=True,
                )
                # Wait before retrying after an unexpected error
                await asyncio.sleep(5)
        logger.info(f"Stopped processing task for stream: {stream_name}")

    async def run(self):
        """Main execution loop."""
        if self.is_running:
            logger.warning("Orchestrator already running")
            return

        self.is_running = True
        logger.info("Starting Redis to Redis synchronization orchestrator")

        try:
            # Ensure target connection is established before starting
            if not await self.target_manager.connect():
                logger.critical(
                    "Failed to connect to target Redis. Orchestrator cannot start."
                )
                self.is_running = False
                return

            # Setup groups for initially configured streams
            await self.source_manager.setup_initial_consumer_groups(
                list(self._current_streams)
            )

            # Start discovery task if enabled
            if STREAM_AUTO_DISCOVERY:
                self._discovery_task = asyncio.create_task(
                    self._discover_streams_periodically()
                )

            # Main processing loop manager
            last_processed_streams = set()
            while self.is_running:
                # Identify streams that are new or stopped
                current_stream_set = self._current_streams.copy()
                new_streams = current_stream_set - last_processed_streams
                stopped_streams = (
                    last_processed_streams - current_stream_set
                )  # Should be rare unless config changes live

                # Start tasks for new streams
                for stream_name in new_streams:
                    logger.info(
                        f"Starting new processing task for stream: {stream_name}"
                    )
                    task = asyncio.create_task(self._process_single_stream(stream_name))
                    self._processing_tasks.append(task)

                # Handle stopped streams (if any logic needed - currently just stop tracking)
                # In a more complex scenario, might cancel corresponding tasks
                if stopped_streams:
                    logger.warning(
                        f"Streams no longer tracked (manual removal?): {stopped_streams}"
                    )

                last_processed_streams = current_stream_set

                if not self._processing_tasks:
                    logger.debug("No active streams to process. Waiting...")

                # Sleep briefly to prevent tight loop when idle and allow discovery/signals
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("Orchestrator run task cancelled.")
        except Exception as e:
            # Catch unexpected errors in the main management loop
            logger.critical(
                f"Critical unexpected error in orchestrator run loop: {e}",
                exc_info=True,
            )
        finally:
            logger.info("Orchestrator run loop exiting. Initiating stop sequence.")
            await self.stop()

    async def stop(self):
        """Stop the service gracefully."""
        if (
            not self.is_running
            and not self._processing_tasks
            and not self._discovery_task
        ):
            logger.info("Orchestrator stop called, but already stopped/inactive.")
            return

        logger.info("Stopping Redis to Redis synchronization orchestrator...")
        self.is_running = False  # Signal loops to stop

        # Cancel discovery task
        if self._discovery_task and not self._discovery_task.done():
            self._discovery_task.cancel()
            try:
                await self._discovery_task
            except asyncio.CancelledError:
                logger.info("Discovery task successfully cancelled.")
            except Exception as e:
                logger.error(
                    f"Error during discovery task cancellation: {e}", exc_info=True
                )
        self._discovery_task = None

        # Cancel running stream processing tasks
        if self._processing_tasks:
            logger.info(
                f"Cancelling {len(self._processing_tasks)} stream processing tasks..."
            )
            for task in self._processing_tasks:
                if not task.done():
                    task.cancel()
            # Wait for tasks to finish cancelling
            results = await asyncio.gather(
                *self._processing_tasks, return_exceptions=True
            )
            for i, result in enumerate(results):
                if isinstance(result, asyncio.CancelledError):
                    logger.debug(f"Processing task {i} cancelled successfully.")
                elif isinstance(result, Exception):
                    logger.error(
                        f"Error during processing task {i} cancellation/cleanup: {result}",
                        exc_info=isinstance(result, Exception),
                    )
            self._processing_tasks = []

        # Close connections
        logger.info("Closing Redis connections...")
        await self.source_manager.close()
        await self.target_manager.close()

        logger.info("Redis to Redis synchronization orchestrator stopped.")
