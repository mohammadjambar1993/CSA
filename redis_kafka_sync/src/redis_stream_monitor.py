#!/usr/bin/env python3
"""
Redis Stream Monitor
-------------------
This module monitors Redis streams and provides real-time information about stream health,
length, consumer groups, and pending messages. It can be used as a standalone service
or integrated into other applications.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import redis
from redis.exceptions import ResponseError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("redis-stream-monitor")

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis-cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

# Monitor Configuration
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", 5))
REDIS_STREAMS = os.getenv("REDIS_STREAMS", "biosignal-stream").split(",")


class RedisStreamMonitor:
    """Monitors Redis streams and provides statistics and health information."""

    def __init__(self):
        """Initialize the monitor."""
        self.redis_client = self._init_redis()
        self.streams = REDIS_STREAMS
        self.interval = MONITOR_INTERVAL
        self.running = False
        self.stream_stats = {}
        self.stream_health = {}

    def _init_redis(self) -> Optional[redis.Redis]:
        """Initialize the Redis client."""
        try:
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD if REDIS_PASSWORD else None,
                socket_timeout=5,
                decode_responses=True,  # Use text for keys and values
            )
            client.ping()  # Test connection
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return client
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return None

    async def monitor_streams(self):
        """Start monitoring all streams."""
        self.running = True
        logger.info(f"Starting stream monitor for: {', '.join(self.streams)}")

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop)

        while self.running:
            try:
                for stream in self.streams:
                    await self.check_stream(stream)

                # Log summary info
                self._log_summary()

                # Wait for next check
                await asyncio.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error during stream monitoring: {e}")
                await asyncio.sleep(self.interval)

    async def check_stream(self, stream_name: str):
        """Check a specific stream and update statistics."""
        try:
            # Get stream information
            stream_info = self.redis_client.xinfo_stream(stream_name)

            # Get consumer group information
            groups_info = []
            try:
                groups_info = self.redis_client.xinfo_groups(stream_name)
            except ResponseError as e:
                if "no such key" in str(e).lower():
                    logger.warning(f"Stream {stream_name} does not exist yet")
                elif "Consumer Group not found" in str(e):
                    logger.info(f"No consumer groups defined for stream {stream_name}")
                else:
                    logger.error(
                        f"Error getting consumer groups for {stream_name}: {e}"
                    )

            # Process stream stats
            self.stream_stats[stream_name] = {
                "length": stream_info.get("length", 0),
                "first_entry_id": stream_info.get("first-entry", {}).get("id", ""),
                "last_entry_id": stream_info.get("last-entry", {}).get("id", ""),
                "groups": len(groups_info),
                "last_checked": datetime.now().isoformat(),
                "consumer_groups": {},
            }

            # Process consumer group stats
            for group in groups_info:
                group_name = group.get("name")
                pending = group.get("pending", 0)
                consumers = group.get("consumers", 0)

                # Get consumer information
                consumer_info = []
                try:
                    if group_name:
                        consumer_info = self.redis_client.xinfo_consumers(
                            stream_name, group_name
                        )
                except ResponseError as e:
                    logger.error(
                        f"Error getting consumer info for {stream_name}/{group_name}: {e}"
                    )

                self.stream_stats[stream_name]["consumer_groups"][group_name] = {
                    "pending": pending,
                    "consumers": consumers,
                    "last_delivered": group.get("last-delivered-id", ""),
                    "consumer_details": consumer_info,
                }

            # Check health
            self._check_health(stream_name)

        except ResponseError as e:
            if "no such key" in str(e).lower():
                logger.warning(f"Stream {stream_name} does not exist yet")
                # Create an empty health record
                self.stream_health[stream_name] = {
                    "status": "NOT_FOUND",
                    "issues": ["Stream does not exist"],
                }
            else:
                logger.error(f"Redis error checking stream {stream_name}: {e}")
        except Exception as e:
            logger.error(f"Error checking stream {stream_name}: {e}")

    def _check_health(self, stream_name: str):
        """Check the health of a stream based on various metrics."""
        issues = []
        stats = self.stream_stats.get(stream_name, {})

        # Check if stream exists but is empty
        if stats.get("length", 0) == 0:
            issues.append("Stream is empty")

        # Check for consumer groups without consumers
        for group_name, group_stats in stats.get("consumer_groups", {}).items():
            if group_stats.get("consumers", 0) == 0:
                issues.append(f"Consumer group '{group_name}' has no consumers")

            # Check for pending messages
            if (
                group_stats.get("pending", 0) > 100
            ):  # Arbitrary threshold, adjust as needed
                issues.append(
                    f"Consumer group '{group_name}' has high pending count: {group_stats.get('pending')}"
                )

        # Determine overall status
        if not issues:
            status = "HEALTHY"
        elif any("does not exist" in issue for issue in issues):
            status = "NOT_FOUND"
        elif any("no consumers" in issue for issue in issues):
            status = "WARNING"
        elif any("high pending" in issue for issue in issues):
            status = "DEGRADED"
        else:
            status = "WARNING"

        self.stream_health[stream_name] = {
            "status": status,
            "issues": issues,
            "last_checked": datetime.now().isoformat(),
        }

    def _log_summary(self):
        """Log a summary of all stream health."""
        logger.info("===== REDIS STREAM HEALTH SUMMARY =====")

        for stream_name, health in self.stream_health.items():
            stats = self.stream_stats.get(stream_name, {})
            status = health.get("status", "UNKNOWN")
            issues = health.get("issues", [])

            # Format the status with color (for terminal output)
            status_display = status
            if status == "HEALTHY":
                status_display = f"\033[92m{status}\033[0m"  # Green
            elif status == "WARNING":
                status_display = f"\033[93m{status}\033[0m"  # Yellow
            elif status == "DEGRADED" or status == "NOT_FOUND":
                status_display = f"\033[91m{status}\033[0m"  # Red

            logger.info(f"Stream: {stream_name} | Status: {status_display}")
            logger.info(
                f"  Length: {stats.get('length', 0)} | Consumer Groups: {stats.get('groups', 0)}"
            )

            if issues:
                logger.info(f"  Issues: {', '.join(issues)}")

            # Log consumer group details
            for group_name, group_stats in stats.get("consumer_groups", {}).items():
                pending = group_stats.get("pending", 0)
                consumers = group_stats.get("consumers", 0)
                logger.info(
                    f"  Group: {group_name} | Consumers: {consumers} | Pending: {pending}"
                )

        logger.info("========================================")

    def stop(self):
        """Stop the monitoring loop."""
        logger.info("Stopping Redis stream monitor...")
        self.running = False

    def get_stream_stats(self, stream_name=None):
        """Get statistics for all streams or a specific stream."""
        if stream_name:
            return self.stream_stats.get(stream_name, {})
        return self.stream_stats

    def get_stream_health(self, stream_name=None):
        """Get health information for all streams or a specific stream."""
        if stream_name:
            return self.stream_health.get(stream_name, {})
        return self.stream_health


async def main():
    """Main entry point for the script."""
    monitor = RedisStreamMonitor()

    try:
        # Monitor streams
        await monitor.monitor_streams()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
    finally:
        monitor.stop()
        logger.info("Stream monitor stopped")


if __name__ == "__main__":
    asyncio.run(main())
