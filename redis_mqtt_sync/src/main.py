#!/usr/bin/env python3
"""
Redis MQTT Synchronization Service
--------------------------------------
Main entry point for Redis to MQTT synchronization.

This module provides the primary entry point for the Redis to MQTT synchronization service.
It handles command-line argument parsing, configuration loading, logging setup,
startup validation, service connectivity checks, and graceful shutdown.

The synchronization service monitors Redis streams and publishes new messages to MQTT topics.
It supports automatic stream discovery, message filtering, and robust connection handling
for both Redis and MQTT.

Usage:
    python main.py                            # Run with default settings
    python main.py --log-level=DEBUG          # Run with debug logging
    python main.py --config-file=/path/to/env.conf  # Use custom config file
"""

import argparse
import asyncio
import logging
import signal
import sys
from types import FrameType
from typing import Callable, List, Optional

from .config import settings
from .core.orchestrator import SyncOrchestrator

# Global logger instance
logger = logging.getLogger("redis-mqtt-sync")

# Global orchestrator instance for signal handling
_orchestrator_instance: Optional[SyncOrchestrator] = None


def setup_logging(log_level: str) -> None:
    """Configure logging based on level."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format=settings.LOG_FORMAT)
    logger.info(f"Logging level set to {log_level.upper()}")


def validate_config() -> bool:
    """Perform basic validation of critical configuration parameters."""
    valid = True
    if settings.REDIS_POOL_SIZE < 1:
        logger.error(
            f"Invalid REDIS_POOL_SIZE: {settings.REDIS_POOL_SIZE}. Must be >= 1."
        )
        valid = False
    if settings.REDIS_MAX_RETRIES < 0:
        logger.error(
            f"Invalid REDIS_MAX_RETRIES: {settings.REDIS_MAX_RETRIES}. Must be >= 0."
        )
        valid = False
    if settings.REDIS_RATE_LIMIT < 1:
        logger.error(
            f"Invalid REDIS_RATE_LIMIT: {settings.REDIS_RATE_LIMIT}. Must be >= 1."
        )
        valid = False

    return valid


async def check_startup_services(orchestrator: SyncOrchestrator) -> bool:
    """Check connectivity to essential services like Redis and MQTT."""
    logger.info("Performing startup service checks...")

    # Check MQTT connection
    mqtt_connected = await orchestrator.mqtt_service.connect()
    if not mqtt_connected:
        logger.critical(
            "Startup Check Failed: Could not connect to MQTT broker after multiple retries."
        )
        return False
    logger.info("Startup Check Passed: MQTT connection successful.")

    # Check Redis connection for at least one DB
    redis_connected = False
    for db_num in settings.REDIS_DBS_TO_SEARCH:
        if await orchestrator.redis_service.connect(db_num):
            redis_connected = True
            logger.info(
                f"Startup Check Passed: Redis connection successful for DB {db_num}."
            )
            break  # Only need one successful connection to proceed
        else:
            logger.warning(f"Startup Check: Could not connect to Redis DB {db_num}.")

    if not redis_connected:
        logger.critical(
            f"Startup Check Failed: Could not connect to any specified Redis DBs {settings.REDIS_DBS_TO_SEARCH}."
        )
        return False

    logger.info("All startup service checks passed.")
    return True


async def handle_shutdown(
    signum: Optional[int] = None, frame: Optional[FrameType] = None
) -> None:
    """Handle shutdown signals gracefully."""
    global _orchestrator_instance
    if _orchestrator_instance:
        logger.info(f"Received signal {signum}. Initiating shutdown...")
        await _orchestrator_instance.stop()
    else:
        logger.info(f"Received signal {signum}. Orchestrator not initialized. Exiting.")
    sys.exit(0)


def create_signal_handler(sig: int) -> Callable[[], None]:
    """Create a properly typed signal handler function for the given signal."""

    def handler() -> None:
        asyncio.create_task(handle_shutdown(sig, None))

    return handler


async def run_sync_mode() -> None:
    """Run the service in synchronization mode."""
    global _orchestrator_instance

    logger.info("Starting Redis to MQTT Sync Service")

    # Initialize orchestrator
    orchestrator = SyncOrchestrator()
    _orchestrator_instance = orchestrator

    # Check if services are reachable
    if not await check_startup_services(orchestrator):
        logger.critical("Startup service checks failed. Exiting.")
        await orchestrator.stop()
        sys.exit(1)

    # Start the orchestrator
    await orchestrator.run()


async def main(args: Optional[List[str]] = None) -> None:
    """Main entry point with argument parsing and setup."""
    parser = argparse.ArgumentParser(
        description="Redis to MQTT Synchronization Service"
    )
    parser.add_argument(
        "--log-level",
        default=settings.LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help=f"Set the logging level (default: {settings.LOG_LEVEL})",
    )
    parser.add_argument(
        "--config-file",
        help="Path to configuration file (default: /app/env.conf)",
    )

    parsed_args = parser.parse_args(args)

    # Load configuration
    if parsed_args.config_file:
        settings.configure(parsed_args.config_file)
    else:
        settings.configure()

    # Setup logging based on arguments/config
    setup_logging(parsed_args.log_level)

    # Validate configuration
    if not validate_config():
        logger.critical("Configuration validation failed. Exiting.")
        sys.exit(1)

    logger.info(f"Searching Redis databases: {settings.REDIS_DBS_TO_SEARCH}")

    # Set up signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, create_signal_handler(sig))

    try:
        await run_sync_mode()
    except Exception as e:
        logger.critical(f"Critical error during main execution: {e}", exc_info=True)
        if _orchestrator_instance:
            await _orchestrator_instance.stop()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
