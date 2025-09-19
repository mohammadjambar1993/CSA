#!/usr/bin/env python3
"""
Redis to Redis Sync
------------------
Main module for Redis to Redis synchronization.

Parses arguments, validates configuration, checks service reachability,
and runs the main synchronization orchestrator.
"""

import argparse  # Import argparse
import asyncio
import logging
import signal
import sys
from typing import List, Optional

# Import config first
import config
from orchestrator import RedisToRedisOrchestrator

# Global logger instance
logger = logging.getLogger("redis-to-redis-sync")

# Global orchestrator instance for signal handling
_orchestrator_instance: Optional[RedisToRedisOrchestrator] = None


def setup_logging(log_level: str):
    """Configure logging based on level."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format=config.LOG_FORMAT)
    logger.info(f"Logging level set to {log_level.upper()}")


def validate_config() -> bool:
    """Perform basic validation of critical configuration parameters."""
    valid = True
    if config.SOURCE_REDIS_POOL_SIZE < 1:
        logger.error(
            f"Invalid SOURCE_REDIS_POOL_SIZE: {config.SOURCE_REDIS_POOL_SIZE}. Must be >= 1."
        )
        valid = False
    if config.TARGET_REDIS_POOL_SIZE < 1:
        logger.error(
            f"Invalid TARGET_REDIS_POOL_SIZE: {config.TARGET_REDIS_POOL_SIZE}. Must be >= 1."
        )
        valid = False
    if config.SOURCE_REDIS_MAX_RETRIES < 0:
        logger.error(
            f"Invalid SOURCE_REDIS_MAX_RETRIES: {config.SOURCE_REDIS_MAX_RETRIES}. Must be >= 0."
        )
        valid = False
    if config.TARGET_REDIS_MAX_RETRIES < 0:
        logger.error(
            f"Invalid TARGET_REDIS_MAX_RETRIES: {config.TARGET_REDIS_MAX_RETRIES}. Must be >= 0."
        )
        valid = False
    if not config.REDIS_DBS_TO_SEARCH:
        logger.error(
            f"Redis databases to search cannot be empty. Check REDIS_DB_SEARCH_RANGE."
        )
        valid = False
    # Add more validation checks as needed (e.g., non-empty consumer group/name)
    return valid


async def check_startup_services(orchestrator: RedisToRedisOrchestrator) -> bool:
    """Check connectivity to essential services (Source & Target Redis)."""
    logger.info("Performing startup service checks...")

    # Check Target Redis connection
    target_connected = await orchestrator.target_manager.connect()
    if not target_connected:
        logger.critical(
            "Startup Check Failed: Could not connect to Target Redis after multiple retries."
        )
        return False
    logger.info("Startup Check Passed: Target Redis connection successful.")

    # Check Source Redis connection for at least one DB
    source_connected = False
    for db_num in config.REDIS_DBS_TO_SEARCH:
        # Use the robust connection method from the manager
        redis_client = await orchestrator.source_manager._get_source_redis_pool(db_num)
        if redis_client:
            source_connected = True
            logger.info(
                f"Startup Check Passed: Source Redis connection successful for DB {db_num}."
            )
            # We don't break here, allow checking all configured source DBs if desired,
            # but log success as soon as one works.
            # If strict checking of *all* source DBs is needed, modify logic here.
        else:
            # _get_source_redis_pool already logs errors after retries
            logger.warning(
                f"Startup Check: Failed to connect to Source Redis DB {db_num} after retries."
            )

    if not source_connected:
        logger.critical(
            f"Startup Check Failed: Could not connect to ANY specified Source Redis DBs {config.REDIS_DBS_TO_SEARCH}."
        )
        return False

    logger.info("All essential startup service checks passed.")
    return True


async def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully."""
    global _orchestrator_instance
    if _orchestrator_instance:
        logger.info(f"Received signal {signum}. Initiating shutdown...")
        # Use asyncio.create_task to avoid blocking the signal handler
        await asyncio.create_task(_orchestrator_instance.stop())
    else:
        logger.info(f"Received signal {signum}. Orchestrator not initialized. Exiting.")
        sys.exit(0)


async def main(args: Optional[List[str]] = None):
    """Main entry point with argument parsing and setup."""
    global _orchestrator_instance

    parser = argparse.ArgumentParser(
        description="Redis to Redis Synchronization Service"
    )
    parser.add_argument(
        "--log-level",
        default=config.LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help=f"Set the logging level (default: {config.LOG_LEVEL})",
    )
    # Add other arguments like --config-file if needed in the future

    parsed_args = parser.parse_args(args)

    # Setup logging first
    setup_logging(parsed_args.log_level)

    # Validate configuration from config.py (already loaded)
    if not validate_config():
        logger.critical("Configuration validation failed. Exiting.")
        sys.exit(1)

    logger.info(f"Source Redis databases to search: {config.REDIS_DBS_TO_SEARCH}")

    # Create orchestrator
    orchestrator = RedisToRedisOrchestrator()
    _orchestrator_instance = orchestrator  # Assign to global for signal handler

    # Perform startup checks
    if not await check_startup_services(orchestrator):
        logger.critical("Startup service checks failed. Exiting.")
        # Attempt to clean up connections even if startup failed
        await orchestrator.stop()
        sys.exit(1)

    # Set up signal handlers using the running event loop
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Add handler without creating task directly in lambda
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(handle_shutdown(s, None))
        )

    try:
        # Run the main orchestrator loop
        await orchestrator.run()
    except Exception as e:
        logger.critical(f"Critical error during main execution: {e}", exc_info=True)
        # Ensure cleanup happens on unexpected main errors
        await orchestrator.stop()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
