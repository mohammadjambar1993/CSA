#!/usr/bin/env python3
"""
PostgreSQL Service
-----------------
Handles PostgreSQL connections, schema setup, and data operations.
"""

import asyncio
import datetime
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import asyncpg  # type: ignore[import-untyped]
from asyncpg import Record

from ..config import settings
from ..core.exceptions import PostgresConnectionError, PostgresError
from ..core.types import ConnectionResult, OperationResult
from .interfaces import PostgresServiceInterface

logger = logging.getLogger("redis-pg-sync")

# Controls how stream message conflicts are handled:
# - If True: overwrite existing records with the same key/ID/date
# - If False (default): skip inserting duplicates
STREAM_CONFLICT_OVERWRITE = getattr(settings, "STREAM_CONFLICT_OVERWRITE", False)


class PostgresService(PostgresServiceInterface):
    """Manages PostgreSQL connections and operations."""

    def __init__(self):
        """Initialize the PostgreSQL service."""
        self.pool: Optional[asyncpg.Pool] = None
        self._lock = asyncio.Lock()
        self._connection_attempts = 0
        self._last_health_check: float = 0
        self._health_check_interval = settings.PG_HEALTH_CHECK_INTERVAL

    async def connect(self, db: Optional[str] = None) -> ConnectionResult:
        """
        Establish connection to PostgreSQL with retry logic and health checks.

        Args:
            db: Optional database name to connect to

        Returns:
            ConnectionResult indicating success or failure
        """
        async with self._lock:
            # Check if already connected and healthy
            current_time = time.time()
            if (
                self.pool is not None
                and current_time - self._last_health_check < self._health_check_interval
            ):
                if await self._check_connection_health():
                    return {"success": True, "error": None, "data": None}
                else:
                    logger.warning(
                        "Unhealthy PostgreSQL connection detected, attempting to reconnect"
                    )
                    await self.close()  # Closes pool if it exists

            # Attempt to connect with exponential backoff
            self._connection_attempts = 0
            while self._connection_attempts < settings.PG_MAX_RETRIES:
                try:
                    # Calculate backoff delay
                    delay = min(
                        settings.PG_BACKOFF_BASE * (2**self._connection_attempts),
                        settings.PG_BACKOFF_MAX,
                    )

                    # Set up type codecs for the connection pool
                    async def init_connection(conn):
                        # Set search path
                        await conn.execute(f"SET search_path TO {settings.PG_SCHEMA}")

                    self.pool = await asyncpg.create_pool(
                        host=settings.PG_HOST,
                        port=settings.PG_PORT,
                        user=settings.PG_USER,
                        password=settings.PG_PASSWORD,
                        database=settings.PG_DATABASE,
                        min_size=1,
                        max_size=settings.PG_POOL_SIZE,
                        max_queries=settings.PG_MAX_QUERIES,
                        command_timeout=settings.PG_QUERY_TIMEOUT,
                        server_settings={"search_path": settings.PG_SCHEMA},
                        init=init_connection,
                    )

                    # Check health after connection
                    if await self._check_connection_health():
                        logger.info("Created PostgreSQL connection pool")
                        self._last_health_check = time.time()
                        self._connection_attempts = 0  # Reset on success
                        return {"success": True, "error": None, "data": None}
                    else:
                        logger.error("PostgreSQL connection established but unhealthy.")
                        await self.close()
                        return {
                            "success": False,
                            "error": "PostgreSQL connection established but unhealthy",
                            "data": None,
                        }

                except Exception as e:
                    logger.error(
                        f"Connection attempt {self._connection_attempts + 1} failed: {e}"
                    )

                self._connection_attempts += 1
                if self._connection_attempts < settings.PG_MAX_RETRIES:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Max connection attempts reached for PostgreSQL")
                    self.pool = None  # Ensure pool is None if all attempts fail
                    return {
                        "success": False,
                        "error": f"Max connection attempts reached for PostgreSQL",
                        "data": None,
                    }

            return {
                "success": False,
                "error": "Failed to establish PostgreSQL connection",
                "data": None,
            }

    async def _check_connection_health(self) -> bool:
        """
        Perform a health check on the PostgreSQL connection pool.

        Returns:
            True if healthy, False otherwise
        """
        if not self.pool:
            return False
        assert self.pool is not None  # Help type checker recognize pool is not None
        try:
            async with self.pool.acquire() as conn:
                # Check if we can run a simple query
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False

    async def setup_schema(self) -> OperationResult:
        """
        Set up required database schema and indexes.

        Returns:
            OperationResult indicating success or failure
        """
        if not await self.connect():  # Ensure connection is active
            logger.error("Cannot setup schema: PostgreSQL connection failed")
            return {
                "success": False,
                "error": "PostgreSQL connection failed",
                "data": None,
            }

        try:
            assert self.pool is not None  # Help type checker
            async with self.pool.acquire() as conn:
                # First, check if we have the necessary permissions
                try:
                    # Check if the schema already exists
                    schema_exists = await conn.fetchval(
                        f"""
                        SELECT EXISTS(
                            SELECT 1 FROM information_schema.schemata 
                            WHERE schema_name = $1
                        )
                        """,
                        settings.PG_SCHEMA,
                    )

                    if schema_exists:
                        logger.info(
                            f"Schema '{settings.PG_SCHEMA}' already exists, skipping creation"
                        )
                    else:
                        # Only try to create schema if it doesn't exist
                        logger.info(
                            f"Schema '{settings.PG_SCHEMA}' does not exist, will attempt to create it"
                        )
                        try:
                            await conn.execute(f"CREATE SCHEMA {settings.PG_SCHEMA}")
                            logger.info(
                                f"Successfully created schema '{settings.PG_SCHEMA}'"
                            )
                        except Exception as schema_error:
                            error_msg = str(schema_error)
                            logger.error(
                                f"Failed to create schema '{settings.PG_SCHEMA}': {error_msg}"
                            )
                            logger.error(
                                "The user lacks schema creation permissions, checking if tables exist anyway"
                            )
                except Exception as perm_error:
                    # Permission error - log detailed information for the administrator
                    error_msg = str(perm_error)
                    logger.error(f"Cannot check schema existence: {error_msg}")
                    logger.error(
                        f"The user '{settings.PG_USER}' may have insufficient permissions on database '{settings.PG_DATABASE}'"
                    )
                    logger.error(
                        "Please check the documentation for required PostgreSQL permissions"
                    )

                    # Check if basic queries work - if yes, continue with limited functionality
                    try:
                        await conn.fetchval("SELECT 1")
                        logger.warning(
                            "Basic PostgreSQL connectivity works, but schema operations failed. Running with limited functionality."
                        )
                        # Continue execution to try using existing tables
                    except Exception:
                        # Cannot even run basic queries
                        logger.error(
                            "Cannot perform even basic PostgreSQL operations. Service will not function correctly."
                        )
                        return {
                            "success": False,
                            "error": "Cannot perform basic PostgreSQL operations",
                            "data": None,
                        }

                # Attempt to create tables but continue even if it fails
                try:
                    # First check if tables already exist
                    stream_table_exists = await conn.fetchval(
                        f"""
                        SELECT EXISTS(
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = $1 AND table_name = $2
                        )
                        """,
                        settings.PG_SCHEMA,
                        settings.STREAM_TABLE_NAME,
                    )

                    if stream_table_exists:
                        logger.info(
                            f"Table '{settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}' already exists"
                        )
                    else:
                        # Create the stream table with partitioning
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} (
                                id SERIAL,
                                stream_key TEXT NOT NULL,
                                message_id TEXT NOT NULL,
                                data JSONB NOT NULL,
                                unix_timestamp TIMESTAMP NOT NULL,
                                streaming_date DATE NOT NULL,
                                UNIQUE(stream_key, message_id, streaming_date)
                            ) PARTITION BY RANGE (streaming_date)
                        """
                        )

                        # Create default partition for any rows that don't match existing partitions
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}_default PARTITION OF {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}
                            DEFAULT
                        """
                        )

                        # Create partition for today
                        today = datetime.date.today()
                        tomorrow = today + datetime.timedelta(days=1)
                        today_str = today.strftime("%Y_%m_%d")

                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}_{today_str} PARTITION OF {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}
                            FOR VALUES FROM ('{today}') TO ('{tomorrow}')
                        """
                        )

                        logger.info(
                            f"Successfully created stream table '{settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}'"
                        )

                    # Check if key-value table exists
                    keyvalue_table_exists = await conn.fetchval(
                        f"""
                        SELECT EXISTS(
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = $1 AND table_name = $2
                        )
                        """,
                        settings.PG_SCHEMA,
                        settings.KEY_VALUE_TABLE_NAME,
                    )

                    if keyvalue_table_exists:
                        logger.info(
                            f"Table '{settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME}' already exists"
                        )
                    else:
                        # Create key-value table if not exists
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME} (
                                id SERIAL PRIMARY KEY,
                                redis_key TEXT NOT NULL UNIQUE,
                                redis_value TEXT NOT NULL,
                                redis_db INTEGER NOT NULL,
                                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                            )
                        """
                        )
                        logger.info(
                            f"Successfully created key-value table '{settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME}'"
                        )

                    # Only create indexes if tables exist
                    if stream_table_exists or not keyvalue_table_exists:
                        # Add indexes if needed
                        await conn.execute(
                            f"""
                            CREATE INDEX IF NOT EXISTS idx_{settings.STREAM_TABLE_NAME}_stream_key_timestamp 
                            ON {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} (stream_key, streaming_date DESC)
                        """
                        )
                        await conn.execute(
                            f"""
                            CREATE INDEX IF NOT EXISTS idx_{settings.STREAM_TABLE_NAME}_message_id 
                            ON {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} (message_id)
                        """
                        )
                        await conn.execute(
                            f"""
                            CREATE INDEX IF NOT EXISTS idx_{settings.STREAM_TABLE_NAME}_unix_timestamp 
                            ON {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} (unix_timestamp)
                        """
                        )

                    if keyvalue_table_exists:
                        # Add indexes for key-value table
                        await conn.execute(
                            f"""
                            CREATE INDEX IF NOT EXISTS idx_{settings.KEY_VALUE_TABLE_NAME}_redis_key 
                            ON {settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME} (redis_key)
                        """
                        )
                        await conn.execute(
                            f"""
                            CREATE INDEX IF NOT EXISTS idx_{settings.KEY_VALUE_TABLE_NAME}_redis_db 
                            ON {settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME} (redis_db)
                        """
                        )

                    logger.info(
                        "PostgreSQL schema and indexes setup completed successfully"
                    )
                except Exception as table_error:
                    error_msg = str(table_error)
                    logger.warning(f"Failed to create or update tables: {error_msg}")
                    logger.warning(
                        "Will attempt to use existing tables if available. Some functionality may be limited."
                    )

                # We've done our best to set up the schema, now let's verify if tables exist
                try:
                    # Check if stream table exists and is accessible
                    await conn.fetchval(
                        f"SELECT 1 FROM {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} LIMIT 1"
                    )
                    logger.info(
                        f"Verified stream table {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} exists and is accessible"
                    )
                except Exception as e:
                    logger.warning(f"Stream table not accessible: {e}")
                    logger.warning("Stream synchronization will not function correctly")

                try:
                    # Check if key-value table exists and is accessible
                    await conn.fetchval(
                        f"SELECT 1 FROM {settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME} LIMIT 1"
                    )
                    logger.info(
                        f"Verified key-value table {settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME} exists and is accessible"
                    )
                except Exception as e:
                    logger.warning(f"Key-value table not accessible: {e}")
                    logger.warning(
                        "Key-value synchronization will not function correctly"
                    )

                return {"success": True, "error": None, "data": None}
        except Exception as e:
            logger.error(f"Failed to setup PostgreSQL schema: {e}")
            logger.warning("Service will continue with limited functionality")
            return {
                "success": False,
                "error": f"Failed to setup PostgreSQL schema: {e}",
                "data": None,
            }

    async def insert_stream_messages(
        self, messages: List[Tuple[str, str, Dict[str, Any]]]
    ) -> OperationResult:
        """
        Insert stream messages into PostgreSQL within a transaction.

        Args:
            messages: List of (stream_key, message_id, data) tuples

        Returns:
            OperationResult indicating success or failure
        """
        if not await self.connect():
            logger.error("Cannot insert messages: PostgreSQL connection failed")
            return {
                "success": False,
                "error": "PostgreSQL connection failed",
                "data": None,
            }

        try:
            assert self.pool is not None  # Help type checker
            async with self.pool.acquire() as conn:
                # First check if the table exists
                table_exists = await conn.fetchval(
                    f"""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )
                    """,
                    settings.PG_SCHEMA,
                    settings.STREAM_TABLE_NAME,
                )

                # If the table doesn't exist, create it now
                if not table_exists:
                    logger.warning(
                        f"Table {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} doesn't exist. Attempting to create it now."
                    )
                    try:
                        # Create partitioned table - the only supported table structure
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} (
                                id SERIAL,
                                stream_key TEXT NOT NULL,
                                message_id TEXT NOT NULL,
                                data JSONB NOT NULL,
                                unix_timestamp TIMESTAMP NOT NULL,
                                streaming_date DATE NOT NULL,
                                UNIQUE(stream_key, message_id, streaming_date)
                            ) PARTITION BY RANGE (streaming_date)
                        """
                        )

                        # Create default partition
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}_default PARTITION OF {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}
                            DEFAULT
                        """
                        )

                        # Create partition for today
                        today = datetime.date.today()
                        tomorrow = today + datetime.timedelta(days=1)
                        today_str = today.strftime("%Y_%m_%d")

                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}_{today_str} PARTITION OF {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}
                            FOR VALUES FROM ('{today}') TO ('{tomorrow}')
                        """
                        )

                        logger.info(
                            f"Successfully created partitioned table {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}"
                        )
                    except Exception as create_error:
                        # If table creation fails, log clearly and return false
                        logger.error(f"Failed to create table: {create_error}")
                        logger.error(
                            "This application requires PostgreSQL 10+ with partitioning support and proper permissions"
                        )
                        logger.error(
                            "Please ask your database administrator to create the required tables"
                        )
                        return {
                            "success": False,
                            "error": f"Failed to create table: {create_error}",
                            "data": None,
                        }

                # Verify the table exists before proceeding
                table_exists = await conn.fetchval(
                    f"""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )
                    """,
                    settings.PG_SCHEMA,
                    settings.STREAM_TABLE_NAME,
                )

                if not table_exists:
                    logger.error(
                        f"Table {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} still doesn't exist after creation attempts"
                    )
                    return {
                        "success": False,
                        "error": f"Table {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} still doesn't exist after creation attempts",
                        "data": None,
                    }

                # Create transaction
                async with conn.transaction():
                    # For partitioned tables, we need today's partition
                    try:
                        # Check if we need to create a new partition for today
                        today = datetime.date.today()
                        tomorrow = today + datetime.timedelta(days=1)
                        today_str = today.strftime("%Y_%m_%d")

                        # Create partition for today if it doesn't exist
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}_{today_str} PARTITION OF {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME}
                            FOR VALUES FROM ('{today}') TO ('{tomorrow}')
                            """
                        )
                    except Exception as partition_error:
                        # Ignore partition errors - might be using a simple table
                        logger.debug(
                            f"Partition creation skipped, possibly using a simple table: {partition_error}"
                        )

                    # Use a direct raw SQL query instead of a prepared statement for better JSONB type preservation
                    for stream_key, message_id, data in messages:

                        # Extract unix timestamp from message_id or use current time
                        timestamp_seconds = int(time.time())
                        # Redis stream message IDs have the format: timestamp-sequence
                        # We can extract the timestamp part from the message_id
                        if "-" in message_id:
                            try:
                                unix_timestamp_ms = int(message_id.split("-")[0])
                                timestamp_seconds = (
                                    unix_timestamp_ms // 1000
                                )  # Convert from ms to seconds
                            except (ValueError, IndexError):
                                # If extraction fails, use current time
                                pass

                        # Convert to timestamp without timezone
                        unix_timestamp = datetime.datetime.fromtimestamp(
                            timestamp_seconds
                        )

                        # Create a date value for partitioning (YYYY-MM-DD)
                        entry_date = datetime.date.fromtimestamp(timestamp_seconds)

                        # Define conflict action based on settings
                        if STREAM_CONFLICT_OVERWRITE:
                            conflict_action = "DO UPDATE SET data = EXCLUDED.data, unix_timestamp = EXCLUDED.unix_timestamp"
                        else:
                            conflict_action = "DO NOTHING"

                        # Execute parameterized SQL query
                        query = f"""
                        INSERT INTO {settings.PG_SCHEMA}.{settings.STREAM_TABLE_NAME} (stream_key, message_id, data, unix_timestamp, streaming_date)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (stream_key, message_id, streaming_date) {conflict_action}
                        """
                        await conn.execute(
                            query,
                            stream_key,
                            message_id,
                            json.dumps(data),
                            unix_timestamp,
                            entry_date,
                        )

                logger.debug(
                    f"Inserted {len(messages)} stream messages into PostgreSQL"
                )
                return {"success": True, "error": None, "data": None}
        except Exception as e:
            logger.error(f"Failed to insert stream messages: {e}")
            return {
                "success": False,
                "error": f"Failed to insert stream messages: {e}",
                "data": None,
            }

    async def upsert_key_values(
        self, key_values: List[Tuple[str, Any]]
    ) -> OperationResult:
        """
        Upsert key-value pairs into PostgreSQL.

        Args:
            key_values: List of (key, value) tuples

        Returns:
            OperationResult indicating success or failure
        """
        if not await self.connect():
            logger.error("Cannot upsert key values: PostgreSQL connection failed")
            return {
                "success": False,
                "error": "PostgreSQL connection failed",
                "data": None,
            }

        try:
            assert self.pool is not None  # Help type checker
            async with self.pool.acquire() as conn:
                # Create transaction
                async with conn.transaction():
                    # Prepare statement for better performance with many upserts
                    stmt = await conn.prepare(
                        f"""
                        INSERT INTO {settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME} (redis_key, redis_value, redis_db)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (redis_key) DO UPDATE
                        SET redis_value = EXCLUDED.redis_value,
                            redis_db = EXCLUDED.redis_db,
                            timestamp = CURRENT_TIMESTAMP
                        """
                    )

                    # Execute statement for each key-value pair
                    for key, value in key_values:
                        # Convert value to string if it's not already
                        value_str = str(value) if value is not None else ""
                        # Use default database number 0
                        db_num = 0
                        await stmt.fetchval(key, value_str, db_num)

                logger.debug(
                    f"Upserted {len(key_values)} key-value pairs into PostgreSQL"
                )
                return {"success": True, "error": None, "data": None}
        except Exception as e:
            logger.error(f"Failed to upsert key-value pairs: {e}")
            return {
                "success": False,
                "error": f"Failed to upsert key-value pairs: {e}",
                "data": None,
            }

    async def get_key_value(self, key: str, db_num: int) -> Optional[Any]:
        """
        Get a key-value pair from PostgreSQL.

        Args:
            key: Key to get
            db_num: Database number of the key

        Returns:
            Record if found, None otherwise
        """
        if not await self.connect():
            logger.error("Cannot get key value: PostgreSQL connection failed")
            return None

        try:
            assert self.pool is not None  # Help type checker
            async with self.pool.acquire() as conn:
                record = await conn.fetchrow(
                    f"""
                    SELECT * FROM {settings.PG_SCHEMA}.{settings.KEY_VALUE_TABLE_NAME}
                    WHERE redis_key = $1 AND redis_db = $2
                    """,
                    key,
                    db_num,
                )
                return record
        except Exception as e:
            logger.error(f"Failed to get key-value pair: {e}")
            return None

    async def close(self) -> None:
        """Close the PostgreSQL connection pool."""
        if self.pool:
            try:
                await self.pool.close()
                logger.info("Closed PostgreSQL connection pool")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection pool: {e}")
            finally:
                self.pool = None

    async def is_running(self) -> bool:
        """
        Check if the PostgreSQL service is running.

        Returns:
            bool: True if the service is running, False otherwise
        """
        if not self.pool:
            return False

        try:
            # Try to get a connection from the pool
            async with self.pool.acquire() as conn:
                # Execute a simple query to check connection
                await conn.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"PostgreSQL service is not running: {e}")
            return False

    async def start(self) -> None:
        """
        Start the PostgreSQL service.
        This is a no-op since the service is managed externally.
        """
        logger.info(
            "PostgreSQL service start requested - no-op as service is managed externally"
        )
        return

    async def stop(self) -> None:
        """
        Stop the PostgreSQL service.
        This is a no-op since the service is managed externally.
        """
        logger.info(
            "PostgreSQL service stop requested - no-op as service is managed externally"
        )
        return
