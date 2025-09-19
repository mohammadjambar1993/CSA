#!/usr/bin/env python3
"""
Metrics Module
------------
Metrics collection for Redis to PostgreSQL synchronization.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Metrics:
    """
    Metrics collection for the synchronization process.

    This class tracks various metrics about the synchronization process,
    including operation counts, errors, and performance statistics.

    Attributes:
        messages_processed: Number of stream messages processed
        key_values_processed: Number of key-value pairs processed
        errors: Number of errors encountered
        last_sync_time: Timestamp of the last successful sync
        redis_operations: Number of Redis operations performed
        postgres_operations: Number of PostgreSQL operations performed
        retries: Number of retry attempts
        stream_discovery_count: Number of streams discovered
    """

    messages_processed: int = 0
    key_values_processed: int = 0
    errors: int = 0
    last_sync_time: Optional[datetime] = None
    redis_operations: int = 0
    postgres_operations: int = 0
    retries: int = 0
    stream_discovery_count: int = 0

    def reset(self) -> None:
        """Reset all metrics to their default values."""
        self.messages_processed = 0
        self.key_values_processed = 0
        self.errors = 0
        self.last_sync_time = None
        self.redis_operations = 0
        self.postgres_operations = 0
        self.retries = 0
        self.stream_discovery_count = 0

    def update_sync_time(self) -> None:
        """Update the last sync time to the current time."""
        self.last_sync_time = datetime.now()

    def increment_redis_ops(self, count: int = 1) -> None:
        """Increment Redis operations count."""
        self.redis_operations += count

    def increment_postgres_ops(self, count: int = 1) -> None:
        """Increment PostgreSQL operations count."""
        self.postgres_operations += count

    def increment_errors(self, count: int = 1) -> None:
        """Increment error count."""
        self.errors += count

    def increment_retries(self, count: int = 1) -> None:
        """Increment retry count."""
        self.retries += count

    def as_dict(self) -> dict:
        """Return metrics as a dictionary."""
        return {
            "messages_processed": self.messages_processed,
            "key_values_processed": self.key_values_processed,
            "errors": self.errors,
            "last_sync_time": (
                self.last_sync_time.isoformat() if self.last_sync_time else None
            ),
            "redis_operations": self.redis_operations,
            "postgres_operations": self.postgres_operations,
            "retries": self.retries,
            "stream_discovery_count": self.stream_discovery_count,
        }
