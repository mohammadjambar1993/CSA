#!/usr/bin/env python3
"""
Service Interfaces
----------------
Abstract base classes for Redis and PostgreSQL services.

This module defines the core interfaces for Redis to PostgreSQL synchronization,
using the common type definitions and protocols for consistent typing across
all sync modules.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from ..core.types import (
    AckResult,
    ConnectionResult,
    MessageResult,
    OperationResult,
    RedisKeyValue,
    RedisKeyValuePair,
    RedisMessage,
    RedisMessageID,
    RedisServiceProtocol,
    RedisStreamInfo,
    RedisStreamMessage,
    RedisStreamMessages,
    RedisStreamOffset,
    ServiceProtocol,
)


class RedisServiceInterface(RedisServiceProtocol):
    """Interface for Redis service operations."""

    @abstractmethod
    async def connect(self, db_num: int) -> ConnectionResult:
        """
        Establish connection to Redis.

        Args:
            db_num: Redis database number

        Returns:
            True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    async def discover_streams(self) -> List[RedisStreamInfo]:
        """
        Discover Redis streams.

        Returns:
            List of tuples (display_name, db_num, stream_key)
        """
        pass

    @abstractmethod
    async def setup_consumer_groups(
        self, streams_to_process: List[str]
    ) -> OperationResult:
        """
        Set up consumer groups for streams.

        Args:
            streams_to_process: List of stream names to process

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def ack_messages(self, stream_key: str, message_ids: List[str]) -> AckResult:
        """
        Acknowledge messages in a stream.

        Args:
            stream_key: Stream key to acknowledge in
            message_ids: List of message IDs to acknowledge

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def read_key_value(self, key: str, db_num: int) -> Optional[RedisKeyValue]:
        """
        Read a key-value pair from Redis.

        Args:
            key: Key to read
            db_num: Database number to read from

        Returns:
            Tuple of (key, value) or None if error
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close Redis connections."""
        pass


class PostgresServiceInterface(ServiceProtocol):
    """Interface for PostgreSQL service operations."""

    @abstractmethod
    async def connect(self) -> ConnectionResult:
        """
        Establish connection to PostgreSQL.

        Returns:
            True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    async def setup_schema(self) -> OperationResult:
        """
        Set up required database schema and indexes.

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def insert_stream_messages(
        self, messages: List[Tuple[str, str, Dict[str, Any]]]
    ) -> OperationResult:
        """
        Insert stream messages into PostgreSQL.

        Args:
            messages: List of (stream_key, message_id, data) tuples

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def upsert_key_values(
        self, key_values: List[RedisKeyValuePair]
    ) -> OperationResult:
        """
        Upsert key-value pairs into PostgreSQL.

        Args:
            key_values: List of (key, value, db_num) tuples

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def get_key_value(self, key: str, db_num: int) -> Optional[Any]:
        """
        Get a key-value pair from PostgreSQL.

        Args:
            key: Key to get
            db_num: Database number of the key

        Returns:
            Record if found, None otherwise
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close PostgreSQL connections."""
        pass
