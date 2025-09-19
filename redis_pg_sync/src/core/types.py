"""
Core type definitions for Redis to PostgreSQL synchronization.

This module defines the core types and protocols used by the Redis to PostgreSQL
synchronization service.
"""

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
    Union,
)

# Redis-specific types
RedisMessage = Dict[Union[str, bytes], Union[str, bytes, int, float]]
RedisMessageID = str
RedisStreamOffset = Union[str, int]  # Can be ">" for latest, "$" for last, or timestamp
RedisConsumerGroup = Tuple[str, str]  # (group_name, consumer_name)
RedisKeyValue = Dict[str, Any]  # Generic key-value pair
RedisKeyValuePair = Tuple[str, Any]  # (key, value) pair
RedisStreamInfo = Dict[str, Any]  # Stream information from Redis
RedisStreamMessage = Dict[str, Any]  # Single stream message
RedisStreamMessages = List[Tuple[str, Dict[str, Any]]]  # List of (id, message) pairs
RedisStreamTuple = Tuple[str, int, str]  # (key, db, original_key)
RedisStreamList = List[RedisStreamTuple]  # List of stream tuples
RedisStreamDict = Dict[str, Any]  # Stream information as dictionary
RedisStreamDictList = List[Dict[str, Any]]  # List of stream dictionaries

# PostgreSQL-specific types
PostgresTable = str
PostgresColumn = str
PostgresSchema = str
PostgresValue = Union[str, int, float, bool, None]
PostgresKeyValue = Tuple[str, Any]  # (key, value) pair
PostgresKeyValueList = List[PostgresKeyValue]  # List of (key, value) pairs
PostgresKeyValueWithDb = Tuple[str, Any, int]  # (key, value, db_num) tuple
PostgresKeyValueWithDbList = List[
    PostgresKeyValueWithDb
]  # List of (key, value, db_num) tuples


# Operation result types
@dataclass
class OperationResult(TypedDict):
    """Result of a Redis operation."""

    success: bool
    error: Optional[str]
    data: Optional[Any]


@dataclass
class ConnectionResult(OperationResult):
    """Result of a connection attempt."""

    pass


@dataclass
class MessageResult(OperationResult):
    """Result of a message operation."""

    message_id: Optional[str]
    stream: Optional[str]
    messages: Optional[List[Tuple[str, Dict[str, Any]]]]


@dataclass
class AckResult(OperationResult):
    """Result of a message acknowledgment."""

    count: int


@dataclass
class ValidationResult(OperationResult):
    """Result of a validation operation."""

    is_valid: bool
    details: Optional[Dict[str, Any]]


@dataclass
class SyncResult(OperationResult):
    """Result of a synchronization operation."""

    synced_count: int
    failed_count: int
    skipped_count: int


# Stream and message information types
@dataclass
class StreamInfo(TypedDict):
    """Information about a Redis stream."""

    key: str
    db: int
    original_key: str
    length: int
    groups: List[str]


@dataclass
class MessageInfo(TypedDict):
    """Information about a Redis stream message."""

    id: str
    stream: str
    data: Dict[str, str]
    timestamp: int


# Configuration types
@dataclass
class ServiceConfig(TypedDict):
    """Configuration for the synchronization service."""

    source_redis: Dict[str, Any]
    target_postgres: Dict[str, Any]
    sync_config: Dict[str, Any]
    logging_config: Dict[str, Any]


# Callback types
ErrorCallback = Callable[[str, Exception], None]
AsyncErrorCallback = Callable[[str, Exception], None]


# Protocol definitions
class ServiceProtocol(Protocol):
    """Protocol defining the interface for service implementations."""

    async def start(self) -> None:
        """Start the service."""
        ...

    async def stop(self) -> None:
        """Stop the service."""
        ...

    async def is_running(self) -> bool:
        """Check if the service is running."""
        ...


class RedisServiceProtocol(Protocol):
    """Protocol defining the interface for Redis service implementations."""

    async def connect(self, db_num: int) -> ConnectionResult:
        """
        Connect to Redis database.

        Args:
            db_num: Redis database number to connect to

        Returns:
            ConnectionResult indicating success or failure
        """
        ...

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        ...

    async def ping(self) -> bool:
        """
        Ping the Redis server to check connection.

        Returns:
            True if ping was successful, False otherwise
        """
        ...

    async def discover_streams(self) -> List[RedisStreamInfo]:
        """
        Discover Redis streams matching the configured pattern.

        Returns:
            List of stream information dictionaries
        """
        ...

    async def read_stream_messages(
        self,
        stream_name: str,
        count: int = 10,
        block: int = 0,
        last_id: RedisStreamOffset = ">",
    ) -> MessageResult:
        """
        Read messages from a Redis stream.

        Args:
            stream_name: Name of the stream to read from
            count: Number of messages to read
            block: Time to block in milliseconds
            last_id: ID or timestamp to start reading from

        Returns:
            MessageResult containing the read messages
        """
        ...

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
        ...

    async def validate_stream(self, stream_key: str) -> ValidationResult:
        """
        Validate a Redis stream's configuration and state.

        Args:
            stream_key: Stream key to validate

        Returns:
            ValidationResult indicating if stream is valid
        """
        ...

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
        ...

    async def ack_messages(self, stream_key: str, message_ids: List[str]) -> AckResult:
        """
        Acknowledge messages in a stream.

        Args:
            stream_key: Stream key to acknowledge messages in
            message_ids: List of message IDs to acknowledge

        Returns:
            AckResult containing acknowledgment statistics
        """
        ...


class PostgresServiceProtocol(Protocol):
    """Protocol defining the interface for PostgreSQL service implementations."""

    async def connect(self, db: Optional[str] = None) -> ConnectionResult:
        """
        Connect to PostgreSQL database.

        Args:
            db: Database name to connect to

        Returns:
            ConnectionResult indicating success or failure
        """
        ...

    async def disconnect(self) -> None:
        """Disconnect from PostgreSQL."""
        ...

    async def setup_schema(self, schema: PostgresSchema) -> OperationResult:
        """
        Set up the database schema.

        Args:
            schema: Schema name to set up

        Returns:
            OperationResult indicating success or failure
        """
        ...

    async def insert_stream_messages(
        self, table: PostgresTable, messages: List[MessageInfo]
    ) -> OperationResult:
        """
        Insert stream messages into a table.

        Args:
            table: Table name to insert into
            messages: List of messages to insert

        Returns:
            OperationResult indicating success or failure
        """
        ...

    async def upsert_key_values(
        self, key_values: PostgresKeyValueWithDbList
    ) -> OperationResult:
        """
        Upsert key-value pairs with database number.

        Args:
            key_values: List of (key, value, db_num) tuples to upsert

        Returns:
            OperationResult indicating success or failure
        """
        ...
