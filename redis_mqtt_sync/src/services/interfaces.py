"""
Service interfaces for Redis MQTT Synchronization.

This module defines abstract base classes for services used in the
Redis MQTT Synchronization system. These interfaces allow for standardized
interaction between components and facilitate dependency injection for easier
testing and future extensibility.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union


class RedisServiceInterface(ABC):
    """
    Interface for Redis operations.

    This interface defines the required methods for interacting with Redis
    in the context of Redis to MQTT synchronization. Implementations must
    handle connection management, stream discovery, message reading, and
    acknowledgment.
    """

    @abstractmethod
    async def connect(self, db: int = 0) -> bool:
        """
        Connect to Redis server on specific database.

        Args:
            db: Redis database number to connect to

        Returns:
            True if connection is successful, False otherwise
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Disconnect from Redis server.

        Closes all connections and releases resources.
        """
        pass

    @abstractmethod
    async def discover_streams(self) -> List[Tuple[str, int, int]]:
        """
        Discover Redis streams across configured databases.

        Returns:
            List of tuples with (stream_key, db_number, length) for each discovered stream
        """
        pass

    @abstractmethod
    async def setup_initial_consumer_groups(self, streams: List[str]) -> bool:
        """
        Create consumer groups for streams if they don't exist.

        Args:
            streams: List of stream keys to create consumer groups for

        Returns:
            True if all required groups were created or already exist, False if errors occurred
        """
        pass

    @abstractmethod
    async def read_stream_messages(
        self, stream_key: str
    ) -> Optional[Tuple[str, List[Tuple[str, Dict[str, Any]]]]]:
        """
        Read messages from a Redis stream using a consumer group.

        Args:
            stream_key: The key of the stream to read from

        Returns:
            A tuple containing (stream_key, [(entry_id, data), ...]) if successful,
            None otherwise
        """
        pass

    @abstractmethod
    async def ack_messages(self, stream_key: str, entry_ids: List[str]) -> int:
        """
        Acknowledge messages from a stream.

        Args:
            stream_key: The key of the stream to acknowledge messages from
            entry_ids: List of message IDs to acknowledge

        Returns:
            Number of messages successfully acknowledged
        """
        pass


class MqttServiceInterface(ABC):
    """
    Interface for MQTT operations.

    This interface defines the required methods for interacting with MQTT
    brokers in the context of Redis to MQTT synchronization. Implementations
    must handle connection management and message publishing.
    """

    @abstractmethod
    async def connect(self) -> bool:
        """
        Connect to MQTT broker.

        Returns:
            True if connection is successful, False otherwise
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Disconnect from MQTT broker.

        Closes the connection and releases resources.
        """
        pass

    @abstractmethod
    async def publish(
        self, topic: str, payload: str, qos: int = 0, retain: bool = False
    ) -> bool:
        """
        Publish a message to an MQTT topic.

        Args:
            topic: MQTT topic to publish to
            payload: Message content as a string
            qos: Quality of Service level (0, 1, or 2)
            retain: Whether the message should be retained by the broker

        Returns:
            True if published successfully, False otherwise
        """
        pass
