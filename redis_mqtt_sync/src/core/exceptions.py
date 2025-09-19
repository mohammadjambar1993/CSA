"""
Exceptions for Redis MQTT Synchronization.

This module defines custom exceptions used throughout the application.
"""


class RedisClientError(Exception):
    """Base exception for Redis client errors."""

    pass


class ConnectionError(RedisClientError):
    """Exception raised when connection to Redis fails."""

    pass


class StreamError(RedisClientError):
    """Exception raised when an error occurs with Redis streams."""

    pass


class ConsumerGroupError(StreamError):
    """Exception raised when an error occurs with Redis consumer groups."""

    pass


class MQTTClientError(Exception):
    """Base exception for MQTT client errors."""

    pass


class MQTTConnectionError(MQTTClientError):
    """Exception raised when connection to MQTT broker fails."""

    pass


class MQTTPublishError(MQTTClientError):
    """Exception raised when publishing to MQTT broker fails."""

    pass


class ConfigurationError(Exception):
    """Exception raised when there's an error in configuration."""

    pass


class ValidationError(Exception):
    """Exception raised when data validation fails."""

    pass
