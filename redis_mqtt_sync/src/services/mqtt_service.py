"""
MQTT Service for Redis MQTT Synchronization.

This module provides MQTT operations for the Redis MQTT Synchronization system.
"""

import asyncio
import logging
import ssl
from typing import Any, Callable, Dict, Optional

import aiomqtt
from aiomqtt import Client, MqttError, Will

from ..config import settings
from .interfaces import MqttServiceInterface

logger = logging.getLogger("redis-mqtt-sync")


class MqttService(MqttServiceInterface):
    """MQTT service implementation."""

    def __init__(self) -> None:
        """Initialize MQTT service."""
        self._client: Optional[Client] = None
        self._is_connected = False
        self._reconnection_task: Optional[asyncio.Task] = None

    async def connect(self) -> bool:
        """
        Connect to MQTT broker.

        Returns:
            True if connection is successful, False otherwise
        """
        if self._is_connected and self._client:
            return True

        # Cancel any existing reconnection task
        if self._reconnection_task:
            self._reconnection_task.cancel()
            self._reconnection_task = None

        # Configure TLS if enabled
        tls_context = None
        if settings.MQTT_USE_TLS:
            tls_context = ssl.create_default_context()

        # Attempt to connect
        try:
            logger.info(
                f"Connecting to MQTT broker at {settings.MQTT_BROKER_HOST}:{settings.MQTT_BROKER_PORT}"
            )

            # Create will for clean disconnect detection
            will = Will(
                topic=f"{settings.MQTT_BASE_TOPIC}/status",
                payload="offline",
                qos=1,
                retain=True,
            )

            # Create client with async context manager
            async with aiomqtt.Client(
                hostname=settings.MQTT_BROKER_HOST,
                port=settings.MQTT_BROKER_PORT,
                username=settings.MQTT_USERNAME,
                password=settings.MQTT_PASSWORD,
                identifier=settings.MQTT_CLIENT_ID,
                tls_context=tls_context,
                will=will,
            ) as client:
                self._client = client

                # Publish online status
                await client.publish(
                    f"{settings.MQTT_BASE_TOPIC}/status",
                    "online",
                    qos=1,
                    retain=True,
                )

                # Set as connected
                self._is_connected = True
                logger.info("Connected to MQTT broker successfully")

                # Keep connection alive
                while self._is_connected:
                    await asyncio.sleep(1)

            return True
        except MqttError as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self._is_connected = False
            self._client = None
            return False
        except asyncio.CancelledError:
            logger.info("MQTT connection task cancelled")
            self._is_connected = False
            self._client = None
            raise

    async def disconnect(self) -> None:
        """Disconnect from MQTT broker."""
        if self._reconnection_task:
            self._reconnection_task.cancel()
            self._reconnection_task = None

        # Set flag to exit the connection loop
        self._is_connected = False

        # The client will auto-disconnect when exiting the context manager
        logger.info("Disconnected from MQTT broker")

    async def publish(
        self, topic: str, payload: str, qos: int = 0, retain: bool = False
    ) -> bool:
        """
        Publish a message to an MQTT topic.

        Args:
            topic: MQTT topic to publish to
            payload: Message content as a string
            qos: Quality of Service level (0, 1, or 2)
            retain: Whether the message should be retained

        Returns:
            True if published successfully, False otherwise
        """
        if not self._is_connected or not self._client:
            logger.warning("Cannot publish: Not connected to MQTT broker")
            return False

        try:
            await self._client.publish(topic, payload, qos=qos, retain=retain)
            return True
        except MqttError as e:
            logger.error(f"Error publishing message to {topic}: {e}")
            # Connection might be broken, trigger reconnect
            if self._is_connected:
                self._is_connected = False
                self._start_reconnection_task()
            return False

    def _start_reconnection_task(self) -> None:
        """Start background task for automatic reconnection."""
        if not self._reconnection_task or self._reconnection_task.done():
            # Create and store the reconnection task
            reconnection_task = asyncio.create_task(self._reconnection_loop())
            self._reconnection_task = reconnection_task

    async def _reconnection_loop(self) -> None:
        """Background loop for reconnection attempts."""
        retry_interval = 5
        max_interval = 60
        current_interval = retry_interval

        while not self._is_connected:
            logger.info(
                f"Attempting to reconnect to MQTT broker in {current_interval} seconds..."
            )
            await asyncio.sleep(current_interval)

            try:
                success = await self.connect()
                if success:
                    logger.info("MQTT reconnection successful")
                    return
            except Exception as e:
                logger.error(f"Error during reconnection attempt: {e}")

            # Exponential backoff with a maximum
            current_interval = min(current_interval * 2, max_interval)
