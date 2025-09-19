"""
Orchestrator for Redis MQTT Synchronization.

This module provides the orchestration for Redis to MQTT synchronization.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from ..config import settings
from ..services.mqtt_service import MqttService
from ..services.redis_service import RedisService

logger = logging.getLogger("redis-mqtt-sync")


def validate_redis_data_for_mqtt(
    entry_id: str, data: Dict[str, Any], stream_key: str
) -> bool:
    """
    Validates Redis data for MQTT publishing.

    Args:
        entry_id: Redis stream entry ID
        data: Data to validate
        stream_key: Stream key the data came from

    Returns:
        True if data is valid, False otherwise
    """
    try:
        # Basic validation - ensure data is a dictionary
        if not isinstance(data, dict):
            logger.error(
                f"Invalid message format in stream {stream_key}, entry {entry_id}: not a dictionary"
            )
            return False

        # Add any additional validation rules here
        return True
    except Exception as e:
        logger.error(
            f"Error validating message {entry_id} from stream {stream_key}: {e}"
        )
        return False


def should_publish_message(data: Dict[str, Any]) -> bool:
    """
    Checks if a message should be published based on filtering rules.

    Args:
        data: Message data from Redis

    Returns:
        True if the message should be published, False otherwise
    """
    # If filtering is disabled, always publish
    if not settings.MQTT_FILTER_FIELD:
        return True

    # If field is set but values are empty, never publish
    if not settings.MQTT_FILTER_VALUES:
        return False

    # Check if the filter field exists in the data
    field_value = data.get(settings.MQTT_FILTER_FIELD)
    if field_value is None:
        logger.debug(
            f"Filter field '{settings.MQTT_FILTER_FIELD}' not found in message. Skipping."
        )
        return False

    # Check if the field's value matches one of the allowed values
    # Convert field_value to string for consistent comparison
    if str(field_value) in settings.MQTT_FILTER_VALUES:
        return True
    else:
        logger.debug(
            f"Message field '{settings.MQTT_FILTER_FIELD}' value '{field_value}' not in allowed values {settings.MQTT_FILTER_VALUES}. Skipping."
        )
        return False


class SyncOrchestrator:
    """
    Orchestrates the Redis to MQTT synchronization process.

    This class is the core component of the Redis MQTT synchronization service.
    It coordinates the discovery of Redis streams, reading of messages, validation,
    filtering, publishing to MQTT, and acknowledgment of processed messages.

    The orchestrator uses the RedisService and MqttService to interact with these systems,
    and manages the entire lifecycle of the synchronization process.

    Features:
    - Stream auto-discovery
    - Consumer group management
    - Parallel stream processing
    - Message validation and filtering
    - Error handling and retry logic
    - Graceful shutdown

    The main processing loop runs continuously until stopped, processing
    messages from all discovered streams in parallel.
    """

    def __init__(self) -> None:
        """Initialize the orchestrator with Redis and MQTT services."""
        self.redis_service = RedisService()
        self.mqtt_service = MqttService()
        self.is_running = False
        self.streams_to_process: Set[str] = set(
            settings.REDIS_STREAMS
        )  # Start with manually configured streams

    async def process_streams(self) -> None:
        """Process all streams in parallel."""
        if not self.streams_to_process:
            logger.debug("No streams to process")
            return

        logger.debug(
            f"Processing {len(self.streams_to_process)} streams: {self.streams_to_process}"
        )
        tasks = [
            self._process_single_stream(stream) for stream in self.streams_to_process
        ]
        await asyncio.gather(*tasks)

    async def _process_single_stream(self, stream_name: str) -> None:
        """
        Process a single Redis stream with validation and filtering.

        Args:
            stream_name: Name of the Redis stream to process
        """
        # Read messages from Redis
        read_result = await self.redis_service.read_stream_messages(stream_name)
        if not read_result:
            return

        source_key, messages = read_result
        if not messages:
            return

        valid_mqtt_messages = []
        invalid_entry_ids_to_ack = []
        filtered_out_ids_to_ack = []  # IDs of messages filtered out

        for entry_id, data in messages:
            if validate_redis_data_for_mqtt(entry_id, data, source_key):
                # Check if the message passes the filter
                if should_publish_message(data):
                    try:
                        # Format valid & filtered message for MQTT
                        mqtt_payload = json.dumps(
                            {
                                "stream": source_key,
                                "id": entry_id,
                                "data": data,
                                "timestamp": datetime.now().isoformat(),
                            }
                        )
                        valid_mqtt_messages.append((entry_id, mqtt_payload))
                    except Exception as format_err:
                        logger.error(
                            f"Error formatting validated message {entry_id} for MQTT: {format_err}"
                        )
                        invalid_entry_ids_to_ack.append(entry_id)
                else:
                    # Message is valid but filtered out
                    logger.debug(
                        f"Message {entry_id} from stream {source_key} was filtered out."
                    )
                    filtered_out_ids_to_ack.append(entry_id)
            else:
                # Validation failed
                invalid_entry_ids_to_ack.append(entry_id)

        # Publish valid messages
        successfully_published_ids = []
        if valid_mqtt_messages:
            successfully_published_ids = await self._publish_messages(
                stream_name, valid_mqtt_messages
            )

        # Combine successfully published, invalid, and filtered IDs for ACK
        all_ids_to_ack = (
            successfully_published_ids
            + invalid_entry_ids_to_ack
            + filtered_out_ids_to_ack
        )

        # Acknowledge all processed messages
        if all_ids_to_ack:
            await self.redis_service.ack_messages(source_key, all_ids_to_ack)

    async def _publish_messages(
        self, stream_name: str, messages_to_publish: List[Tuple[str, str]]
    ) -> List[str]:
        """
        Publishes valid formatted messages to MQTT.

        Args:
            stream_name: The Redis stream name
            messages_to_publish: List of (entry_id, mqtt_payload) tuples to publish

        Returns:
            List of successfully published entry IDs
        """
        topic_suffix = stream_name.replace(":", "/")
        topic = f"{settings.MQTT_BASE_TOPIC}/{topic_suffix}"
        published_ids = []

        for entry_id, mqtt_payload in messages_to_publish:
            success = await self.mqtt_service.publish(
                topic, mqtt_payload, qos=settings.MQTT_QOS, retain=settings.MQTT_RETAIN
            )
            if success:
                published_ids.append(entry_id)
                logger.debug(
                    f"Published message {entry_id} from {stream_name} to {topic}"
                )
            else:
                logger.error(
                    f"Failed to publish message {entry_id} from {stream_name} to {topic}"
                )

        return published_ids

    async def run(self) -> None:
        """Main execution loop."""
        if self.is_running:
            logger.warning("Already running")
            return

        self.is_running = True
        logger.info("Starting Redis to MQTT synchronization service")

        try:
            # Initialize streams to process
            await self.redis_service.setup_initial_consumer_groups(
                list(self.streams_to_process)
            )

            # Main processing loop
            while self.is_running:
                try:
                    # If not connected to MQTT, attempt to connect
                    if not await self.mqtt_service.connect():
                        logger.warning(
                            "Not connected to MQTT broker, waiting to retry..."
                        )
                        await asyncio.sleep(5)
                        continue

                    # Auto-discover streams if enabled
                    if settings.STREAM_AUTO_DISCOVERY:
                        discovered_streams = await self.redis_service.discover_streams()
                        for prefixed_key, _, _ in discovered_streams:
                            if prefixed_key not in self.streams_to_process:
                                self.streams_to_process.add(prefixed_key)
                                logger.info(
                                    f"Added discovered stream to processing list: {prefixed_key}"
                                )
                                # Create consumer group for newly discovered stream
                                await self.redis_service.setup_initial_consumer_groups(
                                    [prefixed_key]
                                )

                    # Process all streams
                    await self.process_streams()

                    # Small delay to prevent tight loop when idle
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error in main sync loop: {e}")
                    await asyncio.sleep(5)  # Longer sleep on error
        except asyncio.CancelledError:
            logger.info("Service task cancelled")
        except Exception as e:
            logger.critical(f"Critical unexpected error in run loop: {e}")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the orchestrator and clean up resources."""
        logger.info("Stopping Redis to MQTT synchronization service")
        self.is_running = False

        # Disconnect from MQTT broker
        await self.mqtt_service.disconnect()

        # Disconnect from Redis
        await self.redis_service.disconnect()

        logger.info("Redis to MQTT synchronization service stopped")
