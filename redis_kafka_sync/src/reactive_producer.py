#!/usr/bin/env python3
"""
Reactive Kafka Producer with Redis Cache (Refactored)
----------------------------------------
Orchestrates sending messages to Kafka, caching them in Redis,
and ensuring synchronization between the two using dedicated managers.
"""

import argparse
import asyncio
import fnmatch
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

# Import the new managers
from kafka_manager import KafkaManager

# Import the consolidated key formatter
from key_format import generate_message_key
from redis_manager import RedisManager

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("reactive-producer-orchestrator")  # Changed logger name

# Load environment variables
load_dotenv(
    os.path.join(os.path.dirname(__file__), "..", "env.conf")
)  # Load from parent dir

# --- Key Generation Helpers (kept here for simplicity) ---

# --- Main Orchestrator Class ---


class ReactiveKafkaProducer:
    """Orchestrates Redis caching and Kafka production using dedicated managers."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """Initialize the orchestrator with Redis and Kafka managers."""
        logger.info("Initializing ReactiveKafkaProducer Orchestrator...")
        self.redis_manager = RedisManager()
        self.kafka_manager = KafkaManager(bootstrap_servers)
        self.is_running = False

    async def start(self):
        """Start the Redis and Kafka managers."""
        if self.is_running:
            logger.warning("Orchestrator already running.")
            return

        logger.info("Starting KafkaManager...")
        try:
            # Ensure Redis clients can be created (pools initialized on demand)
            # Check connectivity to target Redis (remote for caching)
            target_redis = await self.redis_manager.get_target_redis_client()
            if not target_redis:
                raise ConnectionError("Failed to connect to target Redis for caching")

            # Check connectivity to source Redis (local for streams/data)
            source_redis = await self.redis_manager.get_source_redis_client(0)
            if not source_redis:
                logger.warning(
                    "Failed to connect to source Redis for stream reading. Will only process cached data."
                )

            await self.kafka_manager.start()
            self.is_running = True
            logger.info("ReactiveKafkaProducer Orchestrator started successfully.")

            # Initialize unsynced keys sets on startup to ensure they're up-to-date
            # This scans target Redis to find all unsynced keys and populates our tracking sets
            logger.info("Initializing unsynced keys sets for target Redis...")
            try:
                count = await self.redis_manager.rebuild_unsynced_keys_set()
                logger.info(
                    f"Rebuilt unsynced keys set for target Redis with {count} keys."
                )
            except Exception as e:
                logger.error(
                    f"Error rebuilding unsynced keys set for target Redis: {e}"
                )
                # Continue, don't abort startup

        except Exception as e:
            logger.error(f"Orchestrator failed to start: {e}")
            await self.stop()  # Attempt cleanup
            self.is_running = False
            raise  # Re-raise to signal failure

    async def stop(self):
        """Stop the Kafka and Redis managers."""
        if (
            not self.is_running
            and not self.kafka_manager.producer
            and not self.redis_manager.redis_pools
        ):
            logger.info("Orchestrator already stopped or not initialized.")
            return

        logger.info("Stopping ReactiveKafkaProducer Orchestrator...")
        await self.kafka_manager.stop()
        await self.redis_manager.close_all_pools()
        self.is_running = False
        logger.info("ReactiveKafkaProducer Orchestrator stopped.")

    async def _handle_kafka_ack(
        self, key: str, success: bool, error: Optional[Exception]
    ):
        """Callback passed to KafkaManager.send to mark sync status in Redis."""
        if success:
            logger.debug(
                f"Kafka ack received for key {key}. Marking as synced in Redis."
            )
            # Determine DB from key if needed, assuming default 0 here
            db_num = 0
            marked = await self.redis_manager.mark_as_synced(key, db=db_num)
            if not marked:
                logger.error(
                    f"Kafka ack successful for {key}, but FAILED to mark as synced in Redis DB {db_num}!"
                )
            # else: logged by redis_manager
        else:
            # Error already logged by KafkaManager
            logger.warning(
                f"Kafka send failed for key {key} (Error: {error}). Will remain in Redis cache for later sync."
            )
            # No action needed here, sync_unsynced_keys will handle it later.

    async def send_message(
        self,
        topic: str,
        message: bytes,
        device_id: Optional[str] = None,
        key: Optional[str] = None,
    ) -> Optional[str]:
        """
        Cache message in target Redis and send asynchronously to Kafka.
        Handles marking sync status in Redis upon Kafka acknowledgement.

        Returns:
            The message key if cached successfully, None otherwise.
            Note: Returning the key does *not* guarantee Kafka acknowledgement yet.
        """
        if not self.is_running:
            logger.error("Orchestrator not running. Cannot send message.")
            return None

        # 1. Generate Key using imported function
        # Note: key_format.generate_message_key handles timestamp and sequence internally
        message_key = key or (
            generate_message_key(device_id) if device_id else str(uuid.uuid4())
        )

        # 2. Cache in target Redis
        # If caching fails, we don't send to Kafka to maintain consistency guarantee
        if not await self.redis_manager.cache_message(message_key, message):
            logger.error(
                f"Failed to cache message {message_key} in target Redis. Aborting Kafka send."
            )
            return None  # Crucial: Message must be cached before Kafka send attempt

        # 3. Send to Kafka (asynchronously)
        # Pass the _handle_kafka_ack method as the callback
        send_initiated = await self.kafka_manager.send(
            topic=topic,
            value=message,
            key=message_key,
            ack_callback=self._handle_kafka_ack,
        )

        if send_initiated:
            logger.debug(f"Kafka send initiated for key {message_key}. Awaiting ack.")
            # Return the key to the caller, acknowledging receipt and caching.
            # Actual sync confirmation happens in the callback.
            return message_key
        else:
            # Error during send initiation already logged by KafkaManager
            logger.warning(
                f"Kafka send initiation failed for key {message_key}. Message remains cached but unsynced."
            )
            # Return the key because it *is* cached. sync_unsynced_keys will handle it.
            return message_key

    async def sync_unsynced_keys(self, topic: str) -> int:
        """
        Find keys in target Redis not marked as synced and send them to Kafka.
        Uses KafkaManager.send_and_wait for simpler blocking confirmation.

        Returns:
            Number of keys successfully synced in this run.
        """
        if not self.is_running:
            logger.error("Orchestrator not running. Cannot sync unsynced keys.")
            return 0

        synced_count = 0
        logger.info(
            f"Checking for unsynced keys in target Redis to sync to Kafka topic: {topic}..."
        )
        unsynced_keys = await self.redis_manager.get_unsynced_keys()

        if not unsynced_keys:
            logger.info("No unsynced keys found in target Redis.")
            return 0

        logger.info(
            f"Attempting to sync {len(unsynced_keys)} unsynced keys from target Redis..."
        )

        for db_num, key in unsynced_keys:
            # 1. Get message content from Redis
            message = await self.redis_manager.get_message(key, db=db_num)
            if message is None:
                logger.warning(
                    f"Could not retrieve message content for unsynced key {key} in DB {db_num}. Skipping."
                )
                # Consider adding the sync key anyway to prevent retrying a deleted key?
                # await self.redis_manager.mark_as_synced(key, db=db_num, ttl=300) # Short TTL for potentially deleted keys
                continue

            # 2. Send to Kafka and wait for confirmation
            logger.debug(f"Sending unsynced key {key} from DB {db_num} to Kafka...")
            kafka_success = await self.kafka_manager.send_and_wait(
                topic=topic, value=message, key=key
            )

            # 3. Mark as synced in Redis ONLY if Kafka send succeeded
            if kafka_success:
                redis_marked = await self.redis_manager.mark_as_synced(key, db=db_num)
                if redis_marked:
                    synced_count += 1
                    logger.debug(
                        f"Successfully synced key {key} from DB {db_num} to Kafka."
                    )
                else:
                    logger.warning(
                        f"Kafka send for unsynced key {key} (DB {db_num}) succeeded, but FAILED to mark as synced in Redis!"
                    )
            else:
                # Error logged by KafkaManager
                logger.warning(
                    f"Failed to send unsynced key {key} (DB {db_num}) to Kafka. It will be retried later."
                )

        logger.info(
            f"Finished syncing unsynced keys. Synced {synced_count}/{len(unsynced_keys)} keys in this run."
        )
        return synced_count

    # --- Batch Sending (Example Usage) ---
    async def send_batch(
        self, topic: str, device_id: str, count: int = 10
    ) -> Tuple[int, int]:
        """
        Send a batch of test messages asynchronously using the send_message method.

        Returns:
             Tuple (successful_cache_and_initiate, total_attempted)
             Note: successful_cache_and_initiate means Redis cache worked and Kafka send was *initiated*.
        """
        if not self.is_running:
            logger.error("Orchestrator not running. Cannot send batch.")
            return (0, count)

        successful_queued = 0
        tasks = []
        logger.info(
            f"Preparing to send batch of {count} messages for device {device_id}..."
        )

        for i in range(count):
            timestamp = datetime.now(timezone.utc)
            # Let generate_message_key handle sequence internally
            # seq = get_next_sequence(device_id) # Get sequence number correctly - REMOVED
            data = {
                "device_id": device_id,
                "timestamp": timestamp.isoformat(),
                # "sequence": seq, # Sequence is now part of the key generated below, remove if not needed in payload
                "value": random.random() * 100,
                "status": "generated",
            }
            message = json.dumps(data).encode("utf-8")
            # Use the imported generate_message_key which handles sequence internally
            task_key = generate_message_key(device_id, timestamp=timestamp)
            tasks.append(self.send_message(topic=topic, message=message, key=task_key))

        results = await asyncio.gather(*tasks)
        successful_queued = sum(1 for result in results if result is not None)

        logger.info(
            f"Batch send processing complete. Successfully cached & initiated send for {successful_queued}/{count} messages."
        )
        return successful_queued, count

    # Add a new method to read from source Redis streams
    async def process_source_streams(
        self,
        topic_mapping: Optional[Dict[str, str]] = None,
        stream_pattern: str = "*-stream",
        source_db: int = 0,
        batch_size: int = 10,
    ) -> int:
        """
        Read from streams in source Redis and send to Kafka via target Redis caching.

        Args:
            topic_mapping: Dict mapping stream names to Kafka topics. If a stream has no mapping,
                          the default topic specified in send_message will be used.
            stream_pattern: Pattern for discovering streams in source Redis.
            source_db: DB number in source Redis where streams are located.
            batch_size: Maximum number of messages to read per stream.

        Returns:
            Number of messages successfully processed (cached and Kafka send initiated).
        """
        if not self.is_running:
            logger.error("Orchestrator not running. Cannot process source streams.")
            return 0

        # Default empty mapping if none provided
        topic_mapping = topic_mapping or {}

        # Get available streams from source Redis
        streams = await self.redis_manager.get_stream_keys(
            pattern=stream_pattern, db=source_db
        )
        if not streams:
            logger.debug(
                f"No matching streams found in source Redis DB {source_db} with pattern {stream_pattern}"
            )
            return 0

        logger.info(
            f"Processing {len(streams)} streams from source Redis DB {source_db}"
        )

        total_processed = 0
        for stream in streams:
            # Determine target Kafka topic from mapping or use default
            target_topic = topic_mapping.get(stream)
            if not target_topic:
                # Try pattern matching for wildcard mappings
                for pattern, topic in topic_mapping.items():
                    if "*" in pattern and fnmatch.fnmatch(stream, pattern):
                        target_topic = topic
                        break

            if not target_topic:
                target_topic = topic  # Fall back to the default topic

            logger.debug(
                f"Processing stream {stream} from source Redis, sending to topic {target_topic}"
            )

            # Read messages from the stream
            messages = await self.redis_manager.read_stream_messages(
                stream=stream,
                count=batch_size,
                block=100,  # Small blocking time to allow for new messages
                last_id="0-0",  # Start from beginning, in production would track last ID
                db=source_db,
            )

            if not messages:
                logger.debug(f"No messages found in stream {stream}")
                continue

            logger.debug(f"Found {len(messages)} messages in stream {stream}")

            # Process each message
            for msg_id, msg_data in messages:
                try:
                    # Convert message data to a format suitable for Kafka
                    # This depends on your data format and needs
                    processed_data = {}
                    for field, value in msg_data.items():
                        try:
                            # Decode bytes to strings where possible
                            field_str = field.decode("utf-8")
                            try:
                                value_str = value.decode("utf-8")
                                processed_data[field_str] = value_str
                            except UnicodeDecodeError:
                                # Keep binary data as is
                                processed_data[field_str] = value
                        except UnicodeDecodeError:
                            # If field name isn't UTF-8, use hex representation
                            field_hex = field.hex()
                            processed_data[f"field_{field_hex}"] = value

                    # Add stream metadata
                    processed_data["_stream"] = stream
                    processed_data["_id"] = msg_id.decode("utf-8")
                    processed_data["_timestamp"] = int(time.time() * 1000)

                    # Serialize the data
                    json_data = json.dumps(processed_data).encode("utf-8")

                    # Generate a key using the stream and message ID
                    stream_key = f"{stream}:{msg_id.decode('utf-8')}"

                    # Send to Kafka via target Redis
                    result_key = await self.send_message(
                        topic=target_topic, message=json_data, key=stream_key
                    )

                    if result_key:
                        # Successfully cached and initiated Kafka send
                        # Acknowledge the message in the source stream
                        success = await self.redis_manager.acknowledge_stream_message(
                            stream=stream, message_id=msg_id, db=source_db
                        )

                        if success:
                            total_processed += 1
                            logger.debug(
                                f"Successfully processed message {msg_id} from stream {stream}"
                            )
                        else:
                            logger.warning(
                                f"Failed to acknowledge message {msg_id} in stream {stream} after caching"
                            )
                    else:
                        logger.warning(
                            f"Failed to cache/initiate send for message {msg_id} from stream {stream}"
                        )

                except Exception as e:
                    logger.error(
                        f"Error processing message {msg_id} from stream {stream}: {e}"
                    )
                    # Continue with next message

        logger.info(
            f"Finished processing source Redis streams. Processed {total_processed} messages."
        )
        return total_processed


# --- File Producer Helper (Refactored to use Orchestrator) ---


async def file_producer(
    orchestrator: ReactiveKafkaProducer,
    topic: str,
    file_path: str,
    device_id: Optional[str] = None,
) -> bool:
    """
    Read a binary file and send it via the orchestrator.
    """
    try:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False

        with open(file_path, "rb") as f:
            content = f.read()

        if not content:
            logger.warning(f"File is empty: {file_path}. Skipping send.")
            return False  # Don't send empty files

        file_name = os.path.basename(file_path)
        key = f"{device_id or 'file'}:{file_name}:{int(time.time())}"

        # Use the orchestrator's send_message
        result_key = await orchestrator.send_message(topic, content, key=key)

        # send_message returns key if cached & initiated, None otherwise.
        # We don't know the final Kafka status here, but log initiation success.
        if result_key:
            logger.info(
                f"Successfully cached and initiated send for file {file_path} to topic {topic} with key {key}"
            )
            return True
        else:
            logger.error(
                f"Failed to cache or initiate send for file {file_path} to topic {topic}"
            )
            return False

    except Exception as e:
        logger.error(f"Error sending file {file_path} via orchestrator: {e}")
        return False


# --- Main Execution Logic ---


async def main(args):
    """Main async entry point."""
    orchestrator = ReactiveKafkaProducer()
    run_duration = 120  # Default run duration (seconds) if in monitoring mode
    monitoring_task = None
    exit_code = 0

    try:
        await orchestrator.start()

        # Perform initial sync of any old keys
        await orchestrator.sync_unsynced_keys(args.topic)

        if args.file:
            logger.info(f"Sending file: {args.file}")
            success = await file_producer(
                orchestrator, args.topic, args.file, args.device
            )
            logger.info(
                f"File production initiated: {'Success' if success else 'Failed'}"
            )
            # Allow some time for potential ack before stopping
            await asyncio.sleep(5)
        elif args.message:
            logger.info(f"Sending single message for device {args.device}")
            key = await orchestrator.send_message(
                args.topic, args.message.encode("utf-8"), args.device
            )
            if key:
                logger.info(f"Single message cached and send initiated with key: {key}")
                await asyncio.sleep(5)  # Allow time for ack
            else:
                logger.error("Failed to cache or initiate send for single message.")
                exit_code = 1
        elif args.count and args.count > 0:
            logger.info(
                f"Sending batch of {args.count} messages for device {args.device}"
            )
            success_count, total_count = await orchestrator.send_batch(
                args.topic, args.device, args.count
            )
            logger.info(f"Batch send initiated: {success_count}/{total_count}")
            # Allow more time for batch acks
            await asyncio.sleep(15)
        else:
            # Default mode: Monitor Redis for unsynced keys periodically
            logger.info(
                f"No specific send operation requested. Entering monitoring mode for {run_duration} seconds."
            )
            logger.info("Periodically checking for and syncing unsynced keys...")
            start_time = time.monotonic()
            while time.monotonic() - start_time < run_duration:
                await orchestrator.sync_unsynced_keys(args.topic)
                await asyncio.sleep(10)  # Check every 10 seconds
            logger.info("Monitoring mode finished.")

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in main: {e}")
        exit_code = 1
    finally:
        logger.info("Cleaning up orchestrator resources...")
        await orchestrator.stop()
        logger.info("Cleanup complete.")

    return exit_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reactive Kafka Producer Orchestrator")
    parser.add_argument("topic", type=str, help="Kafka topic to produce to")
    parser.add_argument(
        "--device",
        type=str,
        default="biosignal.device.001",
        help="Default Device ID for generating keys/messages",
    )
    # Actions (mutually exclusive group ideally, but simplified here)
    parser.add_argument(
        "--message", type=str, help="Send a single message (UTF-8 string)"
    )
    parser.add_argument("--file", type=str, help="Send the content of a binary file")
    parser.add_argument("--count", type=int, help="Send a batch of N test messages")
    # If none of the above are provided, it runs in periodic sync monitoring mode.

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    args = parser.parse_args()

    # Set log level for our logger and potentially others if needed
    log_level_int = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level_int,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )
    logger.setLevel(log_level_int)
    logging.getLogger("kafka_manager").setLevel(log_level_int)
    logging.getLogger("redis_manager").setLevel(log_level_int)
    # Set aiokafka logger level too?
    # logging.getLogger("aiokafka").setLevel(logging.WARNING)

    logger.info("Starting application...")
    exit_status = asyncio.run(main(args))
    logger.info(f"Application finished with exit code {exit_status}.")
    # Ensure clean exit
    # Sometimes asyncio might keep loops running, explicitly exit
    os._exit(exit_status)
