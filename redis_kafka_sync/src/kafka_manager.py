import asyncio
import logging
import os
import ssl
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", 20971520))
KAFKA_MAX_CONCURRENT_SENDS = int(
    os.getenv("KAFKA_MAX_CONCURRENT_SENDS", 100)
)  # Max in-flight messages


async def handle_kafka_error(operation_desc: str, coro, *args, **kwargs):
    """Helper to execute an AIOKafka coroutine and handle common errors."""
    try:
        return await coro(*args, **kwargs)
    except KafkaError as e:
        logger.error(f"Kafka error during '{operation_desc}': {e}")
    except Exception as e:
        logger.error(f"Unexpected error during '{operation_desc}': {e}")
    return None  # Indicate failure


def _setup_kafka_security() -> Dict[str, Any]:
    """Sets up SSL context and SASL credentials based on environment variables.

    Returns:
        A dictionary containing security parameters for AIOKafkaProducer.
    """
    security_params = {}
    ssl_context = None
    sasl_plain_username = None
    sasl_plain_password = None

    # Determine security protocol
    security_protocol = (
        "PLAINTEXT"
        if os.getenv("KAFKA_IS_SWARM_MODE", "false").lower() == "true"
        else os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    )
    security_params["security_protocol"] = security_protocol
    logger.info(f"Determined Kafka security protocol: {security_protocol}")

    # Setup SSL context if needed
    if security_protocol in ("SSL", "SASL_SSL"):
        truststore_location = os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION", "")
        if not truststore_location:
            logger.error(
                "SSL protocol specified but KAFKA_SSL_TRUSTSTORE_LOCATION is not set."
            )
        else:
            try:
                ssl_context = ssl.create_default_context()
                pem_cert = os.getenv("KAFKA_SSL_PEM_LOCATION")
                if pem_cert and os.path.exists(pem_cert):
                    logger.info(f"Loading PEM certificate from: {pem_cert}")
                    ssl_context.load_verify_locations(pem_cert)
                    logger.info("PEM certificate loaded successfully for SSL context.")
                    security_params["ssl_context"] = ssl_context
                else:
                    logger.warning(
                        f"PEM certificate not found at KAFKA_SSL_PEM_LOCATION. "
                        f"Truststore at {truststore_location} might be JKS, which Python SSL cannot use directly. "
                        f"Ensure a PEM file is generated and KAFKA_SSL_PEM_LOCATION is set."
                    )
            except Exception as e:
                logger.error(f"Failed to setup SSL context: {e}")
                ssl_context = None

    # Setup SASL credentials if needed
    if security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        sasl_plain_username = os.getenv(
            "KAFKA_SASL_USERNAME", "producer"
        )  # Example fallback
        sasl_plain_password = os.getenv(
            "KAFKA_SASL_PASSWORD", "producer-secret"
        )  # Example fallback
        sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")

        if not sasl_plain_username or not sasl_plain_password:
            logger.error(
                f"SASL protocol ({security_protocol}) specified, but KAFKA_SASL_USERNAME or "
                f"KAFKA_SASL_PASSWORD environment variables are not set."
            )
        else:
            security_params["sasl_mechanism"] = sasl_mechanism
            security_params["sasl_plain_username"] = sasl_plain_username
            security_params["sasl_plain_password"] = sasl_plain_password
            logger.info(f"SASL authentication configured (Mechanism: {sasl_mechanism})")

    if security_protocol == "SASL_SSL" and "ssl_context" not in security_params:
        logger.error(
            "SASL_SSL protocol specified, but SSL context setup failed or PEM certificate was not found. Kafka connection will likely fail."
        )

    return security_params


class KafkaManager:
    """Manages the Kafka producer lifecycle and message sending."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BROKER
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_running = False
        self._kafka_security_params = _setup_kafka_security()
        self.send_semaphore = asyncio.Semaphore(KAFKA_MAX_CONCURRENT_SENDS)
        self._ack_listeners = (
            {}
        )  # Store callbacks for acknowledgements {message_key: callback}
        logger.info(
            f"KafkaManager initialized. Concurrency limit: {KAFKA_MAX_CONCURRENT_SENDS}"
        )

    async def start(self):
        """Start the Kafka producer connection."""
        if self.producer is not None and self.is_running:
            logger.warning("Kafka producer already started.")
            return

        try:
            producer_config = {
                "bootstrap_servers": self.bootstrap_servers,
                "max_request_size": KAFKA_MAX_REQUEST_SIZE,
                # Add other relevant AIOKafkaProducer settings here if needed
                # e.g., retries, retry_backoff_ms, acks
            }
            producer_config.update(self._kafka_security_params)

            logger.info(
                "Initializing AIOKafkaProducer with config: {{k: v for k, v in producer_config.items() if k != 'sasl_plain_password'}}"
            )
            self.producer = AIOKafkaProducer(**producer_config)

            await handle_kafka_error(
                "starting Kafka producer connection", self.producer.start
            )

            # Basic check: did handle_kafka_error return None?
            if (
                self.producer._closed
            ):  # Check if producer closed immediately after start attempt
                logger.error("Kafka producer failed to start (remained closed).")
                raise KafkaError("Producer failed to start")

            logger.info(
                f"Kafka producer started successfully for servers: {self.bootstrap_servers}"
            )
            self.is_running = True

        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            if self.producer:
                await handle_kafka_error(
                    "stopping producer after start failure", self.producer.stop
                )
            self.producer = None
            self.is_running = False
            raise  # Re-raise the exception so the caller knows it failed

    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer and self.is_running:
            logger.info("Stopping Kafka producer...")
            await handle_kafka_error("stopping Kafka producer", self.producer.stop)
            self.producer = None
            self.is_running = False
            logger.info("Kafka producer stopped.")
        elif self.producer:
            # Producer exists but wasn't running (maybe start failed)
            await handle_kafka_error(
                "stopping potentially failed Kafka producer", self.producer.stop
            )
            self.producer = None
            self.is_running = False

    async def send(
        self, topic: str, value: bytes, key: str, ack_callback: callable
    ) -> bool:
        """
        Send a message asynchronously using a semaphore for backpressure.
        The ack_callback(key: str, success: bool, error: Optional[Exception]) will be called upon completion.

        Returns:
            bool: True if the send was successfully initiated, False otherwise.
        """
        if not self.is_running or not self.producer:
            logger.error("Cannot send message, Kafka producer is not running.")
            return False

        logger.debug(f"Acquiring semaphore to send message {key}...")
        await self.send_semaphore.acquire()
        logger.debug(f"Semaphore acquired for message {key}. Proceeding with send.")

        try:
            # Store the callback before sending
            self._ack_listeners[key] = ack_callback

            future = await handle_kafka_error(
                f"sending message {key}",
                self.producer.send,
                topic=topic,
                value=value,
                key=key.encode("utf-8"),  # Ensure key is bytes
            )

            if future is None:
                # Error occurred during the immediate send call (logged by helper)
                self.send_semaphore.release()
                logger.warning(f"Kafka send call failed immediately for key {key}.")
                # Clean up listener as ack won't happen
                self._ack_listeners.pop(key, None)
                # Notify caller immediately of failure
                try:
                    ack_callback(key, False, KafkaError("Send call failed immediately"))
                except Exception as cb_err:
                    logger.error(
                        f"Error in ack_callback during immediate failure for key {key}: {cb_err}"
                    )
                return False  # Send initiation failed

            # Add internal callback to handle ack and then call the user's callback
            future.add_done_callback(
                lambda fut: asyncio.create_task(self._handle_ack(fut, key))
            )

            logger.debug(f"Message {key} sent to aiokafka buffer, awaiting ack.")
            return True  # Send initiated successfully

        except Exception as e:
            logger.error(
                f"Unexpected error during Kafka send initiation for key {key}: {e}"
            )
            self.send_semaphore.release()
            self._ack_listeners.pop(key, None)  # Clean up listener
            # Notify caller of failure
            try:
                ack_callback(key, False, e)
            except Exception as cb_err:
                logger.error(
                    f"Error in ack_callback during unexpected failure for key {key}: {cb_err}"
                )
            return False  # Send initiation failed

    async def _handle_ack(self, future: asyncio.Future, key: str):
        """Internal callback to process Kafka ack and trigger user callback."""
        success = False
        error = None
        try:
            record_metadata = future.result()  # Check for exceptions from Kafka
            logger.debug(
                f"Kafka acknowledged message: "
                f"topic={record_metadata.topic}, partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}, key={key}"
            )
            success = True
        except Exception as e:
            logger.error(f"Kafka failed to acknowledge message {key}: {e}")
            error = e
        finally:
            # Retrieve and remove the callback
            callback = self._ack_listeners.pop(key, None)
            if callback:
                try:
                    # Call the original user-provided callback
                    await callback(key, success, error)
                except Exception as cb_err:
                    logger.error(
                        f"Error executing ack_callback for key {key}: {cb_err}"
                    )
            else:
                logger.warning(
                    f"No ack_callback found for completed message key {key}."
                )

            # ALWAYS release the semaphore
            logger.debug(f"Releasing semaphore for message {key}.")
            self.send_semaphore.release()

    async def send_and_wait(self, topic: str, value: bytes, key: str) -> bool:
        """
        Sends a message and waits for acknowledgement. Primarily for specific use cases like
        syncing old keys where immediate confirmation is desired. Uses the semaphore.

        Returns:
            bool: True if acknowledged successfully, False otherwise.
        """
        if not self.is_running or not self.producer:
            logger.error("Cannot send_and_wait, Kafka producer is not running.")
            return False

        logger.debug(f"Acquiring semaphore to send_and_wait for message {key}...")
        await self.send_semaphore.acquire()
        logger.debug(
            f"Semaphore acquired for message {key}. Proceeding with send_and_wait."
        )

        success = False
        try:
            result = await handle_kafka_error(
                f"sending and waiting for message {key}",
                self.producer.send_and_wait,
                topic=topic,
                value=value,
                key=key.encode("utf-8"),
            )
            if result is not None:
                logger.debug(f"Kafka send_and_wait successful for key {key}.")
                success = True
            # else: Error logged by helper

        except Exception as e:
            logger.error(
                f"Unexpected error during Kafka send_and_wait for key {key}: {e}"
            )
            success = False
        finally:
            # ALWAYS release the semaphore
            logger.debug(f"Releasing semaphore for message {key} after send_and_wait.")
            self.send_semaphore.release()

        return success
