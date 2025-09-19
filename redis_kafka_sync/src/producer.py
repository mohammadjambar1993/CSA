import argparse
import os
import time
import uuid
from datetime import datetime
from functools import partial

import redis
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load environment variables from .env file
load_dotenv()

# Load environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_TIME_TO_LIVE = int(os.getenv("REDIS_TIME_TO_LIVE", 3600))
KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", 20971520))
KAFKA_TOPIC_ECG_1 = os.getenv("KAFKA_TOPIC_ECG_1", "ecg_blob")

# Redis client setup
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Sequence number tracking
sequence_counters = {}


def get_next_sequence(device_id):
    """Get the next sequence number for a device."""
    if device_id not in sequence_counters:
        sequence_counters[device_id] = 0
    sequence_counters[device_id] += 1
    return sequence_counters[device_id]


def generate_message_key(device_id, timestamp=None, sequence=None):
    """
    Generate a message key in the format device_id:timestamp:sequence.

    Args:
        device_id: Unique identifier for the device
        timestamp: Message timestamp (default: current time)
        sequence: Message sequence number (default: auto-increment)

    Returns:
        String key in format device_id:timestamp:sequence
    """
    if timestamp is None:
        timestamp = datetime.utcnow()

    iso_timestamp = timestamp.isoformat().replace("+00:00", "Z")

    if sequence is None:
        sequence = get_next_sequence(device_id)

    sequence_str = f"{sequence:04d}"
    return f"{device_id}:{iso_timestamp}:{sequence_str}"


def on_send_success(metadata, topic, value, device_id=None):
    """
    Callback function for when the message is successfully sent to Kafka.
    Also caches the message in Redis.
    """
    print(
        f"Message sent successfully to {metadata.topic}, partition {metadata.partition}, offset {metadata.offset}"
    )

    # Generate a consistent key for Redis caching
    if device_id:
        # Use the business key that was generated when sending the message
        key = metadata.key.decode("utf-8")
        # Also create a reference from Kafka position to this key
        kafka_position_key = f"{metadata.topic}:{metadata.partition}:{metadata.offset}"
        redis_client.setex(f"kafka_index:{kafka_position_key}", REDIS_TIME_TO_LIVE, key)
    else:
        # Legacy format for backwards compatibility
        key = f"{metadata.topic}:{metadata.partition}:{metadata.offset}"

    # Cache the message in Redis
    try:
        redis_client.setex(key, REDIS_TIME_TO_LIVE, value)
        print(f"Cached message in Redis: {key}")
    except Exception as e:
        print(f"Error caching message in Redis: {e}")


def on_send_error(excp):
    """Callback function for when there is an error sending the message to Kafka."""
    print(f"Failed to send message: {excp}")


def produce(ptopic, device_id="default.device.001"):
    """
    Produce sample messages to a Kafka topic.

    Args:
        ptopic: The Kafka topic to produce to
        device_id: The device identifier (default: default.device.001)
    """
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        max_request_size=KAFKA_MAX_REQUEST_SIZE,
        key_serializer=str.encode,
    )
    print(f"Connected to Kafka: {producer.bootstrap_connected()}")

    try:
        for i in range(10):
            timestamp = datetime.utcnow()
            # Generate a business key
            message_key = generate_message_key(device_id, timestamp)

            # Create the message
            message = f"Message {i} from {device_id} at {timestamp.isoformat()}".encode(
                "utf-8"
            )
            print(
                f"Producing a message with key: {message_key}, size: {len(message)} bytes"
            )

            # Send the message with the key
            future = producer.send(ptopic, key=message_key, value=message)
            future.add_callback(
                partial(
                    on_send_success, topic=ptopic, value=message, device_id=device_id
                )
            )
            future.add_errback(on_send_error)
            producer.flush()

            time.sleep(0.5)  # Small delay between messages
    except KafkaError as e:
        print(f"Failed to produce message: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        producer.close()
        print("Producer closed.")


def produce_message(topic, message, device_id=None):
    """
    Produce a message to a given Kafka topic with a business key.

    Args:
        topic: The Kafka topic to produce to
        message: The message string to produce
        device_id: The device identifier (default: generate a UUID)
    """
    if device_id is None:
        device_id = f"unknown.device.{uuid.uuid4().hex[:8]}"

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        max_request_size=KAFKA_MAX_REQUEST_SIZE,
        key_serializer=str.encode,
    )
    print(f"Connected to Kafka: {producer.bootstrap_connected()}")

    try:
        # Generate message key
        timestamp = datetime.utcnow()
        message_key = generate_message_key(device_id, timestamp)

        # Convert message to bytes if it's not already
        if isinstance(message, str):
            message_bytes = message.encode("utf-8")
        else:
            message_bytes = message

        print(
            f"Producing a message with key: {message_key}, size: {len(message_bytes)} bytes"
        )

        # Send with key
        future = producer.send(topic, key=message_key, value=message_bytes)
        future.add_callback(
            partial(
                on_send_success, topic=topic, value=message_bytes, device_id=device_id
            )
        )
        future.add_errback(on_send_error)
        producer.flush()
    except KafkaError as e:
        print(f"Failed to produce message: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        producer.close()
        print("Producer closed.")


def generate_15mb_payload():
    """
    Generate a 15MB binary payload using os.urandom.
    Returns bytes: A binary payload of size 15MB.
    """
    try:
        payload = os.urandom(15 * 1024 * 1024)  # 15 MB of random binary data
        print(f"Generated 15MB binary payload of size: {len(payload)} bytes")
        return payload
    except Exception as e:
        print(f"Error generating 15MB payload: {e}")
        return None


def produce_message_test(topic):
    """
    Function to test producing a 15MB payload message to Kafka.

    Parameters:
        topic (str): The Kafka topic where the message will be produced.
    """
    try:
        # Generate timestamp for message
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Generate the 15MB payload
        test_payload = generate_15mb_payload()  # Generate 15MB binary payload
        if test_payload is None:
            raise Exception("Failed to generate 15MB payload")

        # Create message with payload
        message = f"Test message with 15MB binary content | Timestamp: {timestamp}"

        # Combine message with the payload
        message_bytes = message.encode("utf-8") + test_payload

        # Produce the message with payload
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER], max_request_size=KAFKA_MAX_REQUEST_SIZE
        )
        print(f"Producing test message of size: {len(message_bytes)} bytes")

        future = producer.send(topic, value=message_bytes)
        future.add_callback(partial(on_send_success, topic=topic, value=message_bytes))
        future.add_errback(on_send_error)
        producer.flush()  # Ensure the message is sent
        producer.close()  # Close the producer

        print("Test message with 15MB payload produced successfully.")

    except Exception as e:
        print(f"Error in produce_message_test: {e}")


def produce_file(topic, file_path):
    """
    Produce a binary file to a given Kafka topic.
    Parameters:
        topic (str): The Kafka topic where the file will be produced.
        file_path (str): The path to the binary file to produce.
    """
    try:
        # Check if the file exists
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Get the file size
        file_size = os.path.getsize(file_path)
        if file_size > KAFKA_MAX_REQUEST_SIZE:
            raise ValueError(
                f"File size {file_size} exceeds the maximum allowed size of {KAFKA_MAX_REQUEST_SIZE} bytes"
            )

        # Read the file content
        with open(file_path, "rb") as file:
            file_content = file.read()

        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER], max_request_size=KAFKA_MAX_REQUEST_SIZE
        )
        print(f"Connected to Kafka: {producer.bootstrap_connected()}")

        # Produce the file content
        future = producer.send(topic, value=file_content)
        future.add_callback(partial(on_send_success, topic=topic, value=file_content))
        future.add_errback(on_send_error)
        producer.flush()  # Ensure the message is sent
        producer.close()  # Close the producer

        print(f"File {file_path} produced successfully to topic {topic}.")
    except FileNotFoundError as e:
        print(f"Error in produce_file: {e}")
    except ValueError as e:
        print(f"Error in produce_file: {e}")
    except KafkaError as e:
        print(f"Failed to produce file: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if "producer" in locals():
            producer.close()
            print("Producer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer Script")
    parser.add_argument("topic", type=str, help="The Kafka topic to produce to")
    parser.add_argument(
        "--device",
        type=str,
        help="Device ID (optional)",
        default="biosignal.device.001",
    )
    args = parser.parse_args()

    if not args.topic:
        raise ValueError("Topic argument is required")

    produce(args.topic, device_id=args.device)
