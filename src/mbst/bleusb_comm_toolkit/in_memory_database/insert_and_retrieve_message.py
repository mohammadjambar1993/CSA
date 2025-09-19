from .redis_functions import (
    consumer_redis_key,
    consumer_redis_streams,
    producer_redis_key,
    producer_redis_streams,
)


def insert_and_retrieve_message_streams(message, device="undefined", char="unknown"):
    try:
        # Convert the message to bytes
        message_bytes = message.encode("utf-8")

        # Insert the message into Redis
        key = producer_redis_streams(
            message_bytes, device=device, device_id="0000", char=char
        )

        if key is None:
            print("Failed to store message in Redis.")
            return

        # Retrieve the message from Redis using the key
        retrieved_data = consumer_redis_streams(
            filter=True, stream_key=key, char=char, device=device
        )

        if retrieved_data is not None:
            # Convert the retrieved data back to a string
            for entry in retrieved_data:
                print(
                    f"Entry ID: {entry['id']}, Timestamp: {entry['timestamp']}, Characteristic: {entry['characteristic']}, Value: {entry['value']}, Key:{entry['key']}"
                )
        else:
            print("Failed to retrieve message from Redis Streams.")

    except Exception as e:
        print(f"Error: {e}")


def insert_and_retrieve_message(message, device="undefined", char="unknown"):
    try:
        # Convert the message to bytes
        message_bytes = message.encode("utf-8")

        # Insert the message into Redis
        key = producer_redis_key(message_bytes, device=device)

        if key is None:
            print("Failed to store message in Redis.")
            return

        # Retrieve the message from Redis using the key
        retrieved_data = consumer_redis_key(key, device=device)

        if retrieved_data is not None:
            # Convert the retrieved data back to a string
            retrieved_message = retrieved_data.decode("utf-8")
            print(f"Message retrieved from Redis: {retrieved_message}")
        else:
            print("Failed to retrieve message from Redis.")
    except Exception as e:
        print(f"Error: {e}")


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Insert a message into Redis and then retrieve it using the key.")
#     parser.add_argument("message", type=str, help="The message to insert and retrieve from Redis")
#     args = parser.parse_args()
#
#     insert_and_retrieve_message_streams(args.message)
#     # insert_and_retrieve_message(args.message)
#
#     # Run the following command in the terminal to insert and retrieve a message:
#     # python sample_insert_and_retrieve_message.py 'Hello, Redis!'
