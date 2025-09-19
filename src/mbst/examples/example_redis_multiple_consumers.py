import os
import asyncio
import logging
import pandas as pd
from mbst.bleusb_comm_toolkit import LiveConsumer
from mbst.core.common_tools.logger_setup import init_logger
from mbst.bleusb_comm_toolkit.in_memory_database.redis_functions import producer_redis_streams

class EnrichedCallbackHandler:
    def __init__(self, user_id=None, nick_name=None):
        self.user_id = user_id
        self.nick_name = nick_name

    async def __call__(self, data, device=None, device_addr=None, char=None, timestamp=None):
        await handle_incoming_data(
            data,
            device=device,
            device_addr=device_addr,
            user_id=self.user_id,
            nick_name=self.nick_name,
            char=char,
            timestamp=timestamp
        )


async def handle_incoming_data(data, device=None, device_addr=None, user_id=None, nick_name=None, char=None, timestamp=None):
    try:
        if isinstance(data, list):
            for item in data:
                producer_redis_streams(
                    value=str(item),
                    device=device or "undefined",
                    device_id=device_addr or "0000",
                    uid=user_id,
                    nn=nick_name,
                    char=char or "unknown"
                )
        elif isinstance(data, pd.DataFrame):
            for _, row in data.iterrows():
                producer_redis_streams(
                    value=row.to_json(),
                    device=device or "undefined",
                    device_id=device_addr or "0000",
                    uid=user_id,
                    nn=nick_name,
                    char=char or "unknown"
                )
        else:
            producer_redis_streams(
                value=str(data),
                device=device or "undefined",
                device_id=device_addr or "0000",
                uid=user_id,
                nn=nick_name,
                char=char or "unknown"
            )
    except Exception as e:
        logging.error(f"[Redis Stream Multiple][handle_incoming_data] Failed to send data to Redis: {e}")


async def shutdown(consumers, logger):
    for consumer in consumers:
        await consumer.stop_stream_data()
        await consumer.disconnect()
    logger.info("[Redis Stream Multiple][shutdown] All consumers shut down.")


async def main(loop, user_id, nick_name, logger):
    consumers = []

    # Temporary consumer just to scan
    scanner = LiveConsumer(
        loop=loop,
        connection="ble",
        device="insole",
        cwd=os.getcwd(),
        callback=lambda *args, **kwargs: None
    )
    device_addresses, modality = await scanner.scan_for_devices()
    logger.info(f"[Redis Stream Multiple] Found devices: {device_addresses}, modality: {modality}")

    for addr in device_addresses:
        callback = EnrichedCallbackHandler(user_id=user_id, nick_name=nick_name)
        consumer = LiveConsumer(
            loop=loop,
            connection="ble",
            device="insole",
            cwd=os.getcwd(),
            callback=callback,
            device_id=addr,
            user_id=user_id,
            nick_name=nick_name,
        )
        consumer.select_device(addr, modality)
        await consumer.activate_device()
        consumers.append(consumer)
        print(f"[Redis Stream Multiple] Activated consumer for: {addr}")

    await asyncio.gather(*(c.stream_data() for c in consumers))
    return consumers


def run(device_id=None, user_id=None, nick_name=None):
    loop = asyncio.get_event_loop()
    logger = init_logger(os.getcwd())

    try:
        logger.info(f"[Redis Stream Multiple][run] Streaming for user {user_id}, nick {nick_name}, device {device_id}")
        consumers = loop.run_until_complete(main(loop, user_id, nick_name, logger))
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("[Redis Stream Multiple] Shutdown initiated via KeyboardInterrupt")
        loop.run_until_complete(shutdown(consumers, logger))
    finally:
        loop.close()


if __name__ == "__main__":
    run()
