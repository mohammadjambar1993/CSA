import asyncio
import logging
import os

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
        logging.error(
            f"[Redis Stream][handle_incoming_data] Failed to send data to Redis: {e}"
        )


async def shutdown(consumer, logger):
    await consumer.stop_stream_data()
    logger.info("[Redis Stream][shutdown] Shutdown complete.")


async def main(consumer, logger):
    try:
        if consumer.conn_type == "ble":
            device_addresses, modality = await consumer.scan_for_devices()
            consumer.select_device(device_addresses[0], modality)
            print(f"device addr {device_addresses[0]}, modality {modality}")

        await consumer.activate_device()
        await consumer.stream_data()

    except Exception as e:
        logger.error(f"[Redis Stream][main] Error streaming data: {e}")

    except KeyboardInterrupt:
        logger.info("[Redis Stream][main] Stop monitoring...")
        await consumer.stop_stream_data()
        await consumer.disconnect()


def run(device_id=None, user_id=None, nick_name=None):
    loop = asyncio.get_event_loop()
    logger = init_logger(os.getcwd())
    try:
        logger.info(f"[Redis Stream][run] Starting for user {user_id}, nick {nick_name}, device {device_id}")

        callback_handler = EnrichedCallbackHandler(user_id=user_id, nick_name=nick_name)

        consumer = LiveConsumer(
            loop=loop,
            connection="ble",
            device="pressure_bed",
            cwd=os.getcwd(),
            callback=callback_handler,
            device_id=device_id,
            user_id=user_id,
            nick_name=nick_name,

        )

        try:
            loop.run_until_complete(main(consumer, logger))
            loop.run_forever()
        except KeyboardInterrupt:
            logger.info("[Redis Stream] Shutdown initiated")
            loop.run_until_complete(shutdown(consumer, logger))
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"[Redis Stream][main] Error streaming to Redis: {e}")


if __name__ == "__main__":
    run()
