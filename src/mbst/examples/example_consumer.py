import asyncio
import os

import pandas as pd

from mbst.bleusb_comm_toolkit import LiveConsumer, producer_json
from mbst.core.common_tools.logger_setup import init_logger


dash_stream_url = "http://localhost:8050/update-stream-data"
dash_heatmap_url = "http://localhost:8050/update-heatmap-data"


async def handle_incoming_data(
    data, device=None, device_addr=None, char=None, timestamp=None
):
    if isinstance(data, list):
        print("Callback received data: ", data)
        # msg = await producer_json(data=data, url=dash_heatmap_url)
        # print(msg)
    elif isinstance(data, pd.DataFrame):
        print("Callback received Dataframe data: ", data)
    else:
        print("Callback received data: ", data)


async def shutdown(consumer, logger):
    await consumer.stop_stream_data()
    logger.info("[Example USB] - Shutdown Complete.")


async def main(consumer, logger):
    try:
        if consumer.conn_type == "ble":
            device_addresses, modality = await consumer.scan_for_devices()
            consumer.select_device(device_addresses[0], modality)
            print(f"device addr {device_addresses[0]}, modality {modality}")

        await consumer.activate_device()
        await consumer.stream_data()

    except KeyboardInterrupt:
        logger.info("[Example USB] - Stop Monitoring...")
        await consumer.stop_stream_data()
        await consumer.disconnect()


def run():
    loop = asyncio.get_event_loop()
    logger = init_logger(os.getcwd())
    consumer = None

    try:
        # consumer = LiveConsumer(loop=loop, connection="usb", device="pressure_bed", cwd=os.getcwd(), callback=handle_incoming_data)
        # consumer = LiveConsumer(loop=loop, connection="ble", device="insole", cwd=os.getcwd(),
        #                         callback=handle_incoming_data)
        consumer = LiveConsumer(
            loop=loop,
            connection="ble",
            device="pressure_bed",
            cwd=os.getcwd(),
            callback=handle_incoming_data,
        )
        loop.run_until_complete(main(consumer, logger))
        loop.run_forever()

    except KeyboardInterrupt:
        logger.info("[Example USB] - Shutdown Initiated")
        loop.run_until_complete(shutdown(consumer, logger))
    except Exception as e:
        print(f"Exception Thrown {e}")

    finally:
        loop.close()


if __name__ == "__main__":
    run()
