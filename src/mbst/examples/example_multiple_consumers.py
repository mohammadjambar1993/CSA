import os
import asyncio
from mbst.bleusb_comm_toolkit import LiveConsumer, producer_json
from mbst.core.common_tools.logger_setup import init_logger


async def handle_incoming_data(data, device=None, device_addr=None, char=None, timestamp=None):
    if isinstance(data, list):
        print(f"Callback received data from {device_addr}: ", data)
        # msg = await producer_json(data=data, url=dash_heatmap_url)
        # print(msg)
    else:
        print(f"Callback received data from {device_addr}: ", data)

async def shutdown(consumers, logger):
    for consumer in consumers:
        await consumer.stop_stream_data()
        await consumer.disconnect()
    logger.info("[Example Multiple Consumers] - Shutdown Complete.")

async def main(loop):
    consumers = []

    # Use a temporary consumer just to scan
    temp_consumer = LiveConsumer(
        loop=loop,
        connection="ble",
        device="mat",
        cwd=os.getcwd(),
        callback=handle_incoming_data
    )
    device_addresses, modality = await temp_consumer.scan_for_devices()
    print(f"Scanned devices: {device_addresses}, modality: {modality}")

    # For each device, create a LiveConsumer
    for addr in device_addresses:
        consumer = LiveConsumer(
            loop=loop,
            connection="ble",
            device="mat",
            cwd=os.getcwd(),
            callback=handle_incoming_data
        )
        consumer.select_device(addr, modality)
        await consumer.activate_device()
        consumers.append(consumer)
        print(f"Selected and activated device: {addr}")

    # Stream from all consumers concurrently
    await asyncio.gather(*(c.stream_data() for c in consumers))

    return consumers

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    logger = init_logger(os.getcwd())

    consumers = []
    try:
        consumers = loop.run_until_complete(main(loop))
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("[Example Multiple Consumers] - Shutdown Initiated")
        loop.run_until_complete(shutdown(consumers, logger))
        consumers.clear()
        logger.info("[Example Multiple Consumers] - Shutdown Complete")
        loop.close()


