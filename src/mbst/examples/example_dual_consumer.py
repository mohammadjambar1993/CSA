import asyncio
import os


from mbst.bleusb_comm_toolkit import LiveConsumer, producer_json
from mbst.core.common_tools.logger_setup import init_logger


dash_stream_url = "http://localhost:8050/update-stream-data"
dash_heatmap_url = "http://localhost:8050/update-heatmap-data"


async def handle_incoming_data(
    data, device=None, device_addr=None, char=None, timestamp=None
):
    if isinstance(data, list):
        print(f"Callback received data from {device_addr}: ", data)
        # msg = await producer_json(data=data, url=dash_heatmap_url)
        # print(msg)
    else:
        print(f"Callback received data from {device_addr}: ", data)


async def shutdown(consumer_1, consumer_2):
    await consumer_1.stop_stream_data()
    await consumer_2.stop_stream_data()
    logger.info("[Example BLE] - Shutdown Complete.")


async def main():
    try:
        if consumer_1.conn_type == "ble" and consumer_2.conn_type == "ble":
            device_addresses, modality = await consumer_1.scan_for_devices()
            print(
                f"Scanned devices for consumer_1: {device_addresses}, modality: {modality}"
            )
            consumer_1.select_device(device_addresses[0], modality)
            print(f"Selected device for consumer_1: {device_addresses[0]}")

            # device_addresses, modality = await consumer_2.scan_for_devices()
            # print(f"Scanned devices for consumer_2: {device_addresses}, modality: {modality}")
            consumer_2.select_device(device_addresses[1], modality)
            print(f"Selected device for consumer_2: {device_addresses[1]}")

        await consumer_1.activate_device()
        await consumer_2.activate_device()

        await consumer_1.stream_data()
        await consumer_2.stream_data()

    except KeyboardInterrupt:
        logger.info("[Example USB] - Stop Monitoring...")
        await consumer_1.stop_stream_data()
        await consumer_2.stop_stream_data()
        await consumer_1.disconnect()
        await consumer_2.disconnect()


# async def main():
#     try:
#         if consumer_1.conn_type and consumer_2.conn_type == "ble":
#             device_addresses, modality = await consumer_1.scan_for_devices()
#             print(f"Scanned devices for consumer_1: {device_addresses}, modality: {modality}")
#
#             device_addresses, modality = await consumer_2.scan_for_devices()
#             print(f"Scanned devices for consumer_2: {device_addresses}, modality: {modality}")
#
#             # Pair each address with the same modality
#             device_list = [(address, modality) for address in device_addresses]
#
#             # Initialize all devices
#             for device_address, modality in device_list:
#                 try:
#                     consumer_1.select_device(device_address, modality)
#                     await consumer_1.activate_device()
#                     consumer_2.select_device(de)
#                     logger.info(f"Device {device_address} with modality {modality} activated.")
#                 except Exception as e:
#                     logger.error(f"Failed to activate device {device_address}: {e}")
#
#         # Start streaming data from all devices
#         await consumer_1.stream_data()
#         await consumer_2.stream_data()
#
#     except KeyboardInterrupt:
#         logger.info("[Example BLE] - Stop Monitoring...")
#         await consumer_1.stop_stream_data()
#         await consumer_2.stop_stream_data()
#         await consumer_1.disconnect()
#         await consumer_2.disconnect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    logger = init_logger(os.getcwd())
    consumer_1 = None
    consumer_2 = None

    try:
        consumer_1 = LiveConsumer(
            loop=loop,
            connection="ble",
            device="mat",
            cwd=os.getcwd(),
            callback=handle_incoming_data,
        )
        consumer_2 = LiveConsumer(
            loop=loop,
            connection="ble",
            device="mat",
            cwd=os.getcwd(),
            callback=handle_incoming_data,
        )
        loop.run_until_complete(main())
        loop.run_forever()

    except KeyboardInterrupt:
        logger.info("[Example BLE] - Shutdown Initiated")
        loop.run_until_complete(shutdown(consumer_1, consumer_2))
    except Exception as e:
        print(f"Exception Thrown {e}")

    finally:
        loop.close()
