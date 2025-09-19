import asyncio
import logging
import os
import sys



from mbst.bleusb_comm_toolkit import BLEDevice, create_ble_device
from mbst.core.common_tools.logger import LogManager


device_instance = None


async def handle_incoming_data(
    data, device=None, device_addr=None, char=None, timestamp=None
):
    if isinstance(data, list):
        print("Callback received data: ", data)
        # msg = await producer_json(data=data, url=dash_heatmap_url)
        # print(msg)
    else:
        print("Callback received data: ", data)


async def main(cwd=None):
    global device_instance
    cwd = cwd or os.getcwd()
    logger = LogManager(cwd).logger
    loop = asyncio.get_event_loop()
    device = BLEDevice(loop=loop, cwd=cwd, logger=None)

    device_addresses, modality = await device.scan_for_devices(
        scan_time=3.0, scan_attempts=1
    )
    if not device_addresses or not modality:
        logging.error("No compatible BLE devices found; exiting.")
        return
    device_instance = create_ble_device(
        device_addresses[0],
        modality,
        loop=loop,
        logger=logger,
        callback=handle_incoming_data,
    )
    try:
        print(
            f"device addr {device_addresses[0]}, modality {modality}, device type {type(device_instance)}"
        )
        await device_instance.connect()
        await device_instance.initialize_device()
        await device_instance.start_acquisition()
    except Exception as e:
        logging.error(f"Error while setting up device: {e}")
    except KeyboardInterrupt:
        logging.error("Keyboard Interrupt received, shutting down.")
        if device_instance:
            await device_instance.disconnect()
        return device_instance


def run():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Shutdown initiated.")
        if device_instance is not None:
            loop.run_until_complete(device_instance.shutdown())
        else:
            sys.exit()
    finally:
        pending_tasks = asyncio.all_tasks(loop)
        loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
        loop.close()


if __name__ == "__main__":
    run()


# import asyncio
# import logging
# import os
# import sys
# from src.mbst.bleusb_comm_toolkit import BLEDevice, create_ble_device, LogManager
#
# device_instance = None
#
#
# async def handle_incoming_data(data, device=None, device_addr=None, char=None, timestamp=None):
#     if isinstance(data, list):
#         print("Callback received data: ", data)
#         # msg = await producer_json(data=data, url=dash_heatmap_url)
#         # print(msg)
#     else:
#         print("Callback received data: ", data)
#
#
# async def main(cwd=None):
#     global device_instance
#     cwd = cwd or os.getcwd()
#     logger = LogManager(cwd).logger
#     loop = asyncio.get_event_loop()
#     device = BLEDevice(loop=loop, cwd=cwd, logger=None)
#     device_addresses, modality = await device.scan_for_devices(scan_time=3.0, scan_attempts=1)
#     if not device_addresses or not modality:
#         logging.error("No compatible BLE devices found; exiting.")
#         return
#     device_instance = create_ble_device(device_addresses[0], modality, loop=loop, logger=logger,
#                                         callback=handle_incoming_data)
#     try:
#         print(f"device addr {device_addresses[0]}, modality {modality}, device type {type(device_instance)}")
#         await device_instance.connect()
#         await device_instance.initialize_device()
#         await device_instance.start_acquisition()
#     except Exception as e:
#         logging.error(f"Error while setting up device: {e}")
#     except KeyboardInterrupt:
#         logging.error("Keyboard Interrupt received, shutting down.")
#         if device_instance:
#             await device_instance.disconnect()
#         return device_instance
#
#
# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     try:
#         loop.run_until_complete(main())
#         loop.run_forever()
#     except KeyboardInterrupt:
#         logging.info("Shutdown initiated.")
#         if device_instance is not None:
#             loop.run_until_complete(device_instance.shutdown())
#         else:
#             sys.exit()
#     finally:
#         pending_tasks = asyncio.all_tasks(loop)
#         loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
#         loop.close()
