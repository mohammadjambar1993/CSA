import asyncio
import logging

from mbst.core.basic_definitions import DeviceType
from mbst.core.common_tools.raised_errors import ConfigurationError, DeviceNotFoundError

from ..ble_management import SERVICES_UUIDS, BLEDevice, create_ble_device
from ..usb_communication.usb_comm import SerialCommunication


class LiveConsumer:
    def __init__(
        self,
        loop=None,
        cwd=None,
        connection=None,
        device=None,
        config=None,
        callback=None,
        comport="COM3",
        device_id=None,
        user_id=None,
        nick_name=None,
    ):
        self._loop = loop
        self.cwd = cwd
        self.conn_type = connection
        self.device = device
        # self.device_addr = None
        self.device_id = (device_id,)
        self.user_id = (user_id,)
        self.nick_name = (nick_name,)
        self.logger = logging.getLogger()
        self.callback_functions = callback
        self.device_instance = None
        self.initialize_device(comport)

    def initialize_device(self, comport):
        if self.conn_type:
            if self.conn_type == "ble":
                self.device_instance = BLEDevice(loop=self._loop, cwd=self.cwd)
                self.logger.info("[LiveConsumer] BLE Device Initialized")
            elif self.conn_type == "usb":
                self.device_instance = SerialCommunication(
                    port=comport,
                    device=self.device,
                    loop=self._loop,
                    consumer_callback=self.new_data,
                )
                self.logger.info("[LiveConsumer] USB Device Initialized")
            else:
                raise ConfigurationError(
                    "No Device Configuration Detected. Initialize LiveConsumer argument device as 'ble' or 'usb'"
                )

    async def scan_for_devices(self, service_uuid=None):
        if self.device_instance is None:
            raise RuntimeError("Device instance has not been initialized yet.")
        if self.conn_type == "ble":
            device_addresses, modality = await self.device_instance.scan_for_devices(
                scan_time=3.0,
                scan_attempts=1,
                service_uuid=SERVICES_UUIDS.get(self.device),
            )
            if not device_addresses or not modality:
                logging.error(
                    "[LiveConsumer] No compatible BLE devices found; exiting."
                )
                raise DeviceNotFoundError("No compatible BLE devices found; exiting.")
            else:
                return device_addresses, modality

    def select_device(self, device_addr, modality):
        if self.conn_type == "ble":
            self.device_instance = create_ble_device(
                device_addr, modality, loop=self._loop, callback=self.new_data
            )

    async def activate_device(self):
        if self.device_instance is None:
            raise RuntimeError("Device instance has not been initialized yet.")
        if self.conn_type == "usb":
            self.device_instance.usb_device.end_of_acquisition()
            self.device_instance.connect()

            if not self.device_instance.connection_status:
                self.logger.error(
                    "[LiveConsumer] USB Connection failed. Check Selected COM Port!"
                )
            else:
                self.logger.info("[LiveConsumer] USB COM Port Connected")
                if self.device_instance is None:
                    raise RuntimeError("Device instance has not been initialized yet.")
                self.device_instance.enable_acquisition()
                asyncio.ensure_future(self.stream_data(), loop=self._loop)
                # await self.serial.command_response()
        elif self.conn_type == "ble":
            if self.device_instance is None:
                raise ConfigurationError(
                    "Configuration Error. BLE Device not Configured. Scan for devices and Select Device to activate device"
                )

            await self.device_instance.connect()
            await self.device_instance.initialize_device()
        else:
            raise ConfigurationError(
                "No Device Configuration Detected. Initialize LiveConsumer argument device as 'ble' or 'usb'"
            )

    async def stream_data(self):
        if self.conn_type == "usb":
            if self.device_instance is None:
                raise RuntimeError("Device instance has not been initialized yet.")
            await self.device_instance.start_notify()
        elif self.conn_type == "ble":
            if self.device_instance is None:
                raise RuntimeError("Device instance has not been initialized yet.")
            await self.device_instance.start_acquisition()
        else:
            raise ConfigurationError(
                "No Device Configuration Detected. Initialize LiveConsumer argument device as 'ble' or 'usb'"
            )

    async def stop_stream_data(self):
        if self.conn_type == "usb":
            if self.device_instance is None:
                raise RuntimeError("Device instance has not been initialized yet.")
            self.device_instance.disable_acquisition()
            self.device_instance.usb_device.end_of_acquisition()
        elif self.conn_type == "ble":
            if self.device_instance is None:
                raise RuntimeError("Device instance has not been initialized yet.")
            await self.device_instance.stop_acquisition()
        else:
            raise ConfigurationError(
                "No Device Configuration Detected. Initialize LiveConsumer argument device as 'ble' or 'usb'"
            )

    async def disconnect(self):
        if self.conn_type == "usb":
            if self.device_instance is None:
                raise RuntimeError("Device instance has not been initialized yet.")
            self.device_instance.disconnect()
        elif self.conn_type == "ble":
            if self.device_instance is None:
                raise RuntimeError("Device instance has not been initialized yet.")
            await self.device_instance.disconnect()
        else:
            raise ConfigurationError(
                "No Device Configuration Detected. Initialize LiveConsumer argument device as 'ble' or 'usb'"
            )

    async def new_data(
        self, data, device=None, device_addr=None, char=None, timestamp=None
    ):
        # print(data)
        await self.callback_functions(data, device, device_addr, char, timestamp)
        # return data
