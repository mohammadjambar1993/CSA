import asyncio
import logging
import os
from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional

import pytz
from bleak import BleakClient, BleakScanner
from mbst.core.basic_definitions.device_types import DeviceType
from mbst.core.common_tools.custom_decorators import override, retry
from mbst.core.common_tools.logger import LogManager
from mbst.core.common_tools.raised_errors import (
    ConfigurationError,
    CustomError,
    OperationError,
    UnexpectedDisconnectionError,
)

from ...ble_management.ble_utils import *

local_tz = pytz.timezone("America/Toronto")


class BLEDevice:
    def __init__(
        self,
        loop=None,
        device_address=None,
        device=None,
        cwd=None,
        callback_func=None,
        timezone=local_tz,
        logger=None,
    ):
        self._loop = loop
        self.logger = logger or logging.getLogger()
        self.device = device
        self.device_address = device_address
        self.callback_func = callback_func
        self.client = BleakClient(self.device_address, loop=self._loop)
        self.is_connected = False
        # self.sensor_device = DeviceType.BLE_DEVICE_NAME
        self.sensor_device = DeviceAttributeManager(BLE_DEVICE_NAME)
        self.cwd = os.getcwd()
        self.chars = None
        self.op_codes = None
        self.systems = None

        self.timezone = timezone

    async def scan_for_devices(self, scan_time=5.0, scan_attempts=3, service_uuid=None):
        attempt = 1
        target_uuid = (
            SERVICES_UUIDS.get(self.device) if self.device else service_uuid
        )  # Use specific UUID if device specified
        while attempt <= scan_attempts:
            logging.info(
                f"[BLEDevice][scan_for_devices] Attempt {attempt}/{scan_attempts}: Scanning for devices..."
            )
            try:
                devices = await BleakScanner.discover(scan_time)
                # Filter devices: if target_uuid exists, filter by that; otherwise, scan for all known UUIDs
                filtered_devices = [
                    device
                    for device in devices
                    if device.metadata
                    and "uuids" in device.metadata
                    and (
                        target_uuid.lower()
                        in [s.lower() for s in (device.metadata.get("uuids") or [])]
                        if target_uuid
                        else any(
                            uuid.lower()
                            in [s.lower() for s in (device.metadata.get("uuids") or [])]
                            for uuid in SERVICES_UUIDS.values()
                        )
                    )
                ]
                device_addresses = [device.address for device in filtered_devices]
                if filtered_devices:
                    self.sensor_device.device_addresses = device_addresses
                    # Determine modality based on UUID if device is not specified
                    if not self.device:
                        found_uuid = filtered_devices[0].metadata.get("uuids", [])
                        for modality, uuid in SERVICES_UUIDS.items():
                            if uuid.lower() in [s.lower() for s in found_uuid]:
                                self.sensor_device.device_modality = modality
                                break
                        else:
                            self.sensor_device.device_modality = "unknown_device_type"
                    else:
                        # If device was specified, use that as the modality
                        self.sensor_device.device_modality = self.device
                    logging.info(
                        f"[BLEDevice][scan_for_devices] Found {self.sensor_device.device_modality} device at {device_addresses[0]}"
                    )
                    return (
                        self.sensor_device.device_addresses,
                        self.sensor_device.device_modality,
                    )
                logging.warning(
                    f"[BLEDevice][scan_for_devices] No devices found on attempt {attempt}. Retrying..."
                )
            except Exception as e:
                logging.error(
                    f"[BLEDevice][scan_for_devices] Error during scanning: {e}"
                )
            attempt += 1
            await asyncio.sleep(2)
        logging.error(
            f"[BLEDevice][scan_for_devices] Device {self.device} {self.device_address} not found after maximum attempts. Please check the device."
        )
        return [], None

    # TODO SPREV - Code will get stuck in connect - need a way to break this and communicate back to user
    # async def connect(self):
    #     while not self.client.is_connected:
    #         try:
    #             await self.client.connect()
    #             self.sensor_device.flags.is_connected = True
    #             logging.info(
    #                 f"[BLEDevice][connect] Connected to {self.device} device: {self.device_address}"
    #             )
    #         except Exception as e:
    #             logging.info(
    #                 f"[BLEDevice][connect] Device not found, attempting to reconnect. Exception: {e}"
    #             )

    @retry(retries=3, delay=2)
    async def connect(self):
        if not self.client.is_connected:
            try:
                logging.info(
                    f"[BLEDevice][connect] Attempting to connect to {self.device_address}..."
                )
                await self.client.connect()
                self.sensor_device.flags.is_connected = True
                print(
                    f"[BLEDevice][connect] Connected to {self.device} at {self.device_address}"
                )
            except asyncio.CancelledError:
                logging.warning("[BLEDevice][connect] Task was cancelled.")
                raise
            except Exception as e:
                logging.error(f"[BLEDevice][connect] Connection failed: {e}")
                raise OperationError(
                    f"[BLEDevice][connect] Could not connect to {self.device_address}"
                )

    async def initialize_device(self):
        if self.client.is_connected:
            try:
                asyncio.ensure_future(self.activate_command_response(), loop=self._loop)
                await self.retrieve_module_info()
                logging.info(
                    f"[BLEDevice][initialize_device] {self.device} Device {self.device_address} Initialized"
                )
            except Exception as e:
                logging.warning(
                    f"[BLEDevice][initalize_device] Device {self.device_address} Initialization Failed or Incomplete - {e}."
                )
                self.sensor_device.flags.is_connected = False
                self.sensor_device.flags.device_initialized = False
        else:
            logging.error(
                f"[BLEDevice][initialize_device] Device disconnected: {self.device}: {self.device_address}"
            )
            raise UnexpectedDisconnectionError(
                f"[BLEDevice][initialize_device] Device {self.device}: {self.device_address} disconnected."
            )

    async def activate_command_response(self):
        if not self.client.is_connected:
            logging.error(
                f"[BLEDevice][activate_command_response] Device {self.device}: {self.device_address} not connected. Cannot start command response "
                "notifications."
            )
            raise UnexpectedDisconnectionError(
                f"[BLEDevice][activate_command_response] Device {self.device}: {self.device_address} not connected. Cannot start command response "
            )
        try:
            if self.RESPONSE is not None:  # type: ignore[attr-defined]
                await self.client.start_notify(
                    self.RESPONSE, self.response_notification_handler_parent  # type: ignore[attr-defined]
                )
                logging.info(
                    f"[BLEDevice][activate_command_response] Command response notifications started for device {self.device_address}."
                )
        except Exception as e:
            logging.error(
                f"[BLEDevice][activate_command_response] Error starting notifications for device {self.device_address}. Error: {e}"
            )
            raise OperationError(
                f"[BLEDevice][activate_command_response] Error activating command response for device {self.device_address}"
            )

    async def retrieve_module_info(self):
        try:
            if self.INFORMATION is not None:  # type: ignore[attr-defined]
                module_info = await self.client.read_gatt_char(self.INFORMATION)  # type: ignore[attr-defined]
            fw_version, battery_level = module_parser(list(module_info))
            logging.info(
                f"[BLEDevice][retrieve_module_info] Address: {self.device_address} - FW version: {fw_version} Battery Level: {battery_level}"
            )
            self.sensor_device.firmware = fw_version
            self.sensor_device.battery = battery_level
            await self.read_sensor_config()
        except Exception as e:
            logging.error(
                f"[BLEDevice][retrieve_module_info] Error during retrieve_module_info for device {self.device_address}: {e}"
            )

    async def start_acquisition(self):
        try:
            self.clear_data()
            self.generate_csv_files()

            await self.start_read()
            await self.activate_services()
        except Exception as e:
            logging.error(
                f"[BLEDevice][start_acquisition] Error while starting acquisition for address {self.device_address} - {e}"
            )
            raise OperationError(
                "[BLEDevice][start_acquisition] Error while starting acquisition"
            )

    async def activate_services(self):
        try:
            if self.INFORMATION is not None:  # type: ignore[attr-defined]
                await self.client.start_notify(
                    self.INFORMATION, lambda sender, data: print("Services activated.")  # type: ignore[attr-defined]
                )
                logging.info(
                    f"[BLEDevice][activate_services] Started notifications for Information for device {self.device}:{self.device_address}"
                )
        except Exception as e:
            logging.error(
                f"[BLEDevice][activate_services] Error activating services for device {self.device}:{self.device_address} - {e}"
            )
            raise OperationError(
                f"[BLEDevice][start_acquisition] Error while starting acquisition for {self.device}: {self.device_address}"
            )

    def generate_dynamic_payload(
        self,
        opcode: list,
        sampling_rate: float = None,
        feedback_capacitor: str = None,
        system: str = None,
    ) -> str:
        """
        :param opcode: Name of the command operation (e.g., "CONFIG_SENSOR")
        :param sampling_rate: The sampling rate to be encoded in the payload
        :param feedback_capacitor: Optional feedback capacitor setting; only used for sampling rate configuration.
        :param system: The system name from SYSTEMS (e.g., "PRESSURE")
        :return: Hexadecimal string of the payload.
        """
        opcode_bytes = bytes(opcode)
        opcode_hex = generate_opcode(opcode_bytes)
        system_hex = system if system else ""
        if self.chars is not None:
            feedback_cap_hex = (
                self.chars["CAPACITORS"].get(feedback_capacitor, "")  # type: ignore[attr-defined]
                if feedback_capacitor
                else ""
            )
        sampling_rate_hex = (
            float_to_hex(sampling_rate) if sampling_rate is not None else ""
        )
        payload = f"{opcode_hex}{system_hex}{sampling_rate_hex}{feedback_cap_hex}"
        # print(f"Generated Payload: {payload}")
        return payload

    async def configure(self, payload_sr):
        try:
            await self.stop_all_notifications()

            if self.client.is_connected:
                logging.info(
                    "[BLEDevice][configure] "
                    + " Device Call to write gatt char Function: "
                    + str(datetime.now())
                    + " With Payload: "
                    + str(payload_sr)
                )
                if self.chars is not None:
                    await self.client.write_gatt_char(
                        self.chars["COMMAND"], bytearray.fromhex(payload_sr), False  # type: ignore[attr-defined]
                    )
                await self.read_sensor_config()
                logging.info(
                    "[BLEDevice][configure] "
                    + " Call completed to write gatt char Function: "
                    + str(datetime.now())
                )
                await self.start_read()
                await self.activate_services()

            else:
                logging.info(
                    f"[BLEDevice][configure] Device is no longer connected to application {self.device} : {self.device_address}"
                )
                raise UnexpectedDisconnectionError(
                    f"[BLEDevice][configure] Failed to configure device due to "
                    f"unexpected Device disconnection {self.device_address}"
                )
        except Exception as e:
            logging.error(f"[BLEDevice][configure] Error Thrown: {e} ")
            raise ConfigurationError(
                f"[BLEDevice][configure] Error Configuring Device {self.device_address}"
            )

    def response_notification_handler_parent(self, sender, data):
        if self.client.is_connected:
            asyncio.ensure_future(
                self.response_notification_handler(sender, data), loop=self._loop
            )

    async def response_notification_handler(self, sender, data):
        try:
            # print("Response Data: ", data)
            logging.debug(
                f"[BLEDevice][response_notification_handler] {self.device_address} Response Data "
                + str(data)
            )
            opText, status, system, data = response_parser(data)
            switch = {
                0: " command was successful",
                1: " command failed, reason: invalid opcode",
                2: " command failed, reason: invalid arguments",
                3: " command failed, reason: invalid command",
                4: " command failed, reason: invalid argument",
                5: " command failed, reason: command error",
                6: " command failed, reason: invalid state",
            }
            msg = str(opText) + switch.get(status or 0, "Invalid Status")  # type: ignore[operator]
            logging.info("[BLEDevice][response_notification_handler] " + str(msg))
            logging.info(
                f"[BLEDevice][response_notification_handler] Returned Data for - {opText or 'Unknown'}: {system} {data}"
            )  # type: ignore[operator]
        except Exception as e:
            logging.info(
                f"[BLEDevice][response_notification_handler] Error Thrown for {self.device_address}: {e} "
            )

    def generate_csv_files(self):
        self.sensor_device.csv.file_name_csv = self.generate_csv_file_name("pressure")
        self.sensor_device.csv.file_name_shoe_press = self.generate_csv_file_name(
            "pressure_shoe"
        )
        self.sensor_device.csv.file_name_imu = self.generate_csv_file_name("IMU")
        self.sensor_device.csv.file_name_sflp = self.generate_csv_file_name("SFLP")
        logging.info(
            f"[BLEDevice][start_acquisition] Pressure CSV file name: {self.sensor_device.csv.file_name_shoe_press}"
        )
        logging.info(
            f"[BLEDevice][start_acquisition] IMU CSV file name: {self.sensor_device.csv.file_name_imu}"
        )
        logging.info(
            f"[BLEDevice][start_acquisition] SFLP CSV file name: {self.sensor_device.csv.file_name_sflp}"
        )
        logging.info(
            f"[BLEDevice][start_acquisition] Generated CSV file name for {self.device} address {self.device_address}"
        )

    def generate_csv_file_name(self, data_type: str = "pressure"):
        try:
            # device_type = self.device
            sanitized_address = self.device_address.replace(":", "")
            formatted_date = datetime.now(self.timezone).strftime("%Y-%m-%d_%H-%M-%S")
            unix_time = str(int(datetime.now(self.timezone).timestamp() * 1000))

            sub_folder_path = os.path.join(
                self.cwd, "sessions", sanitized_address, formatted_date
            )
            os.makedirs(sub_folder_path, exist_ok=True)
            file_name = os.path.join(
                sub_folder_path,
                f"{data_type}_{sanitized_address}_{unix_time}.csv",
            )
            logging.info(
                f"[generate_csv_file_name] Generated file name for {data_type}: {file_name}"
            )

            if data_type == "pressure":
                self.sensor_device.csv.file_name_csv = file_name
            elif data_type == "pressure_shoe":
                self.sensor_device.csv.file_name_shoe_press = file_name
            elif data_type == "IMU":
                self.sensor_device.csv.file_name_imu = file_name
            elif data_type == "SFLP":
                self.sensor_device.csv.file_name_sflp = file_name
            else:
                raise ValueError(f"Unknown data type: {data_type}")
            return file_name
        except Exception as e:
            logging.error(
                f"[generate_csv_file_name] Error generating file name for {data_type}: {e}"
            )
            raise OperationError(f"Failed to generate CSV file name for {data_type}.")

    def record_time_info(self):
        try:
            timestamp_python = str(int(datetime.now(self.timezone).timestamp() * 1000))
            return timestamp_python
        except Exception as e:
            logging.error(f"[record_time_info] Error in recording timestamp: {e}")
            return None

    async def stop_acquisition(self):
        if not self.client.is_connected:
            logging.warning(
                "[BLEDevice][stop_acquisition] Device not connected. Nothing to stop."
            )
            return
        try:
            await self.stop_all_notifications()
            await self.force_save()
            logging.info(
                f"[BLEDevice][stop_acquisition] Acquisitions Stopped and Data Saved for {self.device}:{self.device_address}"
            )
        except Exception as e:
            logging.error(
                f"[BLEDevice][stop_acquisition] {self.device_address} Error stopping acquisition: {e}"
            )

    async def stop_all_notifications(self):
        try:
            if not self.client.is_connected:
                logging.warning(
                    "[BLEDevice][stop_all_notifications] Device already disconnected, skipping stop notifications."
                )
                return
            logging.info(
                "[BLEDevice][stop_all_notifications] Stopping all Services notification..."
                + " at "
                + str(datetime.now())
            )

            # Check and stop information characteristic notifications
            if self.sensor_device.flags.information_notifications_started:
                if self.chars is not None:
                    await self.client.stop_notify(self.chars["INFORMATION"])
                logging.info(
                    "[BLEDevice][stop_all_notifications] Stopped notifications for information_char_id."
                )
                self.sensor_device.flags.information_notifications_started = False

            logging.info(
                "[BLEDevice][stop_all_notifications] All notifications stopped at "
                + str(datetime.now())
            )
        except Exception as e:
            logging.error(f"[BLEDevice][stop_all_notifications] Error Thrown: {e}")
            raise OperationError(
                f"[BLEDevice][stop_all_notifications] Error stopping notifications for device {self.device_address}"
            )

    async def force_save(self):
        self.write_to_csv()
        self.clear_data()

    async def disconnect(self):
        if not self.client.is_connected:
            logging.info("[BLEDevice][disconnect] Device is already disconnected.")
            return
        try:
            logging.info(
                f"[BLEDevice][disconnect] Disconnecting from device {self.device}:{self.device_address}."
            )
            await self.stop_acquisition()
            await self.client.disconnect()
            self.sensor_device.flags.is_connected = False
            logging.info(
                f"[BLEDevice][disconnect] Client disconnected and session finished for {self.device}:{self.device_address}."
            )
        except Exception as e:
            logging.error(f"[BLEDevice][disconnect] Error during disconnection: {e}")
            raise OperationError(
                f"[BLEDevice][disconnect] Error during disconnection: {e}"
            )

    async def shutdown(self):
        logging.info("[BLEDevice][shutdown] Initiating shutdown sequence.")
        await self.disconnect()
        self.clear_data()
        logging.info(
            f"[BLEDevice][shutdown] Device shut down completed for {self.device_address}."
        )

    async def start_read(self):
        """Placeholder for starting data reading - to be implemented by subclasses."""
        raise NotImplementedError(
            f"[{self.device}] start_read implemented in Device-specific code"
        )

    def write_to_csv(self):
        raise NotImplementedError(
            f"[{self.device}] write_to_csv implemented in Device-specific code"
        )

    def clear_data(self):
        raise NotImplementedError(
            f"[{self.device}] clear_data implemented in Device-specific code"
        )

    async def read_sensor_config(self):
        raise NotImplementedError(
            f"[{self.device}] read_sensor_config implemented in Device-specific code"
        )

    async def sensor_ble_configuration(self):
        raise NotImplementedError(
            f"[{self.device}] sensor_ble_config implemented in Device-specific code"
        )
