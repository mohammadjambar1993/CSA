import asyncio
import csv
import logging
import os
import time
from datetime import datetime
from typing import Any, Coroutine, Optional

import pandas as pd
import pytz
from mbst.core.common_tools.custom_decorators import override
from mbst.core.common_tools.raised_errors import (
    ConfigurationError,
    CustomError,
    OperationError,
    UnexpectedDisconnectionError,
)
from mbst.core.device_defintions.insole_fw import InsoleFirmware

from ...ble_management.ble_utils import *
from ...ble_management.Devices import BLEDevice

data_lock = asyncio.Lock()

insole_fw = InsoleFirmware()
chars = insole_fw.chars

local_tz = pytz.timezone("America/Toronto")


class InsoleDevice(BLEDevice):
    def __init__(
        self,
        loop=None,
        device_address=None,
        cwd=None,
        timezone=local_tz,
        logger=None,
        callback_func=None,
    ):
        super().__init__(loop, device_address, callback_func=callback_func)
        self.__dict__.update(chars)
        self.__dict__.update(chars["SYSTEMS"])  # type: ignore[call-overload]
        self.__dict__.update(chars["OPCODES"])  # type: ignore[call-overload]
        self.__dict__.update(chars["CAPACITORS"])  # type: ignore[call-overload]
        self.chars = chars
        self.op_codes = chars["OPCODES"]
        self.systems = chars["SYSTEMS"]

        self.logger = logger
        self.cwd = cwd or os.getcwd()
        self.callback_func = callback_func
        self.sampling_rate = 50
        self.selected_sampling_rate = None
        self.timezone = timezone

    async def read_sensor_config(self):
        try:
            payload = self.generate_dynamic_payload(
                opcode=self.READ_SENSOR_CFG, system=self.PRESSURE  # type: ignore[attr-defined]
            )
            logging.info(
                f"[IMUDevice][read_sensor_config] Writing configuration payload: {payload} to {self.device_address}"
            )
            await self.client.write_gatt_char(
                self.COMMAND, bytearray.fromhex(payload), True  # type: ignore[attr-defined]
            )
        except Exception as e:
            logging.error(
                f"[IMUDevice][read_sensor_config] Error during read_sensor_config of {self.device_address}: {e}"
            )
            raise OperationError(
                f"[IMUDevice][read_sensor_config] Error during read_sensor_config of {self.device_address}: {e}"
            )

    async def sensor_ble_configuration(self, sampling_rate="50", cap="1000pF"):
        try:
            payloadSR = self.generate_dynamic_payload(
                opcode=self.CONFIG_SENSOR,  # type: ignore[attr-defined]
                system=self.PRESSURE,  # type: ignore[attr-defined]
                sampling_rate=float(self.sampling_rate),
                feedback_capacitor=cap,
            )
            if not payloadSR:
                logging.error(
                    "[IMUDevice][sensor_ble_configuration] Failed to generate payload."
                )
                raise ConfigurationError(
                    f"Failed to generate payload for device {self.device_address}"
                )
            logging.info(
                "[IMUDevice][sensor_ble_configuration] Configuring BLE Devices, Generated Payload: "
                + str(payloadSR)
            )
            if self.client.is_connected:
                await self.configure(payloadSR)
            else:
                raise UnexpectedDisconnectionError(
                    f"[IMUDevice][sensor_ble_configuration] Failed to Configure device {self.device}:{self.device_address} due to device disconnection"
                )
        except Exception as e:
            logging.error(
                "[IMUDevice][sensor_ble_configuration] Exception thrown " + str(e)
            )

    @override
    async def stop_all_notifications(self):
        if not self.client.is_connected:
            logging.warning(
                "[IMUDevice][stop_all_notifications] Device already disconnected, skipping stop notifications."
            )
            return
        logging.info(
            f"[IMUDevice][stop_all_notifications] Stopping all notifications at {datetime.now()}"
        )

        if self.sensor_device.flags.information_notifications_started:
            await self.client.stop_notify(chars["INFORMATION"])  # type: ignore[arg-type]
            logging.info(
                "[IMUDevice][stop_all_notifications] Stopped notifications for information_char_id."
            )
            self.sensor_device.flags.information_notifications_started = False

        if self.sensor_device.flags.imu_notifications_started:
            await self.client.stop_notify(chars["IMU"])  # type: ignore[arg-type]
            logging.info(
                f"[IMUDevice][stop_all_notifications] Stopped notifications for IMU data for device {self.device}:{self.device_address}."
            )
            self.sensor_device.flags.imu_notifications_started = False

        if self.sensor_device.flags.imu_sflp_notifications_started:
            await self.client.stop_notify(chars["IMU_SFLP"])  # type: ignore[arg-type]
            logging.info(
                f"[IMUDevice][stop_all_notifications] Stopped notifications for SFLP data for device {self.device}:{self.device_address}."
            )
            self.sensor_device.flags.imu_sflp_notifications_started = False

        # if self.sensor_device.flags.pressure_notifications_started:
        #     await self.client.stop_notify(chars["PRESSURE_PROCESSED"])  # type: ignore[arg-type]
        #     logging.info(
        #         f"[InsoleDevice][stop_all_notifications] Stopped notifications for Pressure data for device {self.device}:{self.device_address}."
        #     )
        #     self.sensor_device.flags.pressure_notifications_started = False

    async def start_read(self):
        try:
            # asyncio.ensure_future(self.read_pressure(), loop=self._loop)
            asyncio.ensure_future(self.read_imu(), loop=self._loop)
            asyncio.ensure_future(self.read_imu_sflp(), loop=self._loop)
        except Exception as e:
            logging.error(
                f"[IMUDevice][start_read] Error during start_read for {self.device_address}: {e}"
            )

    # async def read_pressure(self):
    #     if not self.client.is_connected:
    #         logging.error(
    #             f"[InsoleDevice][read_pressure] Device is not connected. Cannot start Pressure notifications for {self.device_address}"
    #         )
    #         raise UnexpectedDisconnectionError(
    #             f"[InsoleDevice][read_pressure] Device is not connected. Cannot start Pressure notifications for {self.device_address}"
    #         )
    #     try:
    #         await self.client.start_notify(
    #             self.PRESSURE_PROCESSED, self.notification_handler  # type: ignore[attr-defined]
    #         )
    #         logging.info(
    #             f"[InsoleDevicee][read_pressure] Started Pressure notifications for {self.device_address}"
    #         )
    #     except Exception as e:
    #         logging.error(
    #             f"[InsoleDevice][read_pressure] Error starting notifications for {self.device_address}: {str(e)}"
    #         )
    #
    # async def notification_handler(self, sender, data):
    #     try:
    #         parsed_data = insole_fw.parse_pressure(data)
    #         timestamp_python = self.record_time_info()
    #         parsed_data["timestamp_python"] = timestamp_python
    #
    #         async with data_lock:
    #             new_row = pd.DataFrame([parsed_data])
    #             self.sensor_device.csv.rx_press_shoe_df = pd.concat(
    #                 [self.sensor_device.csv.rx_press_shoe_df, new_row],
    #                 ignore_index=True,
    #             )
    #         self.sensor_device.shoe_pressure.data_buffer.append(parsed_data)
    #
    #         if self.callback_func:
    #             await self.callback_func(
    #                 self.sensor_device.shoe_pressure.data_buffer,
    #                 self.device,
    #                 self.device_address,
    #                 "PRESSURE_PROCESSED",
    #                 timestamp_python,
    #             )
    #             self.sensor_device.shoe_pressure.data_buffer.clear()
    #
    #         if (
    #             len(self.sensor_device.csv.rx_press_shoe_df)
    #             >= self.sensor_device.shoe_pressure.data_dump_size
    #         ):
    #             await self.write_pressure_to_csv()
    #
    #     except Exception as e:
    #         logging.error(
    #             f"[InsoleDevice][notification_handler] Error in pressure notification handler: {e}"
    #         )
    #         raise OperationError(
    #             f"[InsoleDevice][notification_handler] Error in pressure notification handler: {e}"
    #         )

    async def read_imu(self):
        if not self.client.is_connected:
            logging.error(
                f"[IMUDevice][read_imu] Device is not connected. Cannot start IMU notifications for {self.device_address}."
            )
            raise UnexpectedDisconnectionError(
                f"[IMUDevice][read_imu] Device is not connected. Cannot start IMU notifications for {self.device_address}."
            )
        try:
            await self.client.start_notify(
                self.IMU_ACCGYRO, self.imu_notification_handler  # type: ignore[attr-defined]
            )
            logging.info(
                f"[IMUDevice][read_imu] Started IMU notifications for {self.device_address}"
            )
        except Exception as e:
            logging.error(
                f"[IMUDevice][read_imu] Error starting notifications for {self.device_address}: {str(e)}"
            )
            raise OperationError(
                f"[IMUDevice][read_imu] Error starting notifications for {self.device_address}: {str(e)}"
            )

    # TODO fix csv writing logic -> use buffers (similar to pressure)
    async def imu_notification_handler(self, sender, data):
        try:
            parsed_data = insole_fw.parse_imu(data)
            timestamp_python = self.record_time_info()
            parsed_data["timestamp_python"] = timestamp_python

            async with data_lock:
                self.sensor_device.csv.rx_imu_df = pd.concat(
                    [self.sensor_device.csv.rx_imu_df, pd.DataFrame([parsed_data])],
                    ignore_index=True,
                )
            self.sensor_device.imu.data_buffer.append(parsed_data)
            self.sensor_device.imu.data_df.append(
                [
                    parsed_data["timestamp_python"],
                    parsed_data["ts_board"],
                    parsed_data["packet_counter"],
                    parsed_data["imu_data"],
                ]  # type: ignore[arg-type]
            )

            if self.callback_func:
                await self.callback_func(
                    self.sensor_device.imu.data_buffer,
                    self.device,
                    self.device_address,
                    "IMU_ACCGYRO",
                    timestamp_python,
                )
                self.sensor_device.imu.data_buffer.clear()

            if len(self.sensor_device.imu.data_df) >= 375:
                if self.callback_func:
                    await self.callback_func(
                        pd.DataFrame(self.sensor_device.imu.data_df),
                        self.device,
                        self.device_address,
                        "IMU_ACCGYRO",
                        timestamp_python,
                    )
                    self.sensor_device.imu.data_df.clear()

            if (
                len(self.sensor_device.csv.rx_imu_df)
                >= self.sensor_device.imu.data_dump_size
            ):
                await self.write_imu_to_csv()

        except Exception as e:
            logging.error(
                f"[IMUDevice][imu_notification_handler] Error in IMU notification handler for {self.device_address}: {e}"
            )
            raise OperationError(
                f"[IMUDevice][imu_notification_handler] Error in IMU notification handler for {self.device_address}: {e}"
            )

    async def read_imu_sflp(self):
        if not self.client.is_connected:
            logging.error(
                f"[IMUDevice][read_imu_sflp] Device is not connected. Cannot start SFLP notifications for {self.device_address}."
            )
            raise UnexpectedDisconnectionError(
                f"[IMUDevice][read_imu_sflp] Device is not connected. Cannot start SFLP notifications for {self.device_address}."
            )
        try:
            await self.client.start_notify(
                self.IMU_SFLP, self.imu_sflp_notification_handler  # type: ignore[attr-defined]
            )
            logging.info(
                f"[IMUDevice][read_imu_sflp] Started SFLP notifications for {self.device_address}"
            )
        except Exception as e:
            logging.error(
                f"[IMUDevice][read_imu_sflp] Error starting notifications for {self.device_address}: {str(e)}"
            )
            raise OperationError(
                f"[IMUDevice][read_imu_sflp] Error starting notifications for {self.device_address}: {str(e)}"
            )

    async def imu_sflp_notification_handler(self, sender, data):
        try:
            parsed_data = insole_fw.parse_imu_sflp(data)

            timestamp_python = self.record_time_info()
            parsed_data["timestamp_python"] = timestamp_python

            async with data_lock:
                new_row = pd.DataFrame([parsed_data])
                self.sensor_device.csv.rx_imu_sflp_df = pd.concat(
                    [self.sensor_device.csv.rx_imu_sflp_df, new_row], ignore_index=True
                )
            self.sensor_device.imu_sflp.data_buffer.append(parsed_data)
            self.sensor_device.imu_sflp.data_df.append(
                [
                    parsed_data["timestamp_python"],
                    parsed_data["ts_board"],
                    parsed_data["packet_counter"],
                    parsed_data["sflp_data"],
                ]
            )

            if self.callback_func:
                await self.callback_func(
                    self.sensor_device.imu_sflp.data_buffer,
                    self.device,
                    self.device_address,
                    "IMU_SFLP",
                    timestamp_python,
                )
                self.sensor_device.imu_sflp.data_buffer.clear()

            if len(self.sensor_device.imu_sflp.data_df) >= 375:
                if self.callback_func:
                    await self.callback_func(
                        pd.DataFrame(self.sensor_device.imu_sflp.data_df),
                        self.device,
                        self.device_address,
                        "IMU_SFLP",
                        timestamp_python,
                    )
                    self.sensor_device.imu_sflp.data_df.clear()

            if (
                len(self.sensor_device.csv.rx_imu_sflp_df)
                >= self.sensor_device.imu_sflp.data_dump_size
            ):
                await self.write_sflp_to_csv()

        except Exception as e:
            logging.error(
                f"[IMUDevice][imu_sflp_notification_handler] Error in SFLP notification handler for {self.device_address}: {e}"
            )
            raise OperationError(
                f"[IMUDevice][imu_sflp_notification_handler] Error in SFLP notification handler for {self.device_address}: {e}"
            )

    @override  # type: ignore[override]
    async def write_to_csv(  # type: ignore
        self, file_name: str, headers: list, df: pd.DataFrame, log_tag: str
    ):
        """
        Generalized function to write DataFrame data to a CSV file.

        :param file_name: Path to the CSV file.
        :param headers: List of headers for the CSV file (e.g., HEADERS.PRESSURE).
        :param df: DataFrame to write.
        :param log_tag: Tag for logging (e.g., "Pressure", "IMU", "SFLP").
        """
        try:
            if df.empty:
                logging.warning(
                    f"[IMUDevice][{log_tag}][write_to_csv] DataFrame is empty. Skipping write."
                )
                return
            if not file_name:
                logging.error(
                    f"[IMUDevice][{log_tag}][write_to_csv] CSV file name not set."
                )
                raise OperationError(
                    f"[IMUDevice][{log_tag}][write_to_csv] CSV file name not set."
                )
            df = df.reindex(columns=headers)
            df.to_csv(
                file_name,
                mode="a",
                header=not os.path.exists(
                    file_name
                ),  # Write header if file doesn't exist
                index=False,
            )
            logging.info(
                f"[IMUDevice][{log_tag}][write_to_csv] Data written to {file_name}."
            )
        except Exception as e:
            logging.error(
                f"[IMUDevice][{log_tag}][write_to_csv] Error writing to {file_name}: {e}"
            )
            raise OperationError(
                f"[IMUDevice][{log_tag}][write_to_csv] Error writing to {file_name}: {e}"
            )

    # async def write_pressure_to_csv(self):
    #     """
    #     Writes the collected pressure data to a CSV file.
    #
    #     This function retrieves the pressure data stored in the `rx_press_shoe_df` DataFrame,
    #     writes it to a CSV file, and then clears the DataFrame after writing.
    #
    #     Raises:
    #         OperationError: If there is an error while writing the CSV file.
    #     """
    #     try:
    #         await self.write_to_csv(
    #             file_name=self.sensor_device.csv.file_name_shoe_press,
    #             headers=self.HEADERS_PRESSURE,  # type: ignore[attr-defined]
    #             df=self.sensor_device.csv.rx_press_shoe_df,
    #             log_tag="Pressure",
    #         )
    #         # Clear the DataFrame after writing
    #         self.sensor_device.csv.rx_press_shoe_df = pd.DataFrame(
    #             columns=[
    #                 "timestamp_python",
    #                 "ts_board",
    #                 "packet_counter",
    #                 "pressure_data",
    #             ]
    #         )
    #     except Exception as e:
    #         logging.error(
    #             f"[InsoleDevice][write_pressure_to_csv] Error writing pressure data to CSV: {e}"
    #         )
    #         raise OperationError(
    #             f"[InsoleDevice][write_pressure_to_csv] Error writing pressure data to CSV: {e}"
    #         )

    async def write_imu_to_csv(self):
        """
        Writes the collected IMU data to a CSV file.

        This function checks if the `rx_imu_df` DataFrame contains data. If it does,
        the data is written to a CSV file and then cleared. If the DataFrame is empty,
        the function logs a warning and exits.

        Raises:
            OperationError: If there is an error while writing the CSV file.
        """
        try:
            if self.sensor_device.csv.rx_imu_df.empty:
                logging.warning(
                    "[IMUDevice][write_imu_to_csv] IMU DataFrame is empty. Skipping CSV write."
                )
                return
            await self.write_to_csv(
                file_name=self.sensor_device.csv.file_name_imu,
                headers=self.HEADERS_IMU,  # type: ignore[attr-defined]
                df=self.sensor_device.csv.rx_imu_df,
                log_tag="IMU",
            )
            self.sensor_device.csv.rx_imu_df = pd.DataFrame(
                columns=["timestamp_python", "ts_board", "packet_counter", "imu_data"]
            )
        except Exception as e:
            logging.error(
                f"[IMUDevice][write_imu_to_csv] Error in write_imu_to_csv: {e}"
            )
            raise OperationError(
                f"[IMUDevice][write_imu_to_csv] Error in write_imu_to_csv: {e}"
            )

    async def write_sflp_to_csv(self):
        """
        Writes the collected SFLP (Sensor Fusion Low Power) data to a CSV file.

        This function writes the `rx_imu_sflp_df` DataFrame data to a CSV file and then
        clears the DataFrame after writing.

        Raises:
            OperationError: If there is an error while writing the CSV file.
        """
        try:
            await self.write_to_csv(
                file_name=self.sensor_device.csv.file_name_sflp,
                headers=self.HEADERS_IMU_SFLP,  # type: ignore[attr-defined]
                df=self.sensor_device.csv.rx_imu_sflp_df,
                log_tag="SFLP",
            )
            self.sensor_device.csv.rx_imu_sflp_df = pd.DataFrame(
                columns=["timestamp_python", "ts_board", "packet_counter", "sflp_data"]
            )
        except Exception as e:
            logging.error(
                f"[IMUDevice][write_sflp_to_csv] Error writing SFLP data to CSV: {e}"
            )
            raise OperationError(
                f"[IMUDevice][write_sflp_to_csv] Error writing SFLP data to CSV: {e}"
            )

    # def clear_pressure_data(self):
    #     self.sensor_device.csv.rx_press_shoe.clear()
    #     self.sensor_device.csv.ts_press.clear()
    #     self.sensor_device.csv.cnt_press.clear()

    def clear_data(self):
        self.sensor_device.csv.rx_imu.clear()
        self.sensor_device.csv.ts_imu.clear()
        self.sensor_device.csv.cnt_imu.clear()

    def clear_sflp_data(self):
        self.sensor_device.csv.rx_imu_sflp.clear()
        self.sensor_device.csv.ts_imu_sflp.clear()
        self.sensor_device.csv.cnt_imu_sflp.clear()

    @override
    async def force_save(self):
        try:
            # if not self.sensor_device.csv.rx_press_shoe_df.empty:
            #     logging.debug("[InsoleDevice][force_save] Flushing shoe-pressure data.")
            #     await self.write_pressure_to_csv()

            if not self.sensor_device.csv.rx_imu_df.empty:
                logging.debug("[IMUDevice][force_save] Flushing IMU data.")
                await self.write_imu_to_csv()

            if not self.sensor_device.csv.rx_imu_sflp_df.empty:
                logging.debug("[IMUDevice][force_save] Flushing SFLP data.")
                await self.write_sflp_to_csv()

        except Exception as e:
            logging.error(
                f"[IMUDevice][force_save] Error during force save in IMUDevice: {e}"
            )
