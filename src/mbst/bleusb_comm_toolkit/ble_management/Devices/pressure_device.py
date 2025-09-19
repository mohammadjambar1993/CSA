import asyncio
import csv
import logging
import os
from datetime import datetime
from typing import Optional

import pytz
from mbst.core.common_tools.custom_decorators import override
from mbst.core.common_tools.raised_errors import (
    ConfigurationError,
    CustomError,
    OperationError,
    UnexpectedDisconnectionError,
)
from mbst.core.device_defintions.pressure_bed_fw import PressureMatFirmware

from mbst.bleusb_comm_toolkit.ble_management.ble_utils import *
from mbst.bleusb_comm_toolkit.ble_management.Devices import BLEDevice

# chars = pressure_mat_fw["chars"]
local_tz = pytz.timezone("America/Toronto")

pressure_mat_fw = PressureMatFirmware()
chars = pressure_mat_fw.chars


class PressureMatDevice(BLEDevice):
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

        self.logger = logger or logging.getLogger()
        self.cwd = cwd or os.getcwd()
        self.callback_func = callback_func
        self.sampling_rate = 9091
        self.selected_sampling_rate = None
        self.timezone = timezone

    async def read_sensor_config(self):
        try:
            payload = self.generate_dynamic_payload(
                opcode=self.READ_SENSOR_CFG,  # type: ignore[attr-defined]
                system=self.PRESSURE,  # type: ignore[attr-defined]
            )
            logging.info(
                f"[PressureMatDevice][read_sensor_config] Writing configuration payload: {payload} to {self.device_address}"
            )
            await self.client.write_gatt_char(self.COMMAND, bytearray.fromhex(payload), True)  # type: ignore[attr-defined]
        except Exception as e:
            logging.error(
                f"[PressureMatDevice][read_sensor_config] Error during read_sensor_config of {self.device_address}: {e}"
            )
            raise OperationError(
                f"[PressureMatDevice][read_sensor_config] Error during read_sensor_config of {self.device_address}: {e}"
            )

    async def sensor_ble_configuration(self, sampling_rate="9091", cap="1000pF"):
        try:
            payloadSR = self.generate_dynamic_payload(
                opcode=self.CONFIG_SENSOR,  # type: ignore[attr-defined]
                system=self.PRESSURE,  # type: ignore[attr-defined]
                sampling_rate=float(self.sampling_rate),
                feedback_capacitor=cap,
            )
            if not payloadSR:
                logging.error(
                    "[PressureMatDevice][sensor_ble_configuration] Failed to generate payload."
                )
                raise ConfigurationError(
                    f"Failed to generate payload for device {self.device_address}"
                )
            logging.info(
                "[PressureMatDevice][sensor_ble_configuration] Configuring BLE Devices, Generated Payload: "
                + str(payloadSR)
            )
            if self.client.is_connected:
                await self.configure(payloadSR)
            else:
                raise UnexpectedDisconnectionError(
                    f"[PressureMatDevice][sensor_ble_configuration] Failed to Configure device {self.device}:{self.device_address} due to device disconnection"
                )

        except Exception as e:
            logging.error(
                "[PressureMatDevice][sensor_ble_configuration] Exception thrown "
                + str(e)
            )

    @override
    async def stop_all_notifications(self):
        if not self.client.is_connected:
            logging.warning(
                "[PressureMatDevice][stop_all_notifications] Device already disconnected, skipping stop notifications."
            )
            return
        logging.info(
            f"[PressureMatDevice][stop_all_notifications] Stopping all notifications at {datetime.now()}"
        )

        if self.sensor_device.flags.information_notifications_started:
            await self.client.stop_notify(chars["INFORMATION"])  # type: ignore[arg-type]
            logging.info(
                "[PressureMatDevice][stop_all_notifications] Stopped notifications for information_char_id."
            )
            self.sensor_device.flags.information_notifications_started = False

        if self.sensor_device.flags.notifications_started:
            await self.client.stop_notify(chars["PRESSURE_PROCESSED"])  # type: ignore[arg-type]
            logging.info(
                f"[PressureMatDevice][stop_all_notifications] Stopped notifications for pressure data for device {self.device}:{self.device_address}."
            )
            self.sensor_device.flags.notifications_started = False

        self.clear_pressure_buffer()

    async def start_read(self):
        try:
            self.clear_packet_loss_variables()
            asyncio.ensure_future(self.read_pressure(), loop=self._loop)
        except Exception as e:
            logging.error(
                f"[PressureMatDevice][start_read] Error during start_read for {self.device_address}: {e}"
            )
            raise OperationError(
                f"[PressureMatDevice][start_read] Error during start_read for {self.device_address} {e}"
            )

    async def read_pressure(self):
        if not self.client.is_connected:
            logging.error(
                f"[PressureMatDevice][read_pressure] Device is not connected. Cannot start pressure notifications for {self.device_address}."
            )
            raise UnexpectedDisconnectionError(
                f"[PressureMatDevice][read_pressure] Device is not connected. Cannot start pressure notifications for {self.device_address}."
            )
        try:
            await self.client.start_notify(self.PRESSURE_PROCESSED, self.pressure_notification_handler)  # type: ignore[attr-defined]
            logging.info(
                f"[PressureMatDevice][read_pressure] Started pressure notifications for {self.device_address}"
            )
        except Exception as e:
            logging.error(
                f"[PressureMatDevice][read_pressure] Error starting notifications for {self.device_address}: {str(e)}"
            )

    async def pressure_notification_handler(self, sender, data):
        try:
            parsed_data = PressureMatFirmware.pressure_parser(data)
            read_line = parsed_data["read"]
            trig_line = parsed_data["trigger"]
            pressure_data = parsed_data["pressure_data"]
            timestamp = parsed_data["timestamp"]
            counter = parsed_data["packet_counter"]
            timestamp_python = str(int(datetime.now(self.timezone).timestamp() * 1000))

            # Start Processing Data at (0,0)
            if not self.sensor_device.pressure.ready_to_process:
                if trig_line == 0:
                    self.sensor_device.pressure.ready_to_process = True
                else:
                    return

            data_entry = {
                "timestamp": timestamp,
                "counter": counter,
                "read": read_line,
                "trigger": trig_line,
                "pressure_data": ", ".join(map(str, pressure_data)),
                "timestamp_python": timestamp_python,
            }
            # self.sensor_device.pressure.data_buffer.append(data_entry)

            self.sensor_device.flags.streaming = True
            self.sensor_device.pressure.pressure_ints = []
            self.sensor_device.pressure.pressure_arr = ""

            for pressure_value in pressure_data:
                self.sensor_device.pressure.pressure_ints.append(pressure_value)
                self.sensor_device.pressure.pressure_arr += f"{pressure_value}, "

            if len(pressure_data) < 96:
                logging.info(
                    "[PressureMatDevice][pressure_notification_handler] Insufficient pressure data length for expected 4 columns of 24 values."
                )

            # Split pressure_data into columns for 2D array (24 items per column)
            column_1 = pressure_data[0:24]
            column_2 = pressure_data[24:48]
            column_3 = pressure_data[48:72]
            column_4 = pressure_data[72:96]

            column_1_formatted = (
                column_1[0::4] + column_1[1::4] + column_1[2::4] + column_1[3::4]
            )
            column_2_formatted = (
                column_2[0::4] + column_2[1::4] + column_2[2::4] + column_2[3::4]
            )
            column_3_formatted = (
                column_3[0::4] + column_3[1::4] + column_3[2::4] + column_3[3::4]
            )
            column_4_formatted = (
                column_4[0::4] + column_4[1::4] + column_4[2::4] + column_4[3::4]
            )

            self.check_for_packet_loss(trig_line)

            self.sensor_device.pressure.data_buffer.append(column_1_formatted)
            self.sensor_device.pressure.data_buffer.append(column_2_formatted)
            self.sensor_device.pressure.data_buffer.append(column_3_formatted)
            self.sensor_device.pressure.data_buffer.append(column_4_formatted)

            if len(self.sensor_device.pressure.data_buffer) > 15:
                self.sensor_device.pressure.ready_to_display = True
                self.sensor_device.pressure.matrix_filled = True
                # Keep only the most recent 15 rows for display
                self.sensor_device.pressure.data_matrix = (
                    self.sensor_device.pressure.data_buffer[0:15]
                )
                self.sensor_device.pressure.data_buffer = (
                    self.sensor_device.pressure.data_buffer[15:]
                )

                # if self.callback_func:
                #     await self.callback_func(
                #         self.sensor_device.pressure.data_matrix,
                #         self.device,
                #         self.device_address,
                #         "PRESSURE_PROCESSED",
                #         timestamp_python,
                #     )

            # Build the packet payload
            data_to_redis = {
                "matrix": self.sensor_device.pressure.data_matrix,
                "counter": counter,
                "trigger": trig_line,
                "read": read_line,
                "timestamp": timestamp,
                "timestamp_python": timestamp_python,
            }

            if self.callback_func:
                await self.callback_func(
                    data_to_redis,
                    self.device,
                    self.device_address,
                    "PRESSURE_PROCESSED",
                    timestamp_python,
                )

            self.data_available = True
            self.sensor_device.csv.ts_pressure.append(timestamp)
            self.sensor_device.csv.cnt_pressure.append(counter)
            self.sensor_device.csv.rx_press.append(data_entry)
            # self.sensor_device.csv.rx_press.append(self.sensor_device.pressure.pressure_arr.strip(', '))
            self.sensor_device.csv.read_pressure.append(read_line)
            self.sensor_device.csv.trig_pressure.append(trig_line)

            if (
                len(self.sensor_device.csv.rx_press)
                >= self.sensor_device.pressure.data_dump_size
            ):
                # logging.info(f"[pressure_notification_handler] CSV buffer size "
                #            f"({self.sensor_device.pressure.data_dump_size}) reached. Initiating CSV write.", "info")
                self.write_to_csv()
                self.clear_data()
        except Exception as e:
            logging.error(
                "[pressure_notification_handler] Error in Pressure notification Handler: "
                + str(e)
            )
            raise OperationError(
                f"[pressure_notification_handler] Error in Pressure notification Handler for {self.device_address}: {str(e)}"
            )

    def check_for_packet_loss(self, trig):
        """
        Checks if packet loss occurred by comparing trigger values.

        Args:
            trig (int): Current trigger value.

        Returns:
            None
        """
        if self.sensor_device.pressure.prev_trig_number is None:
            self.sensor_device.pressure.prev_trig_number = trig
            return

        trig_values = [0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15]
        trig_index = trig_values.index(trig)
        prev_trig_index = trig_values.index(
            self.sensor_device.pressure.prev_trig_number
        )

        flag = False

        if self.sensor_device.pressure.prev_trig_number == 14:
            flag = trig == 3
        elif self.sensor_device.pressure.prev_trig_number == 13:
            flag = trig == 2
        elif self.sensor_device.pressure.prev_trig_number == 12:
            flag = trig == 1
        elif self.sensor_device.pressure.prev_trig_number == 11:
            flag = trig == 0
        else:
            flag = self.sensor_device.pressure.prev_trig_number + 4 == trig

        if flag:
            self.sensor_device.pressure.prev_trig_number = trig
        else:
            pckt_loss = [-1000] * 24
            self.sensor_device.pressure.packet_loss += (
                abs(trig_index - prev_trig_index) - 1
            )
            logging.warning(
                "[PressureMatDevice][check_for_packet_loss] Packet Loss Detected. Number of Packets Lost: "
                + str(abs(trig_index - prev_trig_index) - 1)
                + ". Total Packets lost: "
                + str(self.sensor_device.pressure.packet_loss)
            )
            count = (abs(trig_index - prev_trig_index) - 1) * 4
            self.sensor_device.pressure.prev_trig_number = trig

            for _ in range(count):
                self.sensor_device.pressure.data_buffer.append(pckt_loss)  # type: ignore[arg-type]

                if len(self.sensor_device.pressure.data_buffer) > 15:
                    self.sensor_device.pressure.data_matrix = (
                        self.sensor_device.pressure.data_buffer[0:15]
                    )
                    self.sensor_device.pressure.data_buffer = (
                        self.sensor_device.pressure.data_buffer[15:]
                    )

        self.sensor_device.pressure.packet_loss_detected = not flag

    def write_to_csv(self):
        try:
            if not self.sensor_device.csv.file_name_csv:
                logging.error(
                    "[PressureMatDevice][write_to_csv] CSV file name not set."
                )
                raise OperationError(
                    "[PressureMatDevice][write_to_csv] CSV file name not set."
                )
            with open(self.sensor_device.csv.file_name_csv, "a+", newline="") as f:
                writer = csv.writer(f)
                if os.stat(self.sensor_device.csv.file_name_csv).st_size == 0:
                    headers = self.HEADERS_PRESSURE  # type: ignore[attr-defined]
                    writer.writerow(headers)
                for data_entry in self.sensor_device.csv.rx_press:
                    if not isinstance(data_entry, dict):
                        logging.error(
                            f"[PressureMatDevice][write_to_csv] Expected dict, got {type(data_entry)}: {data_entry}"
                        )
                        return
                    timestamp = data_entry.get("timestamp")
                    counter = data_entry.get("counter")
                    read_pressure = data_entry.get("read")
                    trigger_pressure = data_entry.get("trigger")
                    pressure_data = data_entry.get("pressure_data")
                    formatted_date = data_entry.get("timestamp_python")
                    writer.writerow(
                        [
                            formatted_date,
                            timestamp,
                            counter,
                            read_pressure,
                            trigger_pressure,
                            pressure_data,
                        ]
                    )

            # Clear `data_buffer` after writing to avoid duplicate entries
            self.sensor_device.csv.rx_press.clear()
            logging.info(
                f"[PressureMatDevice][write_to_csv] Data written to {self.sensor_device.csv.file_name_csv} for {self.device_address}"
            )
        except Exception as e:
            logging.error(
                f"[PressureMatDevice][write_to_csv] Error writing to CSV for {self.device_address}: {str(e)}"
            )

    def clear_data(self):
        self.sensor_device.csv.rx_press.clear()
        self.sensor_device.csv.ts_pressure.clear()
        self.sensor_device.csv.cnt_pressure.clear()
        self.sensor_device.csv.read_pressure.clear()
        self.sensor_device.csv.trig_pressure.clear()

    def clear_pressure_buffer(self):
        self.sensor_device.pressure.data_buffer = []

    def clear_packet_loss_variables(self):
        self.sensor_device.pressure.prev_trig_number = None
        self.sensor_device.pressure.packet_loss_detected = False
        self.sensor_device.pressure.packet_loss = 0
