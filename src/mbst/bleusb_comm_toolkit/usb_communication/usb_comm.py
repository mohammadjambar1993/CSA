import asyncio
import logging

import serial
from serial.tools import list_ports

from mbst.bleusb_comm_toolkit.usb_communication.chars import (
    PressureMat,
    pressure_mat_char,
)


def build_device(device):
    if device in {"mat", "pressure_mat"}:
        return PressureMat(pressure_mat_char)


class SerialCommunication:
    def __init__(self, port, loop, device, consumer_callback=None, baud_rate=19200):
        self.port = port
        self.baud_rate = baud_rate
        self.serial_connection = None
        self.connection_status = False
        self.usb_device = build_device(device)
        self.enable_acq = self.usb_device.enable
        self.disable_acq = self.usb_device.disable
        self.serial_streaming = False
        self.disconnect_event = asyncio.Event()
        self.loop = loop
        self.consumer_callback = consumer_callback
        self.logger = logging.getLogger()

    def connect(self):
        try:
            self.serial_connection = serial.Serial(self.port, self.baud_rate, timeout=1)
            self.logger.info(
                f"[SerialCommunication] Connected to {self.port} at {self.baud_rate} baud."
            )
            # print(f"Connected to {self.port} at {self.baud_rate} baud.")
            self.connection_status = True
        except serial.SerialException as e:
            self.logger.error(
                f"[SerialCommunication] Failed to connect to {self.port}: {e}"
            )
            self.connection_status = False
            raise ConnectionError(
                f"[SerialCommunication] Failed to connect to {self.port}: {e}"
            )

    def restart_connection(self):
        self.disconnect()
        self.connect()

    def change_port_and_baud_rate(self, new_port, new_baud_rate):
        self.port = new_port
        self.baud_rate = new_baud_rate
        self.restart_connection()

    def disconnect(self):
        if self.serial_connection and self.serial_connection.is_open:
            self.serial_connection.close()
            self.connection_status = False
            self.serial_connection = None
            self.serial_streaming = False
            print("Connection closed.")

    def enable_acquisition(self):
        self.disable_acquisition()
        self.send_command(self.enable_acq)
        self.serial_streaming = True
        self.usb_device.matDevice.flags.usb_comm_streaming = True

    def disable_acquisition(self):
        self.send_command(self.disable_acq)
        self.serial_streaming = False
        self.usb_device.matDevice.flags.usb_comm_streaming = False
        self.logger.info("[SerialCommunication] USB Acquisition Disabled.")
        # print("USB Acquisition Disabled.")

    def send_command(self, hex_data):
        try:
            if self.serial_connection is None:
                raise serial.SerialException("Serial connection is not initialized.")
            # Convert hex string to bytes
            data_bytes = bytes.fromhex(hex_data)
            self.serial_connection.write(data_bytes)
            print(f"Sent hex data: {hex_data}")
        except serial.SerialException as e:
            print(f"SerialException: {e}")

    # async def check_connection_status(self):
    #     while self.connection_status:
    #         await asyncio.sleep(1)
    #         if not self.port_available():
    #             print("USB connection closed unexpectedly.")
    #             self.connection_status = False
    #             self.disconnect_event.set()
    #             break

    # TODO Verify callback function logic
    async def command_response(self, callback_func, payload=None):
        try:
            if self.serial_connection is None:
                raise serial.SerialException("Serial connection is not initialized.")

            if callback_func is None:
                callback_func = self.usb_device.callback_functions[
                    self.usb_device.response_char_index
                ]

            if payload is None:
                payload = self.usb_device.activate_command_response

            self.send_command(payload)
            await asyncio.sleep(0)
            data = self.serial_connection.read(6)
            header_data = self.header_information(data)
            remaining_data = self.serial_connection.read(header_data[2])

            if header_data[1] == "9803":
                await callback_func(header_data, remaining_data)

        except serial.SerialException as e:
            print(f"SerialException: {e}")

    async def start_notify(self, callback_functions=None):
        try:
            if self.serial_connection is None:
                raise serial.SerialException("Serial connection is not initialized.")
            # connection_checker_task = asyncio.create_task(self.check_connection_status())
            if callback_functions is None:
                callback_functions = self.usb_device.callback_functions

            while self.connection_status:
                if not self.serial_streaming:
                    break

                data = self.serial_connection.read(6)
                header_data = self.header_information(data)
                remaining_data = self.serial_connection.read(header_data[2])

                # await asyncio.sleep(1)
                # await callback_functions["9801"](header_data, remaining_data)
                # Header_data[0] == "0000" trash data
                if header_data[1] == "9880":
                    payload = await callback_functions["9880"](
                        header_data, remaining_data
                    )
                    await self.consumer_callback(payload)

                await asyncio.sleep(0)

        except KeyboardInterrupt:
            print("KeyboardInterrupt: Stopping data reading.")
            self.disable_acquisition()
            self.usb_device.end_of_acquisition()

            # stop stream and disconnect
        except serial.SerialException as e:
            if "FileNotFoundError" in str(e):
                print("USB connection closed.")
                raise SystemError("USB connection closed.")
            else:
                print(f"SerialException: {e}")
                raise SystemError(f"SerialException: {e}")
        # finally:
        #     self.disconnect()
        #     # await connection_checker_task
        #     print("Connection closed.")

    def header_information(self, byte_sequence):
        """
        :param byte_sequence: bytearray
        :return: the header information based on the byte sequence
        """
        part1 = int.from_bytes(byte_sequence[0:2], byteorder="little")
        part2 = int.from_bytes(byte_sequence[2:4], byteorder="little")
        part3 = int.from_bytes(byte_sequence[4:], byteorder="little")
        result = [f"{part1:03x}", f"{part2:04x}", part3]
        return result

    def port_available(self):
        available_ports = [port.device for port in serial.tools.list_ports.comports()]
        return self.port in available_ports
