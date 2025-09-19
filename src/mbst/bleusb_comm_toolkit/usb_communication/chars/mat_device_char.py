from datetime import datetime

from ..device_manager import *

pressure_mat_char = {
    "service_uuid": "20049800-9a30-4ea8-9d21-04148452e81f",
    "information_char_id": "20049801-9a30-4ea8-9d21-04148452e81f",
    "command_char_id": "20049802-9a30-4ea8-9d21-04148452e81f",
    "response_char_id": "20049803-9a30-4ea8-9d21-04148452e81f",
    "response_char_index": "9803",
    "pressure_service_char_id": "20049880-9a30-4ea8-9d21-04148452e81f",
    "enable": "309a0298020001a0",
    "disable": "309a0298020002a0",
    "activate_command_response": "309a02980b0006a0000000000000000000",
}


class PressureMat:
    def __init__(self, data, cwd=None):
        self.__dict__.update(data)
        self.data_dump_size = 64
        self.cwd = cwd
        self.matDevice = DeviceAttributesManager(name="mat")
        self.callback_functions = {
            "9880": self.pressure_notification_handler,
            "9803": self.response_notification_handler,
        }
        # self.callback_functions = {"9803": self.response_notification_handler,
        #                            "9880": self.pressure_notification_handler,
        #                            "98a0": self.audio_notification_handler,
        #                            "9801": self.notification_handler}

    async def response_notification_handler(self, sender, data, callback_func=None):
        return None

    async def pressure_notification_handler(self, sender, data, callback_func=None):
        try:
            self.matDevice.flags.streaming = True
            pressure_data = data[2:194]
            readline = data[0]
            trigline = data[1]
            # header_info = data[194:]
            # header_converted = [b for b in header_info]
            # time_info = header_info[0:3]
            self.matDevice.pressure.pressure_ints = []
            self.matDevice.pressure.pressure_arr = ""

            # Start Processing Data at (0,0)
            if not self.matDevice.pressure.ready_to_process:
                if trigline == 0:
                    self.matDevice.pressure.ready_to_process = True
                else:
                    return

            for i in range(2, len(pressure_data) + 2, 2):
                pressure_sample = pressure_data[i - 2 : i]
                pressure_value = int.from_bytes(
                    pressure_sample, byteorder="little", signed=True
                )
                self.matDevice.pressure.pressure_ints.append(pressure_value)
                self.matDevice.pressure.pressure_arr += str(pressure_value)
                self.matDevice.pressure.pressure_arr += ", "

            # Format Packet Data - For Table View
            pressure_data = self.matDevice.pressure.pressure_ints.copy()
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

            self.matDevice.pressure.pressure_ints.insert(0, [readline, trigline])

            # TODO Check for Packet Loss
            # self.check_for_packet_loss(trigline)

            # Add to data buffer
            self.matDevice.pressure.data_buffer.append(column_1_formatted)
            self.matDevice.pressure.data_buffer.append(column_2_formatted)
            self.matDevice.pressure.data_buffer.append(column_3_formatted)
            self.matDevice.pressure.data_buffer.append(column_4_formatted)

            # Format Packet Data for FFT Plots
            read1 = column_1[0::4] + column_2[0::4] + column_3[0::4] + column_4[0::4]
            read3 = column_1[1::4] + column_2[1::4] + column_3[1::4] + column_4[1::4]
            read2 = column_1[2::4] + column_2[2::4] + column_3[2::4] + column_4[2::4]
            read4 = column_1[3::4] + column_2[3::4] + column_3[3::4] + column_4[3::4]

            self.matDevice.pressure.fft_buffer[0].extend(read1)
            self.matDevice.pressure.fft_buffer[1].extend(read3)
            self.matDevice.pressure.fft_buffer[2].extend(read2)
            self.matDevice.pressure.fft_buffer[3].extend(read4)

            # Check Matrix
            if len(self.matDevice.pressure.data_buffer) > 15:
                self.matDevice.pressure.data_matrix = (
                    self.matDevice.pressure.data_buffer[0:15]
                )
                self.matDevice.pressure.data_buffer = (
                    self.matDevice.pressure.data_buffer[15:]
                )
                self.matDevice.pressure.ready_to_display = True  # reset in dispaly
                self.matDevice.pressure.matrix_filled = True  # reset after csv dump
                self.data_available = True

            if self.data_available:
                self.data_available = False
                return self.matDevice.pressure.data_matrix
            else:
                # print("Data Buffering")
                return False

            # CSV
            # self.matDevice.csv.ts_pressure.append(int.from_bytes(time_info, byteorder='little', signed=False))
            # self.matDevice.csv.cnt_pressure.append(int.from_bytes(header_converted[3:], byteorder='little',
            #                                                       signed=False))  # Packet counter is four bytes now changed to [3:]inf
            # self.matDevice.csv.rx_press.append(self.matDevice.pressure.pressure_arr.replace("\n", "").replace(',', '~'))
            # self.matDevice.csv.read_pressure.append(readline)
            # self.matDevice.csv.trig_pressure.append(trigline)
            # self.record_time_info()
            #
            # if len(self.matDevice.csv.rx_press) >= self.data_dump_size:
            #     # self.write_to_csv()
            #     self.clear_pressure_lists()

        except Exception as e:
            print(
                "[pressure_notification_handler] Error in Pressure notification Handler: "
                + str(e)
            )
            # logging.exception("[pressure_notification_handler] Error in Pressure notification Handler: " + str(e))

    def record_time_info(self):
        present_time = datetime.now()
        self.matDevice.csv.rx_timestamps_pressure.append(
            present_time.strftime("%H:%M:%S.%f")
        )

    def clear_pressure_lists(self):
        self.matDevice.csv.rx_press.clear()
        self.matDevice.csv.rx_timestamps_pressure.clear()
        self.matDevice.csv.ts_pressure.clear()
        self.matDevice.csv.cnt_pressure.clear()
        self.matDevice.csv.read_pressure.clear()
        self.matDevice.csv.trig_pressure.clear()

    def clear_pressure_buffer(self):
        self.matDevice.pressure.data_buffer = []
        self.matDevice.pressure.fft_buffer = [[], [], [], []]
        self.matDevice.pressure.ready_to_process = False

    def force_save(self):
        # self.write_to_csv()
        self.clear_pressure_lists()

    def end_of_acquisition(self):
        # Device Manager
        self.matDevice.flags.streaming = False
        self.matDevice.flags.audio_streaming = False
        self.matDevice.flags.usb_comm_streaming = False

        # Packet Loss
        # self.clear_packet_loss_variables()

        # Clear Buffers
        self.clear_pressure_buffer()

        # FORCE SAVE
        self.force_save()


pressure_mat = PressureMat(pressure_mat_char)
