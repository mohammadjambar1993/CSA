from dataclasses import dataclass, field
from typing import Any, List

# import pyqtgraph as pg
import numpy as np
from bleak import BleakClient


@dataclass
class Csv:
    rx_press: list[str]
    rx_imu: list[str]
    rx_audio: np.ndarray

    rx_timestamps_pressure = list[str]
    rx_timestamps_imu = list[str]
    rx_timestamps_audio = list[str]

    ts_pressure: list[int]
    cnt_pressure = list[int]

    read_pressure = list[int]
    trig_pressure = list[int]

    ts_imu: list[int]
    cnt_imu = list[int]

    ts_audio: list[int]
    cnt_audio = list[int]

    file_name_csv: str = ""
    file_name_imu_csv: str = ""
    file_name_audio_csv: str = ""

    data_dump_size_press: int = 512
    data_dump_size_imu: int = 128
    data_dump_size_audio: int = 480000

    column_names = ["timestamp_python", "ts_board", "packet_counter", "pressure_data"]
    column_names_imu = ["timestamp_python", "ts_board", "packet_counter", "acc_data"]

    def __init__(self):
        self.rx_audio = np.array([], dtype=np.int16)
        self.rx_press: List[Any]
        self.rx_imu: List[Any]
        self.rx_timestamps_pressure: List[Any]
        self.rx_timestamps_imu: List[Any]
        self.ts_pressure: List[Any]
        self.cnt_pressure: List[Any]
        self.read_pressure: List[Any]
        self.trig_pressure: List[Any]
        self.cnt_audio: List[Any]
        self.ts_imu: List[Any]
        self.cnt_imu: List[Any]
        self.ts_audio: List[Any]


@dataclass
class BleFlags:
    usb_comm: bool = False
    usb_comm_streaming: bool = False
    created: bool = False
    rescan: bool = False
    global_service: bool = False
    scanning: bool = False
    streaming: bool = False
    audio_streaming: bool = False
    data_available: bool = False
    imu_on: bool = False
    pressure_on: bool = False
    audio_on: bool = False
    force_save: bool = False


@dataclass
class Pressure:
    data_buffer: list[int]
    fft_buffer: list[list]
    data_matrix: list[int]
    fft_plot_data: list[list]
    pressure_ints: list[int]
    packet_trigline: list[int]
    pressure_arr: str

    prev_trig_number: int = 0
    packet_number: int = 0
    packet_loss: int = 0

    matrix_filled: bool = False
    ready_to_display: bool = False
    ready_to_process: bool = False
    plot_live_fft: bool = False

    def __init__(self):
        self.data_buffer = []
        self.fft_buffer = [[], [], [], []]
        self.data_matrix = []
        self.fft_plot_data = [[], [], [], []]
        self.pressure_ints = []
        self.pressure_arr = ""

        self.packet_trigline = [0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11]
        self.packet_trig_inc = 0


# @dataclass
# class Audio:
#     audio_buffer: list[int]
#     audio_ints: list[int]
#     media_player: field(default_factory=QMediaPlayer)
#     media_playlist: field(default_factory=QMediaPlaylist)
#     save_interval: int
#     gain_applied: int
#
#     def __init__(self):
#         self.audio_ints = []
#         self.audio_buffer = []
#         self.save_interval = 45
#         self.gain_applied = 1
#         self.media_player = QMediaPlayer()
#         self.media_playlist = QMediaPlaylist()
#         self.media_playlist.setPlaybackMode(QMediaPlaylist.Sequential)
#         self.media_player.setPlaylist(self.media_playlist)


@dataclass
class Imu:
    imu_arr: str
    imu_ints: list[int]

    def __init__(self):
        self.imu_arr = ""
        self.imu_ints = []


# @dataclass
# class Graphing:
#     # Pressure
#     timeSeries: field(default_factory=pg.GraphicsLayoutWidget)
#     curve: field(default_factory=pg.PlotDataItem)
#     data: np.ndarray
#
#     # IMU
#     data_ax: list[int]
#     data_ay: list[int]
#     data_az: list[int]
#     data_gx: list[int]
#     data_gy: list[int]
#     data_gz: list[int]
#
#     imuSeries: field(default_factory=pg.GraphicsLayoutWidget)
#     curve_accelx: field(default_factory=pg.PlotDataItem) = pg.PlotDataItem()
#     curve_accely: field(default_factory=pg.PlotDataItem) = pg.PlotDataItem()
#     curve_accelz: field(default_factory=pg.PlotDataItem) = pg.PlotDataItem()
#     curve_gyrox: field(default_factory=pg.PlotDataItem) = pg.PlotDataItem()
#     curve_gyroy: field(default_factory=pg.PlotDataItem) = pg.PlotDataItem()
#     curve_gyroz: field(default_factory=pg.PlotDataItem) = pg.PlotDataItem()
#
#     def __init__(self):
#         self.timeSeries = pg.GraphicsLayoutWidget(show=True)
#         self.curve = pg.PlotDataItem()
#         self.data = np.zeros(300)
#         self.imuSeries = pg.GraphicsLayoutWidget(show=False)
#         data = (np.zeros(36)).tolist()
#         self.data_ax = data
#         self.data_ay = data
#         self.data_az = data
#         self.data_gx = data
#         self.data_gy = data
#         self.data_gz = data


@dataclass
class DeviceAttributesManager:
    name: str
    address: str
    firmware: str
    battery: str
    samplingRate: float
    samplingRateSelection: str
    client: BleakClient
    flags: BleFlags
    pressure: Pressure
    # audio: Audio
    imu: Imu
    csv: Csv
    # graphing: Graphing
    visuals: dict = field(default_factory=dict)

    def __init__(self, name: str):
        self.name = name
        self.address = ""
        self.firmware = ""
        self.battery = ""
        self.flags = BleFlags()
        self.pressure = Pressure()
        # self.audio = Audio()
        self.imu = Imu()
        self.csv = Csv()
        self.visuals = {}
        # self.graphing = Graphing()
