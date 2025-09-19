from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from bleak import BleakClient


@dataclass
class Csv:
    # Pressure-related fields
    rx_press: List[Dict[str, Any]] = field(default_factory=list)
    rx_timestamps_pressure: List[str] = field(default_factory=list)
    ts_pressure: List[int] = field(default_factory=list)
    cnt_pressure: List[int] = field(default_factory=list)
    read_pressure: List[int] = field(default_factory=list)
    trig_pressure: List[int] = field(default_factory=list)

    # IMU-related fields
    ts_imu: List[int] = field(default_factory=list)
    cnt_imu: List[int] = field(default_factory=list)
    rx_imu: List[int] = field(default_factory=list)
    rx_timestamps_imu: List[str] = field(default_factory=list)
    rx_imu_df = pd.DataFrame(
        columns=["timestamp_python", "ts_board", "packet_counter", "sensor_data"]
    )

    # IMU SFLP-related fields
    ts_imu_sflp: List[int] = field(default_factory=list)
    cnt_imu_sflp: List[int] = field(default_factory=list)
    rx_imu_sflp: List[int] = field(default_factory=list)
    rx_timestamps_imu_sflp: List[str] = field(default_factory=list)
    rx_imu_sflp_df = pd.DataFrame(
        columns=["timestamp_python", "timestamp", "counter", "imu_sflp_data"]
    )

    # Pressure Shoe-related fields
    ts_press: List[int] = field(default_factory=list)
    cnt_press: List[int] = field(default_factory=list)
    rx_press_shoe: List[int] = field(default_factory=list)
    rx_timestamps_press_shoe: List[str] = field(default_factory=list)
    rx_press_shoe_df = pd.DataFrame(
        columns=["timestamp_python", "timestamp", "counter", "pressure_data"]
    )

    # File names for CSV storage
    file_name_csv: str = ""
    file_name_imu: str = ""
    file_name_sflp: str = ""
    file_name_shoe_press: str = ""


@dataclass
class BleFlags:
    created: bool = False
    rescan: bool = False
    global_service: bool = False
    scanning: bool = False
    streaming: bool = False
    data_available: bool = False
    pressure_on: bool = False
    sensor_configured: bool = False
    notifications_started: bool = False
    information_notifications_started: bool = False
    pressure_notifications_started: bool = False
    imu_notifications_started: bool = False
    imu_sflp_notifications_started: bool = False
    is_connected: bool = False
    device_initialized: bool = False


@dataclass
class ShoePressure:
    data_buffer: List[Dict[Any, Any]] = field(default_factory=list)
    data_packet: dict = field(default_factory=dict)

    matrix_filled: bool = False
    ready_to_display: bool = False
    ready_to_process: bool = False
    data_dump_size: int = 8  # Number of samples before writing to CSV


@dataclass
class Imu:
    data_buffer: List[Dict[Any, Any]] = field(default_factory=list)
    data_df: List[int] = field(default_factory=list)
    imu_ints: List[int] = field(default_factory=list)
    imu_arr: str = ""

    prev_trig_number: Optional[int] = None
    packet_number: int = 0
    packet_loss: int = 0

    matrix_filled: bool = False
    ready_to_display: bool = False
    ready_to_process: bool = False
    data_dump_size: int = 64  # Number of samples before writing to CSV


@dataclass
class ImuSflp:
    data_buffer: List[Dict[Any, Any]] = field(default_factory=list)
    data_df: List[List[Any]] = field(default_factory=list)
    imu_sflp_ints: List[int] = field(default_factory=list)
    imu_sflp_arr: str = ""

    prev_trig_number: int = 0
    packet_number: int = 0
    packet_loss: int = 0

    matrix_filled: bool = False
    ready_to_display: bool = False
    ready_to_process: bool = False
    data_dump_size: int = 64  # Number of samples before writing to CSV


@dataclass
class Pressure:
    data_buffer: List[int] = field(default_factory=list)
    data_matrix: List[int] = field(default_factory=list)
    pressure_ints: List[int] = field(default_factory=list)
    pressure_arr: str = ""

    packet_number: int = 0
    packet_loss: int = 0
    packet_loss_detected: bool = False

    matrix_filled: bool = False
    ready_to_display: bool = False
    ready_to_process: bool = False
    data_dump_size: int = 64  # Number of samples before writing to CSV

    def __init__(self):
        self.data_buffer: List[List[int]] = []
        self.data_matrix = []
        self.pressure_ints = []
        self.pressure_arr = ""
        self.packet_trigline = [0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11]
        self.prev_trig_number: Optional[int] = None  # type: ignore[assignment]
        self.packet_trig_inc = 0


@dataclass
class DeviceAttributeManager:
    name: str
    address: str = ""
    firmware: str = ""
    battery: str = ""
    samplingRate: float = 0.0
    samplingRateSelection: str = ""
    client: Optional[BleakClient] = None
    flags: BleFlags = field(default_factory=BleFlags)
    pressure: Pressure = field(default_factory=Pressure)
    shoe_pressure: ShoePressure = field(default_factory=ShoePressure)
    imu: Imu = field(default_factory=Imu)
    imu_sflp: ImuSflp = field(default_factory=ImuSflp)
    csv: Csv = field(default_factory=Csv)
    device_modality: str = ""
    device_addresses: List[str] = field(default_factory=list)
