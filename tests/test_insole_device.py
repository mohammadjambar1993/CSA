import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.mbst.bleusb_comm_toolkit.ble_management.Devices import insole_device
from src.mbst.bleusb_comm_toolkit.ble_management.Devices.insole_device import (
    InsoleDevice,
)


@pytest.mark.asyncio
async def test_read_sensor_config():
    device = InsoleDevice(device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.generate_dynamic_payload = MagicMock(return_value="aabbcc")

    await device.read_sensor_config()

    device.client.write_gatt_char.assert_called_once()


@pytest.mark.asyncio
async def test_sensor_ble_configuration_connected():
    device = InsoleDevice(device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = True
    device.generate_dynamic_payload = MagicMock(return_value="aabbcc")
    device.configure = AsyncMock()

    await device.sensor_ble_configuration()

    device.configure.assert_called_once()


@pytest.mark.asyncio
async def test_sensor_ble_configuration_disconnected():
    device = InsoleDevice(device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = False
    device.generate_dynamic_payload = MagicMock(return_value="aabbcc")

    await device.sensor_ble_configuration()


@pytest.mark.asyncio
async def test_stop_all_notifications():
    device = InsoleDevice(device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = True
    device.sensor_device.flags.information_notifications_started = True
    device.sensor_device.flags.imu_notifications_started = True
    device.sensor_device.flags.imu_sflp_notifications_started = True
    # device.sensor_device.flags.pressure_notifications_started = True

    # Patch the global `chars` used in insole_device module
    insole_device.chars["INFORMATION"] = "info_uuid"
    insole_device.chars["IMU"] = "imu_uuid"
    insole_device.chars["IMU_SFLP"] = "sflp_uuid"
    # insole_device.chars["PRESSURE_PROCESSED"] = "pressure_uuid"

    await device.stop_all_notifications()

    device.client.stop_notify.assert_any_call("info_uuid")
    device.client.stop_notify.assert_any_call("imu_uuid")
    device.client.stop_notify.assert_any_call("sflp_uuid")
    # device.client.stop_notify.assert_any_call("pressure_uuid")

    assert device.sensor_device.flags.information_notifications_started is False
    assert device.sensor_device.flags.imu_notifications_started is False
    assert device.sensor_device.flags.imu_sflp_notifications_started is False
    # assert device.sensor_device.flags.pressure_notifications_started is False


@pytest.mark.asyncio
async def test_start_read():
    device = InsoleDevice(device_address="AA:BB:CC:DD:EE:FF")
    # device.read_pressure = AsyncMock()
    device.read_imu = AsyncMock()
    device.read_imu_sflp = AsyncMock()

    await device.start_read()

    # device.read_pressure.assert_called_once()
    device.read_imu.assert_called_once()
    device.read_imu_sflp.assert_called_once()


# @pytest.mark.asyncio
# async def test_read_pressure_disconnected():
#     device = InsoleDevice(device_address="AA:BB:CC:DD:EE:FF")
#     device.client = AsyncMock()
#     device.client.is_connected = False
#
#     with pytest.raises(Exception):
#         await device.read_pressure()


@pytest.mark.asyncio
async def test_write_to_csv(tmp_path):
    device = InsoleDevice(device_address="AA:BB:CC:DD:EE:FF")
    test_file = tmp_path / "test.csv"
    df = pd.DataFrame(
        [
            {
                "timestamp_python": 123,
                "ts_board": 456,
                "packet_counter": 1,
                "imu_data": [1, 2, 3],
            }
        ]
    )

    await device.write_to_csv(
        file_name=str(test_file),
        headers=["timestamp_python", "ts_board", "packet_counter", "imu_data"],
        df=df,
        log_tag="IMU",
    )

    assert test_file.exists()
    assert test_file.read_text().count("imu_data") == 1
