import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.mbst.bleusb_comm_toolkit.ble_management.Devices.ble_device import BLEDevice


@pytest.mark.asyncio
@patch(
    "src.mbst.bleusb_comm_toolkit.ble_management.Devices.ble_device.BleakScanner.discover"
)
async def test_scan_for_devices_found(mock_discover):
    mock_device = MagicMock()
    mock_device.metadata = {"uuids": ["1234"]}
    mock_device.address = "AA:BB:CC:DD:EE:FF"
    mock_discover.return_value = [mock_device]

    device = BLEDevice(device=None, device_address="AA:BB:CC:DD:EE:FF")
    addresses, modality = await device.scan_for_devices(service_uuid="1234")

    assert addresses == ["AA:BB:CC:DD:EE:FF"]
    assert modality == "unknown_device_type"


@pytest.mark.asyncio
@patch(
    "src.mbst.bleusb_comm_toolkit.ble_management.Devices.ble_device.BleakScanner.discover",
    side_effect=Exception("Scan error"),
)
async def test_scan_for_devices_error(mock_discover):
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    addresses, modality = await device.scan_for_devices(
        service_uuid="1234", scan_attempts=1
    )
    assert addresses == []
    assert modality is None


@pytest.mark.asyncio
async def test_connect_success():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = False
    device.client.connect = AsyncMock()

    async def fake_connect():
        device.client.is_connected = True

    device.client.connect.side_effect = fake_connect

    await asyncio.wait_for(device.connect(), timeout=2.0)
    assert device.sensor_device.flags.is_connected is True


@pytest.mark.asyncio
async def test_connect_timeout():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = False

    # Simulate a hanging connect method
    async def hanging_connect():
        await asyncio.sleep(999)

    device.client.connect.side_effect = hanging_connect

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(device.connect(), timeout=2.0)


@pytest.mark.asyncio
async def test_start_acquisition():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.clear_data = MagicMock()
    device.generate_csv_files = MagicMock()
    device.start_read = AsyncMock()
    device.activate_services = AsyncMock()

    await device.start_acquisition()

    device.clear_data.assert_called_once()
    device.generate_csv_files.assert_called_once()
    device.start_read.assert_called_once()
    device.activate_services.assert_called_once()


@pytest.mark.asyncio
async def test_activate_services_success():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.INFORMATION = "info_uuid"

    await device.activate_services()
    device.client.start_notify.assert_called_once()


@pytest.mark.asyncio
async def test_configure_success():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = True
    device.chars = {"COMMAND": "cmd_uuid"}
    device.stop_all_notifications = AsyncMock()
    device.read_sensor_config = AsyncMock()
    device.start_read = AsyncMock()
    device.activate_services = AsyncMock()

    await device.configure("aabbcc")

    device.stop_all_notifications.assert_called_once()
    device.client.write_gatt_char.assert_called_once()
    device.read_sensor_config.assert_called_once()
    device.start_read.assert_called_once()
    device.activate_services.assert_called_once()


@pytest.mark.asyncio
async def test_stop_acquisition():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = True
    device.stop_all_notifications = AsyncMock()
    device.force_save = AsyncMock()

    await device.stop_acquisition()

    device.stop_all_notifications.assert_called_once()
    device.force_save.assert_called_once()


@pytest.mark.asyncio
async def test_stop_all_notifications():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = True
    device.sensor_device.flags.information_notifications_started = True
    device.chars = {"INFORMATION": "info_uuid"}

    await device.stop_all_notifications()

    device.client.stop_notify.assert_called_once_with("info_uuid")
    assert device.sensor_device.flags.information_notifications_started is False


@pytest.mark.asyncio
async def test_force_save():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.write_to_csv = MagicMock()
    device.clear_data = MagicMock()

    await device.force_save()

    device.write_to_csv.assert_called_once()
    device.clear_data.assert_called_once()


@pytest.mark.asyncio
async def test_disconnect():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.client = AsyncMock()
    device.client.is_connected = True
    device.stop_acquisition = AsyncMock()
    device.client.disconnect = AsyncMock()

    await device.disconnect()

    device.stop_acquisition.assert_called_once()
    device.client.disconnect.assert_called_once()
    assert device.sensor_device.flags.is_connected is False


@pytest.mark.asyncio
async def test_shutdown():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.disconnect = AsyncMock()
    device.clear_data = MagicMock()

    await device.shutdown()

    device.disconnect.assert_called_once()
    device.clear_data.assert_called_once()


def test_generate_dynamic_payload():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.chars = {"CAPACITORS": {"low": "ff"}}

    payload = device.generate_dynamic_payload(
        opcode=[0x01, 0x02], sampling_rate=10.5, feedback_capacitor="low", system="AA"
    )
    assert isinstance(payload, str)
    assert "AA" in payload


def test_response_notification_handler_parent():
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device._loop = asyncio.get_event_loop()
    device.client = MagicMock()
    device.client.is_connected = True
    device.response_notification_handler = AsyncMock()

    device.response_notification_handler_parent("sender", b"\x00\x01")
    assert device.response_notification_handler.call_count == 1


@pytest.mark.asyncio
async def test_generate_csv_files(tmp_path):
    device = BLEDevice(device="pressure", device_address="AA:BB:CC:DD:EE:FF")
    device.cwd = str(tmp_path)
    device.timezone = None

    device.generate_csv_files()

    assert device.sensor_device.csv.file_name_csv.endswith(".csv")
    assert device.sensor_device.csv.file_name_imu.endswith(".csv")
    assert device.sensor_device.csv.file_name_sflp.endswith(".csv")
    assert device.sensor_device.csv.file_name_shoe_press.endswith(".csv")
