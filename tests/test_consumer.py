import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
from mbst.core.common_tools.raised_errors import ConfigurationError, DeviceNotFoundError

from src.mbst.bleusb_comm_toolkit import LiveConsumer


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def live_consumer_mock():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    callback_mock = AsyncMock()
    return LiveConsumer(
        loop=loop,
        connection="ble",
        callback=callback_mock,
    )


@patch("src.mbst.bleusb_comm_toolkit.data_management.consumer.create_ble_device")
def test_initialize_device_ble(mock_create_ble_device):
    mock_logger = Mock()
    mock_device = Mock()
    mock_create_ble_device.return_value = mock_device

    with patch(
        "src.mbst.bleusb_comm_toolkit.data_management.consumer.logging.getLogger",
        return_value=mock_logger,
    ):
        consumer = LiveConsumer(connection="ble")
        consumer.select_device("device_1", "modality_1")

    mock_create_ble_device.assert_called_once_with(
        "device_1",
        "modality_1",
        loop=consumer._loop,
        callback=consumer.new_data,
    )


@patch("src.mbst.bleusb_comm_toolkit.data_management.consumer.SerialCommunication")
def test_initialize_device_usb(mock_serial_comm):
    mock_logger = Mock()
    mock_serial_instance = Mock()
    mock_serial_comm.return_value = mock_serial_instance
    mock_device = Mock()
    mock_device.enable = Mock()

    with patch(
        "src.mbst.bleusb_comm_toolkit.data_management.consumer.logging.getLogger",
        return_value=mock_logger,
    ):
        consumer = LiveConsumer(connection="usb", comport="COM4")

    mock_serial_comm.assert_called_once_with(
        port="COM4",
        device=None,
        loop=consumer._loop,
        consumer_callback=consumer.new_data,
    )


@patch(
    "src.mbst.bleusb_comm_toolkit.ble_management.BLEDevice.scan_for_devices",
    new_callable=AsyncMock,
)
def test_scan_for_devices_empty(mock_scan, live_consumer_mock):
    mock_scan.return_value = ([], None)
    with pytest.raises(DeviceNotFoundError):
        asyncio.run(live_consumer_mock.scan_for_devices())


@patch(
    "src.mbst.bleusb_comm_toolkit.ble_management.BLEDevice.scan_for_devices",
    new_callable=AsyncMock,
)
def test_scan_for_devices(mock_scan_for_devices, live_consumer_mock):
    mock_scan_for_devices.return_value = (["device_1"], "modality_1")

    # Inject a mocked BLEDevice
    live_consumer_mock.device_instance = Mock()
    live_consumer_mock.device_instance.scan_for_devices = mock_scan_for_devices

    result = asyncio.run(live_consumer_mock.scan_for_devices())
    assert result == (["device_1"], "modality_1")
    mock_scan_for_devices.assert_called_once()


@patch("src.mbst.bleusb_comm_toolkit.data_management.consumer.create_ble_device")
def test_select_device(mock_create_ble_device, live_consumer_mock):
    mock_device = Mock()
    mock_create_ble_device.return_value = mock_device

    live_consumer_mock.select_device("device_1", "modality_1")

    mock_create_ble_device.assert_called_once_with(
        "device_1",
        "modality_1",
        loop=live_consumer_mock._loop,
        callback=live_consumer_mock.new_data,
    )


@patch("src.mbst.bleusb_comm_toolkit.data_management.consumer.create_ble_device")
def test_activate_device_ble(mock_create_ble_device, live_consumer_mock):
    mock_ble = Mock()
    mock_ble.connect = AsyncMock()
    mock_ble.initialize_device = AsyncMock()

    mock_create_ble_device.return_value = mock_ble

    live_consumer_mock.select_device("mock_addr", "modality_1")
    asyncio.run(live_consumer_mock.activate_device())

    mock_ble.connect.assert_awaited_once()
    mock_ble.initialize_device.assert_awaited_once()


@patch(
    "src.mbst.bleusb_comm_toolkit.ble_management.BLEDevice.start_acquisition",
    new_callable=AsyncMock,
)
def test_stream_data_ble(mock_start_acquisition, live_consumer_mock):
    asyncio.run(live_consumer_mock.stream_data())
    mock_start_acquisition.assert_called_once()


@patch(
    "src.mbst.bleusb_comm_toolkit.ble_management.BLEDevice.stop_acquisition",
    new_callable=AsyncMock,
)
def test_stop_stream_data_ble(mock_stop_acquisition, live_consumer_mock):
    asyncio.run(live_consumer_mock.stop_stream_data())
    mock_stop_acquisition.assert_called_once()


@patch(
    "src.mbst.bleusb_comm_toolkit.ble_management.BLEDevice.disconnect",
    new_callable=AsyncMock,
)
def test_disconnect_ble(mock_disconnect, live_consumer_mock):
    asyncio.run(live_consumer_mock.disconnect())
    mock_disconnect.assert_called_once()


def test_initialize_device_invalid():
    with pytest.raises(ConfigurationError):
        LiveConsumer(connection="invalid")
