import pytest

from src.mbst.bleusb_comm_toolkit.ble_management.Devices.ble_device import BLEDevice
from src.mbst.bleusb_comm_toolkit.ble_management.Devices.device_factory import (
    create_ble_device,
)
from src.mbst.bleusb_comm_toolkit.ble_management.Devices.insole_device import (
    InsoleDevice,
)
from src.mbst.bleusb_comm_toolkit.ble_management.Devices.pressure_device import (
    PressureMatDevice,
)


def test_create_pressure_mat_device():
    device = create_ble_device("AA:BB:CC:DD:EE:FF", "pressure_bed")
    assert isinstance(device, PressureMatDevice)
    assert device.device == "pressure_bed"


def test_create_insole_device():
    device = create_ble_device("AA:BB:CC:DD:EE:FF", "insole")
    assert isinstance(device, InsoleDevice)
    assert device.device == "insole"


def test_create_ble_base_device():
    device = create_ble_device("AA:BB:CC:DD:EE:FF", "ble_base")
    assert isinstance(device, BLEDevice)
    assert device.device == "ble_base"


def test_create_device_with_unknown_modality():
    device = create_ble_device("AA:BB:CC:DD:EE:FF", "unknown")
    assert isinstance(device, BLEDevice)
    assert device.device == "unknown"


def test_create_device_with_invalid_modality_and_no_base_class():
    with pytest.raises(ValueError):
        create_ble_device("AA:BB:CC:DD:EE:FF", "invalid", base_device_class=None)
