from ...ble_management.Devices import BLEDevice
from ...ble_management.Devices.insole_device import InsoleDevice
from ...ble_management.Devices.pressure_device import PressureMatDevice


def create_ble_device(
    device_address,
    modality,
    loop=None,
    base_device_class=BLEDevice,
    cwd=None,
    logger=None,
    callback=None,
) -> BLEDevice:
    """
    Creates and returns a BLE device instance based on the specified modality.

    Args:
        device_address (str): The BLE address of the device.
        modality (str): The type of BLE device (e.g., "pressure_bed", "insole", "ble_base").
        loop: The event loop to use for asynchronous operations (optional).
        base_device_class (BLEDevice): The base BLE device class to use for generic BLE devices.
        cwd (str, optional): The current working directory.
        logger (optional): Logger instance for logging events.
        callback (optional): Callback function for handling received data.

    Returns:
        BLEDevice: An instance of the appropriate BLE device class.

    Raises:
        ValueError: If an unsupported modality is provided and no base device class is available.
    """

    device_instance: BLEDevice

    if modality == "pressure_bed":
        device_instance = PressureMatDevice(
            loop=loop,
            device_address=device_address,
            cwd=cwd,
            logger=logger,
            callback_func=callback,
        )
    elif modality == "insole":
        device_instance = InsoleDevice(
            loop=loop,
            device_address=device_address,
            cwd=cwd,
            logger=logger,
            callback_func=callback,
        )
    elif modality == "ble_base" and base_device_class:
        device_instance = base_device_class(
            loop=loop,
            device_address=device_address,
            cwd=cwd,
            logger=logger,
            callback_func=callback,
        )
    else:
        if base_device_class:
            device_instance = base_device_class(
                loop=loop,
                device_address=device_address,
                cwd=cwd,
                logger=logger,
                callback_func=callback,
            )
        else:
            raise ValueError(
                "Unsupported device modality and no base device class provided."
            )

    # Set the device modality on the instance
    device_instance.device = modality
    return device_instance
