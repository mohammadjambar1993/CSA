import re
import struct
from typing import Any, List, Optional, Tuple, Union

from mbst.core.basic_definitions.fw_config_types import OpCodeType, SystemType


def generate_opcode(opcode: bytes) -> str:
    """
    Converts a given opcode (byte sequence) into a hexadecimal string.

    Args:
        opcode (bytes): The opcode represented as bytes.

    Returns:
        str: The hexadecimal representation of the opcode.
    """
    opcode_hex = "".join(f"{byte:02x}" for byte in opcode)
    return opcode_hex


def float_to_hex(f: float) -> str:
    """
    Converts a floating-point number into its little-endian hexadecimal representation.

    Args:
        f (float): The floating-point number to convert.

    Returns:
        str: The hexadecimal representation of the float in little-endian format.
    """
    hex_value = hex(struct.unpack(">I", struct.pack(">f", f))[0])[
        2:
    ]  # [2:] removes the '0x' prefix
    hex_value = hex_value.zfill(8)  # Pad with leading zeros if necessary
    hex_value = "".join(
        reversed([hex_value[i : i + 2] for i in range(0, len(hex_value), 2)])
    )  # Reverse byte order
    return hex_value


def hex_to_float(hex_data: List[int]) -> float:
    """
    Converts a list of hexadecimal bytes into a floating-point number.

    Args:
        hex_data (List[int]): List of integers representing hexadecimal byte values.

    Returns:
        float: The corresponding floating-point value.
    """
    hex_string = "".join(f"{b:02x}" for b in hex_data)
    # print("test 1: ", hex_string)
    # hex_str = re.sub(r"^0+|0+$", "", hex_string)
    # print("test 2: ", hex_str)
    int_value = int(hex_string, 16)
    # print("test 3: ", int_value)
    # Convert the integer value to a floating-point value
    float_value = struct.unpack("!f", struct.pack("!I", int_value))[0]
    # print("Calculated Float: ", float_value)
    return float_value


def module_parser(data: List[int]) -> Tuple[str, str]:
    """
    Parses a list of integers representing module data and extracts firmware version and battery level.

    Args:
        data (List[int]): List of integers representing module-related data.

    Returns:
        Tuple[str, str]: A tuple containing:
            - firmwareVer (str): The firmware version formatted as "X.Y.Z".
            - battery_lvl (str): The battery level as a percentage string.
    """
    firmwareVer = str(data[0]) + "." + str(data[1]) + "." + str(data[2])
    battery_lvl = str(data[7]) + "%"
    return firmwareVer, battery_lvl


# TODO SPREV - Failed Dynamic Test
def response_parser(
    data: bytes,
) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[float]]:
    """
    Parses response data from the device and extracts:
    - `opcode`: The operation type as a string.
    - `status`: The response status as an integer.
    - `system`: The system type as an integer (or None).
    - `float_value`: A floating-point value if applicable.

    Args:
        data (bytes): Response data from the BLE device.

    Returns:
        Tuple[Optional[str], Optional[int], Optional[int], Optional[float]]: Parsed values.
    """
    if len(data) < 4:  # Minimum length for opcode + status + system
        print("[ERROR][response_parser] Response data too short to parse.")
        return None, None, None, None

    opcode: Optional[str] = None
    status: Optional[int] = data[2]
    system: Optional[int] = None
    float_value: Optional[float] = None

    # Step 1: Identify the opcode
    for op in OpCodeType:
        if data[:2] == bytes(op.value):
            opcode = op.name
            break

    if opcode is None:
        print("[ERROR][response_parser] Opcode not recognized in response data.")
        return None, status, None, None

    # Step 2: Identify system type if applicable
    if opcode in {"CONFIG_SENSOR", "READ_SENSOR_CFG"}:
        system_byte = data[3]
        for sys in SystemType:
            if system_byte == sys.value:
                system = int(sys.value)
                break

    # Step 3: Extract float value if system == 0
    if system == 0 and len(data) > 4:
        reversed_data = list(data[4:-1][::-1])  # Ensure a list is passed
        try:
            float_value = hex_to_float(reversed_data)
        except (ValueError, struct.error):
            print(
                "[ERROR][response_parser] Failed to parse float value from response data."
            )

    return opcode, status, system, float_value


def default_sampling_rate(fwVer):  # TODO SPREV - Failed Dynamic Test
    if fwVer in ["0.0.0", "0.1.0", "0.2.0", "0.2.100"]:
        return 100
    elif fwVer in ["0.2.1"]:
        return 1802
    elif fwVer in ["0.3.0"]:
        return 6250
    else:
        return 9091
