from mbst.core.basic_definitions.device_types import make_mat_uuid, make_uuid

# Base Class
BLE_DEVICE_NAME = "BLEDevice"

SERVICES_UUIDS = {"pressure_bed": make_mat_uuid("9800"), "insole": make_uuid("9800")}

# Device states to represent various BLE connection statuses
DEVICE_STATE_CONNECTED = "Connected"
DEVICE_STATE_CONNECTING = "Connecting"
DEVICE_STATE_DISCONNECTED = "Disconnected"
DEVICE_STATE_DISCONNECTING = "Disconnecting"
DEVICE_STATE_STREAMING = "Streaming"

# BLE states to represent different connection statuses
BLE_STATE_ON = "on"
BLE_STATE_OFF = "off"
BLE_STATE_UNAUTHORIZED = "unauthorized"
BLE_STATE_UNKNOWN = "unknown"
BLE_STATE_TURNING_OFF = "turning_off"
BLE_STATE_TURNING_ON = "turning_on"

# Connection states to represent various stages of BLE connection management
CONNECTION_STATE_UNPAIRED = "unpaired"
CONNECTION_STATE_RECONNECTING = "reconnecting"
CONNECTION_STATE_INITIALIZING = "initializing"
CONNECTION_STATE_RECONNECT_SUCCESS = "success"
CONNECTION_STATE_RECONNECT_FAIL = "fail"
CONNECTION_STATE_INITIAL_DISCONNECT = "initDisconnect"
CONNECTION_STATE_SUBSEQUENT_DISCONNECT = "disconnect"
CONNECTION_STATE_MUST_UPGRADE = "upgrade"
CONNECTION_STATE_BROKEN_FIRMWARE = "broken"
CONNECTION_STATE_UPDATE_IN_PROGRESS = (
    "updating"  # Used to disable auto-reconnect during firmware upgrade
)
