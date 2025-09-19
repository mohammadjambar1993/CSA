from unittest.mock import MagicMock, patch

import pytest

import src.mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.consumer_redis as consumer_redis_module
import src.mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.producer_redis as producer_redis_module


@patch(
    "src.mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.producer_redis.connect_to_redis"
)
def test_producer_redis_key(mock_connect):
    mock_redis = MagicMock()
    mock_connect.return_value = mock_redis

    key = producer_redis_module.producer_redis_key(
        "test_value", device="pressure_bed", deviceId="001", char="sensorX"
    )
    mock_redis.set.assert_called_once()
    assert "pressure_bed-001-sensorX" in key


@patch(
    "src.mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.producer_redis.connect_to_redis"
)
def test_producer_redis_streams(mock_connect):
    mock_redis = MagicMock()
    mock_connect.return_value = mock_redis

    stream_key = producer_redis_module.producer_redis_streams(
        "some_json_data", device="pressure_bed", device_id="001", char="sensorX"
    )
    mock_redis.xadd.assert_called_once()
    assert stream_key.startswith("stream:pressure_bed:001")


@patch(
    "src.mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.consumer_redis.connect_to_redis"
)
def test_consumer_redis_key(mock_connect):
    mock_redis = MagicMock()
    mock_connect.return_value = mock_redis
    mock_redis.get.return_value = b"test_value"

    value = consumer_redis_module.consumer_redis_key("some_key", device="pressure_bed")
    mock_redis.get.assert_called_once()
    assert value == b"test_value"


@patch(
    "src.mbst.bleusb_comm_toolkit.in_memory_database.redis_functions.consumer_redis.connect_to_redis"
)
def test_consumer_redis_streams(mock_connect):
    mock_redis = MagicMock()
    mock_connect.return_value = mock_redis

    mock_redis.xrange.return_value = [
        (
            b"1234567890-0",
            {
                b"characteristic": b"sensorX",
                b"timestamp": b"1640995200000",
                b"value": b"sample_data",
                b"key": b"key123",
            },
        )
    ]

    result = consumer_redis_module.consumer_redis_streams(
        device="pressure_bed", device_id="001", char="sensorX"
    )
    assert result[0]["characteristic"] == "sensorX"
