import sys
from io import StringIO
from unittest.mock import patch

import pytest

from src.mbst.bleusb_comm_toolkit import cli


@patch("src.mbst.bleusb_comm_toolkit.cli.run_ble")
def test_cli_ble(mock_run_ble):
    test_args = ["cli.py", "ble"]
    with patch.object(sys, "argv", test_args):
        cli.main()
    mock_run_ble.assert_called_once()


@patch("src.mbst.bleusb_comm_toolkit.cli.run_redis")
def test_cli_redis(mock_run_redis):
    test_args = ["cli.py", "redis"]
    with patch.object(sys, "argv", test_args):
        cli.main()
    mock_run_redis.assert_called_once()


@patch("src.mbst.bleusb_comm_toolkit.cli.run_consumer")
def test_cli_consumer(mock_run_consumer):
    test_args = ["cli.py", "consumer"]
    with patch.object(sys, "argv", test_args):
        cli.main()
    mock_run_consumer.assert_called_once()


def test_cli_invalid_command():
    test_args = ["cli.py", "invalid"]
    with patch.object(sys, "argv", test_args):
        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            with pytest.raises(SystemExit) as exit_info:
                cli.main()
            assert exit_info.value.code == 2
            assert "invalid choice" in mock_stderr.getvalue()


def test_cli_version_output():
    test_args = ["cli.py", "--version"]
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit) as exit_info:
            cli.main()
        assert exit_info.value.code == 0
