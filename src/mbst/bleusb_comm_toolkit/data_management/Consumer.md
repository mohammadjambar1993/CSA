
# LiveConsumer Class

The `LiveConsumer` module provides a way to manage live data acquisition from USB devices. It utilizes asynchronous programming to handle data streaming and logging, making it suitable for real-time applications. The module is designed to integrate seamlessly with the `usb_communication` package for USB communication.

## Features

- **Asynchronous Data Streaming**: Enables real-time data acquisition using Python's `asyncio` library.
- **Logging**: Automatically creates a log file for capturing information and errors during data acquisition.
- **Device Management**: Supports connecting to and disconnecting from USB devices.

## Class: `LiveConsumer`

### Initialization

To create an instance of the `LiveConsumer` class, provide the necessary parameters. If no parameters are specified, default values will be used.

```python
live_consumer = LiveConsumer(
    loop=asyncio.get_event_loop(),  # Event loop for asynchronous operations
    cwd=os.getcwd(),                 # Current working directory for logging
    connection='usb',                # Connection type: 'usb'
    device='YourDeviceName',          # Specify the device name
    config=None,                     # Additional configuration if needed
    callback=your_callback_function   # A callback function to handle new data
)
```

### Parameters

- `loop` (asyncio.AbstractEventLoop): The event loop to use for asynchronous operations (default: `None`).
- `cwd` (str): The current working directory where logs will be stored (default: `None`).
- `connection` (str): Type of connection, either `'usb'` (default).
- `device` (str): Name of the device to connect (default: `None`).
- `config`: Configuration parameters for the device (default: `None`).
- `callback` (callable): Function to call when new data is received (default: `None`).

### Methods

- `activate_device()`: Activates the USB device and starts data acquisition.
- `stream_data()`: Begins streaming data from the connected USB device.
- `stop_stream_data()`: Stops data acquisition from the USB device.
- `disconnect()`: Disconnects the USB device.
- `new_data(data)`: Callback function that processes the incoming data.

### Example Usage

Here’s an example of how to use the `LiveConsumer` class:

```python
import asyncio
from bleusb_comm_toolkit import LiveConsumer  # Assuming LiveConsumer is in data_management module


async def process_data(data):
    # Handle incoming data
    print("Received data:", data)


async def main():
    live_consumer = LiveConsumer(
        loop=asyncio.get_event_loop(),
        cwd='.',  # Current directory for logging
        connection='usb',
        device='YourDeviceName',
        callback=process_data
    )

    await live_consumer.activate_device()  # Activate the device to start data acquisition
    # Add additional logic here, such as waiting for data or processing it


if __name__ == "__main__":
    asyncio.run(main())
```

## Installation

To use this module, ensure you have the required libraries installed. You can install them using pip:

```bash
pip install asyncio
```

Additionally, make sure that the `usb_communication` package is properly installed and configured.

## Logging

The module will create a logs directory in the current working directory and generate a log file named `Myant_PyQtBLE_logfile_<timestamp>.log`. This file will contain information and errors related to data acquisition.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request to enhance this module.

## Author

Susan Peters
susan.peters@myant.ca 
```
