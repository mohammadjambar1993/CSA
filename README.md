---

# Myant Biomedical Software Team (MBST) bleusb_comm_toolkit

bleusb_comm_toolkit is a Python library designed to manage communication with USB and BLE devices and handle data management tasks. It’s suitable for applications that require flexible connectivity options for device communication and data streaming, and provides support for modular, purpose-built communication channels.

## Modules

This library consists of three main modules:

1. **USB Communication** (`usb_communication`): Provides functions and classes to establish and manage USB device connections.
2. **BLE Management** (`ble_management`): Enables BLE (Bluetooth Low Energy) connectivity, managing device discovery, connection, and data transmission.
3. **Data Management** (`data_management`): Includes utilities for consuming and producing data streams, designed to integrate with either USB or BLE for continuous data management.
3. **In Memory Database** (`in_memory_database`): Includes utilities for consuming and producing binary data, designed to integrate with Redis In memory database running on docker container.
---

## Installation

### General Installation
To install the base package with all modules:
```bash
pip install git+<https://github.com/myantandco/mbst_bleusb_comm_toolkit.git[all]>
```

### Specific Release Installation
You must install the entire package for complete functionality

- **Pip install via terminal**:
```bash
pip install git+<https://github.com/myantandco/mbst_bleusb_comm_toolkit.git@0.0.4#egg=bleusb_comm_toolkit[all]>
```

- **Pip install via terminal**:
```bash
pip install "bleusb_comm_toolkit[all] @ git+https://github.com/myantandco/mbst_bleusb_comm_toolkit.git@0.0.4"
```

- **requirements.txt in project**:
```requirements
git+https://github.com/myantandco/mbst_bleusb_comm_toolkit.git@0.0.4#egg=bleusb_comm_toolkit[all]
```

---
## Imports
### High Level Imports on Package Root Level
```python
from bleusb_comm_toolkit import SerialCommunication
from bleusb_comm_toolkit import LiveConsumer
```
### Low Level Import
```python
from bleusb_comm_toolkit.usb_communication import SerialCommunication
from bleusb_comm_toolkit.data_management import LiveConsumer
```
---

## Usage

### High Level Encapsulation Communication with Data Management
To use USB or BLE communication alongside data management, follow this example:

```python
import os
import asyncio
from bleusb_comm_toolkit import LiveConsumer


async def handle_incoming_data(data):
    print("Callback received data: ", data)


async def main():
    # Use data management functionalities
    await consumer.activate_device()

    try:
        await consumer.stream_data()

    except KeyboardInterrupt:
        print("Stopping monitoring...")
        await consumer.stop_stream_data()
        await consumer.disconnect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    consumer = None

    try:
        # Initialize Data Consumer Communication and Define connection type ("usb", "ble", "redis")
        # consumer = LiveConsumer(loop=loop, connection="usb", device="pressure_bed", cwd=os.getcwd(), callback=handle_incoming_data)
        # consumer = LiveConsumer(loop=loop, connection="ble", device="insole", cwd=os.getcwd(), callback=handle_incoming_data)
        consumer = LiveConsumer(loop=loop, connection="ble", device="pressure_bed", cwd=os.getcwd(), callback=handle_incoming_data)
   
        loop.run_until_complete(main())
        loop.run_forever()

    except KeyboardInterrupt:
        print("Shutdown initiated.")

    finally:
        loop.close()
```

### Granular USB Communication with Data Management

To use direct USB communication, follow this example:

```python
from bleusb_comm_toolkit.usb_communication import SerialCommunication
import asyncio

loop = asyncio.get_event_loop()

# Callback Function to access data
def new_data(self, data):
    print(data)

async def stream_data(self):
    await usb_device.start_notify()
    
# Initialize USB communication
usb_device = SerialCommunication(port="COM3", device="mat", loop=loop, consumer_callback=new_data)
usb_device.connect()
usb_device.enable_acquisition()
asyncio.ensure_future(stream_data(), loop)


```

### BLE Communication Only

If you only need BLE functionality without data management:

```python
import asyncio
import logging
import os
import sys
from bleusb_comm_toolkit import BLEDevice, create_ble_device, LogManager

device_instance = None


async def handle_incoming_data(data, device=None, device_addr=None, char=None, timestamp=None):
    if isinstance(data, list):
        print("Callback received data: ", data)
        # msg = await producer_json(data=data, url=dash_heatmap_url)
        # print(msg)
    else:
        print("Callback received data: ", data)


async def main(cwd=None):
    global device_instance
    cwd = cwd or os.getcwd()
    logger = LogManager(cwd).logger
    loop = asyncio.get_event_loop()
    device = BLEDevice(loop=loop, cwd=cwd, logger=None)

    device_addresses, modality = await device.scan_for_devices(scan_time=3.0, scan_attempts=1)
    if not device_addresses or not modality:
        logging.error("No compatible BLE devices found; exiting.")
        return
    device_instance = create_ble_device(device_addresses[0], modality, loop=loop, logger=logger,
                                        callback=handle_incoming_data)
    try:
        print(f"device addr {device_addresses[0]}, modality {modality}, device type {type(device_instance)}")
        await device_instance.connect()
        await device_instance.initialize_device()
        await device_instance.start_acquisition()
    except Exception as e:
        logging.error(f"Error while setting up device: {e}")
    except KeyboardInterrupt:
        logging.error("Keyboard Interrupt received, shutting down.")
        if device_instance:
            await device_instance.disconnect()
        return device_instance


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Shutdown initiated.")
        if device_instance is not None:
            loop.run_until_complete(device_instance.shutdown())
        else:
            sys.exit()
    finally:
        pending_tasks = asyncio.all_tasks(loop)
        loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
        loop.close()
```

### Redis In Memory Database

If you only need BLE functionality without data management:

```python
from bleusb_comm_toolkit import insert_and_retrieve_message_streams
insert_and_retrieve_message_streams("Example Script test", device="undefined")
```

---

## Module Descriptions

### USB Communication (`usb_communication`)

This module allows you to connect to and manage USB-connected devices. Key functionality includes:
- Device connection and disconnection
- Data transmission and reception over USB

### BLE Management (`ble_management`)

The BLE management module facilitates Bluetooth Low Energy communication, with functionalities such as:
- Discovering BLE devices
- Establishing BLE connections
- Sending and receiving data over BLE

### Data Management (`data_management`)

The Data Management module provides classes and functions for handling streaming data, including:
- `LiveConsumer` for consuming real-time data streams
- `produce_message` for producing data messages for both USB and BLE connections

### In Memory Database (`in_memory_database`)

The Data Management module provides classes and functions for handling streaming data, including:
- `insert_and_retrieve_message_streams` utilizes producer and consumer redis functionalities to insert binary data into redis database and retrieve data

---

## Example Project Directory Structure

Here’s an example of the project structure for reference:

```
mbst_bleusb_comm_toolkit/
├── src/mbst/bleusb_comm_toolkit/                
│   ├── __init__.py
│   ├── usb_communication/
│   │   ├── __init__.py
│   │   ├── usb_comm.py
│   │   ├── device_manager.py
│   │   └── chars/
│   ├── ble_management/
│   │   ├── __init__.py
│   │   └── ble_manager.py
│   ├── data_management/
│   │   ├── __init__.py
│   │   ├── consumer.py
│   │   └── producer.py
│   ├── in_memory_database/
│   │   ├── __init__.py
│   │   ├── insert_and_retrieve_message.py
│   │   └── redis_functions/
├── examples/                
│   ├── example_ble.py
│   ├── example_consumer.py
│   ├── example_dual_consumer.py
│   ├── example_redis.py
├── tests/                
│   ├── test_ble.py
│   ├── test_consumer.py
│   ├── test_dual_consumer.py
│   ├── test_redis.py
├── README.md
├── requirements.txt
├── .env
└── setup.py
```

---
## Author
Susan Peters
susan.peters@myant.ca

---
# Developer Usage
## `setup.py` Configuration

## Installation Instructions

To install the bleusb_comm_toolkit package, follow these steps:

### Prerequisites

Ensure you have Python installed on your system (version 3.9). You can download Python from the official website: [python.org](https://www.python.org/downloads/).

### Clone the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/myantandco/mbst_bleusb_comm_toolkit.git
cd mbst_bleusb_comm_toolkit


### Install the Package

You can install the package using `setup.py`. This will also install all required dependencies. Run the following command:

```bash
pip install .
```

This command installs the package in the current directory, including all common dependencies specified in `setup.py`.


### Verify Installation

To verify that the installation was successful, you can run a simple Python command:

```bash
python -c "import bleusb_comm_toolkit; print('bleusb_comm_toolkit installed successfully!')"
```

If you see the message, your installation was successful.

## Update Readme
Update the `README.md` file to reflect major changes in information about the module

## Updating and Implementing Code
Add your module's code to the project.

1. **Add Code Files:**

   - Place your Python code inside `src/mbst/bleusb_comm_toolkit/`.

2. **Update `__init__.py`:**

   - In `src/mbst/bleusb_comm_toolkit/__init__.py`, import the main classes or functions you want to expose:

     ```python
     from .your_module_code import YourMainClass, your_main_function
     ```

3. **Follow Coding Standards:**

   - Ensure your code adheres to PEP 8 guidelines.
   - Include type hints and docstrings.

##  Version Control

Follow best practices for version control.

1. **Branching Strategy:**

   - Use feature branches for development.
   - The name of every of your branches should start with your initials, followed by a '/'. For instane: 'jm/new_branch'
   - Merge changes to `main` via pull requests.

3. **Commit Messages:**

   - Write clear and descriptive commit messages.

4. **Tagging Releases:**

   - Tag releases in Git with version numbers, e.g., `0.1.0`.

     ```bash
     git tag 0.1.0
     git push origin 0.1.0
     ```
     
5. **Update `setup.cfg`:**
Update the version number of setup.cfg to be consistent with the tagged release 
   - Open `setup.cfg` and modify the `[metadata]` section:

        ```ini
        [metadata]
        name = bleusb_comm_toolkit
        version = 0.1.0
        ```
     

## Running Tests and Checks

Ensure your module passes all tests and code quality checks.

### 1. **Create Unit Tests:**

   - Write tests in the `tests/` directory, e.g., `tests/test_your_module_code.py`.

### 2. **Install Development Dependencies:**

   ```bash
    pip install -e .[dev]
   ```
### 3. Run Tests

```bash
tox -e py39
```

### 4. Run Code Formatting and Linting

- **Format code:**

  ```bash
  tox -e format-imports -e format-code 
  ```

- **Check code formatting:**

  ```bash
  tox -e check-format
  ```

### 5. Run Static Type Checking

```bash
tox -e static-analysis
```
---

### Troubleshooting

If you encounter any issues during installation, please check the following:

- Ensure that your Python and pip versions are up to date.
- Make sure you have the necessary permissions to install packages on your system.
- If using a virtual environment, ensure it's activated before running the installation commands.

For further assistance, feel free to raise an issue on the GitHub repository.
