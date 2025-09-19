# Redis-to-MQTT Synchronization Service

This service monitors Redis streams on a source Redis instance and publishes new messages to an MQTT broker.

## Features

- **Stream Processing**: Consumes messages from source Redis streams using consumer groups.
- **MQTT Publishing**: Publishes consumed messages to dynamically generated MQTT topics.
- **Multi-DB Support**: Can discover and process streams across multiple databases on the source Redis instance.
- **Automatic Stream Discovery**: Scans source Redis databases for streams matching configurable patterns.
- **Robust Connection Handling**: Implements connection health checks, automatic recovery, and exponential backoff for both Redis and MQTT connections.
- **Null Safety**: Strong type checking and null-safety throughout the Redis service to prevent null reference errors.
- **Async/Await Patterns**: Proper async/await usage in all Redis operations for optimal performance and reliability.
- **Configuration Management**: Comprehensive configuration system supporting both environment variables and configuration files.
- **Rate Limiting**: Token bucket rate limiting for Redis operations to prevent overloading the servers.
- **Message Filtering**: Optional message filtering based on field values.
- **Validation**: Data validation for both configuration settings and stream messages.
- **Graceful Shutdown**: Proper handling of shutdown signals with clean resource cleanup.
- **Dockerized**: Runs as a Docker container managed via `docker-compose`.

## Architecture

The service employs a modular, layered architecture:

### Core Layer
- **`SyncOrchestrator`**: Coordinates the overall synchronization process. Manages the main loop, applies filtering/validation, and handles the service lifecycle.
- **`Exceptions`**: Defines custom exception types for better error handling throughout the application.

### Services Layer
- **`RedisService`**: Handles connections to the source Redis instance(s). Manages stream discovery, consumer group creation, message reading, and acknowledgments. Includes health checks, retry logic, and null-safety. All operations follow proper async/await patterns.
- **`MqttService`**: Handles the connection to the MQTT broker. Manages publishing messages. Includes health checks and retry logic.
- **`Interfaces`**: Defines abstract interfaces for Redis and MQTT services, enabling dependency injection and easier testing.

### Configuration Layer
- **`Settings`**: Centralizes loading and parsing of configuration parameters from environment variables and configuration files.

### Utilities Layer
- **`RateLimiter`**: Implements a token bucket rate limiter to control the rate of Redis operations.

### Entry Points
- **`main.py`**: The main module providing a robust entry point with argument parsing, configuration validation, startup checks, and graceful shutdown handling.
- **`sync_redis_with_mqtt.py`**: A simplified CLI wrapper for the main module.

## Directory Structure

```
redis_mqtt_sync/
├── docker/
│   ├── docker-compose.yml      # Docker Compose configuration
│   └── Dockerfile              # Dockerfile for the sync service
├── env.conf                    # Configuration file (YOU create this)
├── scripts/
│   └── manage_redis_with_sync_to_mqtt.py  # Start/stop script
├── src/
│   ├── __init__.py             # Package initialization
│   ├── main.py                 # Main service entry point
│   ├── sync_redis_with_mqtt.py # CLI wrapper
│   ├── config/                 # Configuration management
│   │   ├── __init__.py
│   │   └── settings.py         # Configuration handling
│   ├── core/                   # Core functionality
│   │   ├── __init__.py
│   │   ├── exceptions.py       # Custom exceptions
│   │   └── orchestrator.py     # Main sync orchestration
│   ├── services/               # Service implementations
│   │   ├── __init__.py
│   │   ├── interfaces.py       # Service interfaces
│   │   ├── redis_service.py    # Redis service implementation
│   │   └── mqtt_service.py     # MQTT service implementation
│   └── utils/                  # Utilities
│       ├── __init__.py
│       └── rate_limiter.py     # Rate limiting utility
├── requirements.txt            # Python dependencies
├── README.md                   # This file
└── logs/                       # Log files (created at runtime, if configured)
```

## Configuration (`env.conf`)

Configure the service by creating and editing the `redis_mqtt_sync/env.conf` file:

```
# === General Settings ===
LOG_LEVEL=INFO
LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s

# === Redis Settings ===
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_USERNAME=
REDIS_SSL=false
REDIS_POOL_SIZE=10
REDIS_MAX_RETRIES=5
REDIS_RETRY_INTERVAL=5
REDIS_RATE_LIMIT=1000
REDIS_DBS_TO_SEARCH=0,1,2

# === MQTT Settings ===
MQTT_BROKER_HOST=localhost
MQTT_BROKER_PORT=1883
MQTT_CLIENT_ID=redis-mqtt-sync
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_USE_TLS=false
MQTT_QOS=1
MQTT_RETAIN=false
MQTT_BASE_TOPIC=redis
MQTT_FILTER_FIELD=
MQTT_FILTER_VALUES=

# === Stream Settings ===
REDIS_STREAMS=
STREAM_AUTO_DISCOVERY=true
STREAM_GROUP=mqtt-group
STREAM_CONSUMER=mqtt-sync
STREAM_BATCH_SIZE=100
STREAM_READ_TIMEOUT_MS=1000
```

## Installation

### Manual Installation

1. Clone the repository.
2. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create and configure `redis_mqtt_sync/env.conf` based on your environment.
5. Run the service:
   ```bash
   cd redis_mqtt_sync
   python -m src.sync_redis_with_mqtt
   ```

### Docker Installation

1. Clone the repository.
2. Create and configure `redis_mqtt_sync/env.conf` based on your environment.
3. Use the management script to build and start the services:
   ```bash
   cd redis_mqtt_sync/scripts
   python manage_redis_with_sync_to_mqtt.py start
   ```

## Usage

### Command Line Arguments

The service supports the following command line arguments:

```bash
python -m src.main --log-level=DEBUG --config-file=/path/to/env.conf
```

- `--log-level`: Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--config-file`: Specify the path to the configuration file

### Using the Management Script

Use the Python management script located in the `redis_mqtt_sync/scripts` directory to manage the Docker services.

#### Start the Service
```bash
cd redis_mqtt_sync/scripts

# Start all services (sync app, redis-cache, mqtt-broker defined in docker-compose)
# This builds the Docker image if necessary.
python manage_redis_with_sync_to_mqtt.py start

# Start only the sync service (connect to external Redis/MQTT)
# Assumes external services are configured in env.conf
python manage_redis_with_sync_to_mqtt.py start --services redis-mqtt-sync

# Start sync service and local Redis only
python manage_redis_with_sync_to_mqtt.py start --services redis-mqtt-sync redis-cache

# Start sync service and local MQTT Broker only
python manage_redis_with_sync_to_mqtt.py start --services redis-mqtt-sync mqtt-broker
```

#### Stop the Service
```bash
cd redis_mqtt_sync/scripts
python manage_redis_with_sync_to_mqtt.py stop
```

#### View Logs
```bash
# View logs for all services managed by docker-compose
cd redis_mqtt_sync/docker
docker-compose logs -f

# View logs for the sync service specifically
docker-compose logs -f redis-mqtt-sync
```

## Message Flow and Processing

1. **Stream Discovery**: The service either uses configured stream names or discovers streams in Redis databases.
2. **Consumer Group Setup**: For each stream, a consumer group is created if it doesn't exist.
3. **Message Reading**: Messages are read from Redis streams using consumer groups.
4. **Validation**: Each message is validated before processing.
5. **Filtering**: If message filtering is configured, messages are filtered based on field values.
6. **Publishing**: Valid messages are published to MQTT topics constructed from the base topic and stream name.
7. **Acknowledgment**: Successfully processed messages are acknowledged in Redis.

### MQTT Topic Structure

MQTT topics are constructed using the following pattern:
```
{MQTT_BASE_TOPIC}/{stream_name_with_slashes}
```

For example:
- Redis stream `device:sensor:data` with base topic `redis` becomes MQTT topic `redis/device/sensor/data`

### Message Format

Messages published to MQTT have the following JSON format:
```json
{
  "stream": "device:sensor:data",
  "id": "1651234567890-0",
  "data": {
    "field1": "value1",
    "field2": "value2",
    "field3": 123
  },
  "timestamp": "2023-05-01T12:34:56.789012"
}
```

## Development

### Type Checking

This project uses static type checking with mypy to ensure code quality and prevent runtime errors. To run type checking:

```bash
cd redis_mqtt_sync
python -m mypy .
```

The codebase implements careful null-safety patterns, especially in the Redis service, to prevent null reference errors:
- Using `Optional` types to explicitly mark values that might be `None`
- Adding null checks before accessing methods on possibly `None` objects
- Proper handling of async/await patterns in all Redis operations

When modifying code, always maintain these patterns and run the type checker to verify type safety.

### Code Style

Follow these guidelines when contributing to the code:
- Use type annotations for all function parameters and return values
- Add proper docstrings for all modules, classes, and methods
- Maintain the modular architecture
- Follow async/await best practices for all I/O operations

## Advanced Features

### Rate Limiting

The service includes a token bucket rate limiter to prevent overloading Redis with too many operations. Configure the rate limit with the `REDIS_RATE_LIMIT` setting, which represents the maximum number of operations per second.

### Message Filtering

Configure message filtering with:
- `MQTT_FILTER_FIELD`: The field name to filter on
- `MQTT_FILTER_VALUES`: Comma-separated list of values to include

For example, to only publish messages with a "type" field value of "alert" or "warning":
```
MQTT_FILTER_FIELD=type
MQTT_FILTER_VALUES=alert,warning
```

### TLS/SSL Support

Both Redis and MQTT connections support TLS/SSL:
- For Redis: Set `REDIS_SSL=true`
- For MQTT: Set `MQTT_USE_TLS=true`

## Troubleshooting

### Connection Issues

- **Redis Connection Errors**: 
  - Verify Redis host, port, username, and password
  - Check if Redis server is reachable from your environment
  - Check if the Redis database numbers are correct

- **MQTT Connection Errors**:
  - Verify MQTT broker host, port, username, and password
  - Check if TLS settings are correct
  - Ensure the MQTT broker is reachable from your environment

### Processing Issues

- **No Messages Being Published**:
  - Check if Redis streams exist and contain messages
  - Verify consumer group creation works properly
  - Check if message filtering is configured correctly
  - Look for any validation errors in the logs

- **Performance Issues**:
  - Adjust the `REDIS_POOL_SIZE` for better connection pooling
  - Tune the `REDIS_RATE_LIMIT` for optimal throughput
  - Adjust `STREAM_BATCH_SIZE` based on message size and frequency

## Development and Extending

The modular architecture makes it easy to extend the service:

1. **Adding New Services**: Implement the appropriate interface in the `services` package
2. **Changing Message Processing**: Modify the `_process_single_stream` method in `orchestrator.py`
3. **Custom Validation**: Enhance the `validate_redis_data_for_mqtt` function in `orchestrator.py`
4. **Additional Settings**: Add new settings to the `DEFAULT_CONFIG` dictionary in `settings.py`

## Logging

The service uses Python's built-in logging framework. Configure the log level and format in `env.conf`:

```
LOG_LEVEL=INFO
LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s
```

Available log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

## License

Proprietary - Copyright © MbST