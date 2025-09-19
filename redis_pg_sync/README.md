# PostgreSQL Synchronization Service

A robust service for synchronizing data from various sources to PostgreSQL, with a focus on Redis integration.

## Features

### Redis to PostgreSQL Synchronization
- **Stream Synchronization**
  - Automatic stream discovery
  - Consumer group management
  - Batch processing
  - Configurable stream patterns

- **Key-Value Synchronization**
  - Automatic key discovery
  - Pattern-based filtering
  - Efficient batch upserts

- **Robust Error Handling**
  - Exponential backoff for retries
  - Configurable retry limits
  - Comprehensive error logging

- **Rate Limiting**
  - Token bucket algorithm
  - Configurable rate limits
  - Thread-safe implementation

- **Metrics Collection**
  - Operation counts
  - Error tracking
  - Performance monitoring
  - Stream discovery statistics

### Startup Behavior
- **Configuration Validation**: Checks critical configuration parameters at startup.
- **Service Connectivity Checks**: Verifies reachability of Redis and PostgreSQL before starting.
- **Command-line Arguments**: Supports overriding configurations like log level and operational mode.

## Architecture

The service follows a clean, modular architecture with clear separation of concerns:

1. **Configuration Layer**
   - `config/settings.py`: Centralized configuration management with environment variable support
   - Configuration validation with sensible defaults
   - Support for runtime configuration via configure() function

2. **Service Layer**
   - `services/redis_service.py`: Handles Redis operations with robust error handling
     - Connection pooling with health checks
     - Stream discovery and consumer group management
     - Message reading and acknowledgment
     - Key-value operations
   - `services/postgres_service.py`: Manages PostgreSQL operations
     - Connection pooling with health checks
     - Schema setup and maintenance
     - Efficient batch operations with transaction support
     - Indexed table management for performance

3. **Core Layer**
   - `core/orchestrator.py`: Coordinates the synchronization process
     - Implements SyncOrchestrator with pluggable service dependencies
     - Provides synchronization logic with configurable modes
     - Manages service lifecycle
     - Implements backoff and retry logic
   - `core/exceptions.py`: Custom exception hierarchy for specific error handling

4. **Utility Layer**
   - `utils/rate_limiter.py`: Token bucket rate limiting algorithm
   - `utils/metrics.py`: Comprehensive metrics collection

5. **Application Layer**
   - `main.py`: Entry point with command-line argument support and dual operational modes:
     - Sync mode: Full synchronization including streams and key-values
     - Monitor mode: Stream-focused monitoring and processing

6. **Interface Layer**
   - `services/interfaces.py`: Abstract base classes defining service interfaces
     - RedisServiceInterface: Interface for Redis operations
     - PostgresServiceInterface: Interface for PostgreSQL operations

This architecture follows SOLID principles, particularly:
- **Single Responsibility**: Each class has a well-defined responsibility
- **Open/Closed**: Extendable through dependency injection and interfaces
- **Liskov Substitution**: Implementations properly fulfill their interfaces
- **Interface Segregation**: Clean interface definitions without unnecessary methods
- **Dependency Inversion**: High-level modules depend on abstractions

## Configuration

The service is configured through environment variables:

### Redis Configuration
- `REDIS_HOST`: Redis server host (default: localhost)
- `REDIS_PORT`: Redis server port (default: 6379)
- `REDIS_PASSWORD`: Redis password (default: None)
- `REDIS_DB`: Default Redis database (default: 0)
- `REDIS_POOL_SIZE`: Redis connection pool size (default: 10)
- `REDIS_DB_SEARCH_RANGE`: Range of Redis databases to search (e.g., "0-15")
- `SOURCE_REDIS_DB`: Source Redis database (default: 0)
- `REDIS_MAX_RETRIES`: Maximum number of connection retry attempts (default: 5)
- `REDIS_BACKOFF_BASE`: Base delay for exponential backoff in seconds (default: 1.0)
- `REDIS_BACKOFF_MAX`: Maximum backoff delay in seconds (default: 30.0)

### Redis Connection Management
- **Health Checks**: Performed every 60 seconds by default
- **Connection Pool**: Validated for minimum size of 1
- **Retry Strategy**: Exponential backoff with configurable parameters
- **Error Recovery**: Automatic reconnection on unhealthy connections
- **Thread Safety**: Asyncio locks for thread-safe connection management

### Redis Stream Configuration
- `REDIS_STREAMS`: Comma-separated list of streams to process
- `STREAM_CONSUMER_GROUP`: Consumer group name (default: redis-pg-sync)
- `STREAM_CONSUMER_NAME`: Consumer name (default: redis-pg-sync-1)
- `STREAM_BATCH_SIZE`: Number of messages to process per batch (default: 100)
- `STREAM_POLL_INTERVAL`: Polling interval in seconds (default: 0.1)
- `STREAM_AUTO_DISCOVERY`: Enable automatic stream discovery (default: true)
- `STREAM_DISCOVERY_PATTERNS`: Comma-separated list of stream patterns (default: *)
- `STREAM_AUTO_CREATE_CONSUMER_GROUPS`: Auto-create consumer groups (default: true)
- `STREAM_CONFLICT_OVERWRITE`: Controls how stream message conflicts are handled (default: false)
  - When `false`: Skip inserting duplicate messages (ON CONFLICT DO NOTHING)
  - When `true`: Overwrite existing records with new data (ON CONFLICT DO UPDATE)

### Rate Limiting and Backoff
- `REDIS_RATE_LIMIT`: Maximum Redis operations per second (default: 100)
- `REDIS_BACKOFF_BASE`: Base delay for exponential backoff in seconds (default: 1.0)
- `REDIS_BACKOFF_MAX`: Maximum backoff delay in seconds (default: 30.0)
- `REDIS_MAX_RETRIES`: Maximum number of retry attempts (default: 5)

### Key-Value Configuration
- `KEY_VALUE_AUTO_DISCOVERY`: Enable automatic key discovery (default: true)
- `KEY_VALUE_PATTERNS`: Comma-separated list of key patterns (default: *)

### PostgreSQL Configuration
- `PG_HOST`: PostgreSQL server host (default: localhost)
- `PG_PORT`: PostgreSQL server port (default: 5432)
- `PG_USER`: PostgreSQL user (default: postgres)
- `PG_PASSWORD`: PostgreSQL password (default: postgres)
- `PG_DATABASE`: PostgreSQL database name (default: redis_sync)
- `PG_SCHEMA`: PostgreSQL schema name (default: public)
- `PG_POOL_SIZE`: PostgreSQL connection pool size (default: 10)
- `PG_MAX_QUERIES`: Maximum queries per connection (default: 50000)
- `PG_QUERY_TIMEOUT`: Query timeout in seconds (default: 30.0)
- `PG_MAX_RETRIES`: Maximum number of connection retry attempts (default: 5)
- `PG_BACKOFF_BASE`: Base delay for exponential backoff in seconds (default: 1.0)
- `PG_BACKOFF_MAX`: Maximum backoff delay in seconds (default: 30.0)

### PostgreSQL Permissions

The database user specified in your configuration needs specific permissions to create schemas, tables, and indexes. If the user lacks these permissions, the service will attempt to continue with limited functionality.

For detailed information about required permissions, how to check them, and how to grant them, please refer to the [PostgreSQL Permissions Guide](POSTGRES_PERMISSIONS.md).

Common scenarios:
- **Administrator access**: If your database user has administrator privileges, no additional setup is needed.
- **Limited permissions**: The service will try to use existing tables if it cannot create them.
- **Permission errors**: Check the logs for specific permission errors and refer to the permissions guide.

#### Quick Permission Setup

```sql
-- Run as superuser
GRANT CONNECT ON DATABASE your_database TO your_user;
CREATE SCHEMA IF NOT EXISTS your_schema;
GRANT USAGE, CREATE ON SCHEMA your_schema TO your_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA your_schema TO your_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA your_schema TO your_user;
```

### Table Configuration
- `STREAM_TABLE_NAME`: Name of the stream table (default: redis_streams)
- `KEY_VALUE_TABLE_NAME`: Name of the key-value table (default: redis_key_values)

### Synchronization Configuration
- `SYNC_INTERVAL`: Synchronization interval in seconds (default: 1.0)

### Logging Configuration
- `LOG_LEVEL`: Logging level (default: INFO)
- `LOG_FORMAT`: Log message format (default: %(asctime)s - %(name)s - %(levelname)s - %(message)s)

## Metrics

The service collects and reports the following metrics:
- Messages processed
- Key-values processed
- Redis operations
- PostgreSQL operations
- Streams discovered
- Errors encountered
- Retry attempts
- Last sync time

## Error Handling

The service implements robust error handling:
- Exponential backoff for retries
- Configurable retry limits
- Comprehensive error logging
- Graceful degradation
- Automatic recovery

## Rate Limiting

The service uses a token bucket algorithm for rate limiting:
- Configurable rate limits
- Thread-safe implementation
- Efficient token management
- Automatic rate adjustment

## Installation

1. Clone the repository
2. Install dependencies (primarily for the management script):
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables in `redis_pg_sync/env.conf`
4. Use the management script to build and start the services (see Usage section).

## Development

### Running Tests
```bash
python -m pytest tests/
```

### Code Style
The project follows PEP 8 guidelines. Use the following tools:
```bash
# Format code
black .

# Check code style
flake8

# Type checking
mypy .
```

## License

This project is licensed under the Myant License and protected by CopyRight. 

## Overview

The Redis-PostgreSQL Synchronization Tool provides:

1. **Key-Value Synchronization**: Copies Redis key-value pairs to PostgreSQL
2. **Stream Processing**: Consumes Redis streams and stores entries in PostgreSQL
3. **Auto-Discovery**: Automatically detects new Redis streams matching configurable patterns

## Prerequisites

- Docker and Docker Compose installed
- Access to a remote PostgreSQL server
- PostgreSQL connection credentials
- Python 3.7+
- (First time only) Install colorama for colored output:
  ```bash
  pip install colorama
  ```

## Configuration

Configuration is managed through environment variables in the `env.conf` file. You can configure the setup in two ways:

1. Using environment variables (automatically applied to env.conf)
2. Directly editing the env.conf file

### Redis Settings
```
REDIS_HOST=redis-cache
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_TIME_TO_LIVE=3600
REDIS_DB=0
REDIS_MAXMEMORY=1gb
REDIS_MAXMEMORY_POLICY=volatile-ttl
REDIS_POOL_SIZE=10
```

### PostgreSQL Settings
```
POSTGRES_HOST=remote-postgres-server
POSTGRES_PORT=5432
POSTGRES_DB=edn
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_SCHEMA=public
```

### Sync Parameters
```
SYNC_INTERVAL=5  # Redis to PostgreSQL sync interval in seconds
```

### Stream Settings
```
REDIS_STREAMS_ENABLED=true
REDIS_STREAMS=biosignal-stream
STREAM_CONSUMER_GROUP=sync-group
STREAM_CONSUMER_NAME=sync-consumer
STREAM_BATCH_SIZE=100
STREAM_POLL_INTERVAL=1
MONITOR_INTERVAL=5
```

### Stream Auto-Discovery Configuration
```
STREAM_AUTO_DISCOVERY=true
STREAM_DISCOVERY_PATTERNS=*-stream,device:*:stream,biosignal-*
STREAM_DISCOVERY_INTERVAL=10
STREAM_AUTO_CREATE_CONSUMER_GROUPS=true
REDIS_DB_SEARCH_RANGE=0-15
```

## Usage

Use the Python management script located in the `redis_pg_sync/scripts` directory to manage the Docker services.

### Start the Service
```bash
cd redis_pg_sync/scripts

# Start all services (sync app, local redis, local postgres)
# This builds the Docker images if necessary.
python manage_redis_with_sync_to_postgres.py start

# Start only the sync service (connect to external Redis/Postgres)
# Assumes external Redis/Postgres are configured in env.conf
python manage_redis_with_sync_to_postgres.py start --services redis-postgres-sync-service

# Start sync service and local Redis only
python manage_redis_with_sync_to_postgres.py start --services redis-postgres-sync-service redis-cache

# Start sync service and local Postgres only (if defined in docker-compose)
# python manage_redis_with_sync_to_postgres.py start --services redis-postgres-sync-service postgres
```
This script handles building the Docker images (using files in `redis_pg_sync/docker`) and starting the containers as defined in `redis_pg_sync/docker/docker-compose.yml`. By default, it starts the sync service (`redis-postgres-sync-service`) and potentially local `redis-cache` and `postgres` containers if they are defined and not excluded via the `--services` flag.

### Stop the Service
```bash
cd redis_pg_sync/scripts
python manage_redis_with_sync_to_postgres.py stop
```
Stops all related containers started by the script. It will prompt whether to remove associated Docker volumes.

### View Logs
```bash
# View logs for all services managed by docker-compose
cd redis_pg_sync/docker
docker-compose logs -f

# View logs for a specific service (e.g., the sync service)
docker-compose logs -f redis-postgres-sync-service

# Or view logs for the stream processor
docker-compose logs -f redis-stream-processor-service

# Alternatively, check the management script's log file
less redis_pg_sync/scripts/manage_sync.log
```

### Environment Configuration
- The management script automatically loads environment variables from `redis_pg_sync/env.conf` to configure the Docker containers.

## Notes
- The old `start.sh` and `stop.sh` scripts are deprecated and can be removed.
- For troubleshooting, check the log file or use Docker Compose logs as needed.

## Stream Auto-Discovery Feature

### Overview

The auto-discovery feature allows the service to dynamically detect and process Redis streams that match configurable patterns, even if they 
don't exist when the service starts. This is particularly useful when:

- Stream names are generated dynamically (e.g., with device IDs)
- Streams are created by external applications at runtime
- You want to avoid manually configuring every stream

By default, it searches across **all** Redis databases (0-15), but this can be configured.

### Configuration Parameters

| Parameter | Description | Example | Default |
|-----------|-------------|---------|---------|
| `STREAM_AUTO_DISCOVERY` | Enable/disable auto-discovery | `true` | `false` |
| `STREAM_DISCOVERY_PATTERNS` | Comma-separated glob patterns to match stream names | `*-stream,device:*:stream` | `*-stream` |
| `STREAM_DISCOVERY_INTERVAL` | How often to check for new streams (seconds) | `10` | `10` |
| `STREAM_AUTO_CREATE_CONSUMER_GROUPS` | Whether to automatically create consumer groups for discovered streams | `true` | `true` |
| `REDIS_DB_SEARCH_RANGE` | Which Redis databases to scan for streams (comma-separated list or range) | `0,1,3-5` | `0-15` |

### Pattern Matching

The service uses `fnmatch` for pattern matching, which supports glob-style patterns:

| Pattern | Description | Example Matches |
|---------|-------------|----------------|
| `*` | Matches any sequence of characters | `device-stream`, `any-stream` |
| `?` | Matches any single character | `log-?-stream` matches `log-1-stream`, `log-A-stream` |
| `[seq]` | Matches any character in seq | `device-[AB]-stream` matches `device-A-stream`, `device-B-stream` |
| `[!seq]` | Matches any character not in seq | `device-[!AB]-stream` matches `device-C-stream` but not `device-A-stream` |

#### Pattern Examples:

1. `*-stream` - Matches any key ending with "-stream" (e.g., `device-123-stream`, `sensor-stream`)
2. `device:*:data` - Matches keys starting with "device:" and ending with ":data" (e.g., `device:123:data`)
3. `sensor-*-readings` - Matches keys starting with "sensor-" and ending with "-readings" (e.g., `sensor-temperature-readings`)
4. `biosignal-*` - Matches any key starting with "biosignal-" (e.g., `biosignal-ecg`, `biosignal-stream-123`)

### How Auto-Discovery Works

1. Every `STREAM_DISCOVERY_INTERVAL` seconds, the service iterates through the Redis databases defined in `REDIS_DB_SEARCH_RANGE` (default: 0-15).
2. For each database, it scans Redis for keys.
3. For each key, it checks if:
   - The key is a Redis stream (type = "stream")
   - The key name matches any of the configured patterns
4. If a match is found and the stream isn't already being processed:
   - The stream is added to the processing list (prefixed with `db{number}:` if not in the default DB).
   - A consumer group is created in the correct database if `STREAM_AUTO_CREATE_CONSUMER_GROUPS` is enabled.
   - The service begins consuming and syncing data from the stream.

### Best Practices

1. **Database Range**: Configure `REDIS_DB_SEARCH_RANGE` to only include databases you expect to contain streams to improve performance.

2. **Pattern Selection**:
   - Make patterns specific enough to only match streams you want to process
   - Avoid overly generic patterns like `*` that would match all keys
   - Consider using a consistent naming convention for streams (e.g., `app-name:entity-type:id:stream`)

3. **Discovery Interval**:
   - Set an appropriate discovery interval based on your needs:
     - Lower values (1-5 seconds): Faster discovery, higher Redis load
     - Higher values (30+ seconds): Lower Redis load, delayed discovery
   - Default: 10 seconds is a balanced choice for most applications

4. **Consumer Groups**:
   - Enable `STREAM_AUTO_CREATE_CONSUMER_GROUPS` to ensure discovered streams are processed
   - The service creates consumer groups with the name defined in `STREAM_CONSUMER_GROUP`
   - All discovered streams use the same consumer group name

### Performance Considerations

Auto-discovery performs a `KEYS *` operation in each configured database, followed by a `TYPE` check for each key, which can impact Redis performance.

1. **On large Redis instances**:
   - Configure `REDIS_DB_SEARCH_RANGE` to limit the search scope.
   - Increase the discovery interval.
   - Use more specific patterns to reduce checks.

2. **Memory usage**:
   - If many streams are discovered, memory usage will increase
   - Monitor the service's memory consumption

### Updating DB Search Range Without Restarting

You can update the `REDIS_DB_SEARCH_RANGE` for a running `redis-postgres-sync-service` container without restarting it:

**Method 1: Update env.conf and Signal Process**
```bash
# 1. Edit postgres_sync/env.conf to set the new range
# Example: only scan DB 0 and 2
# Find the REDIS_DB_SEARCH_RANGE line and change it to:
# REDIS_DB_SEARCH_RANGE=0,2

# 2. Copy the updated file into the container
docker cp postgres_sync/env.conf redis-postgres-sync-service:/app/env.conf

# 3. Signal the process to reload (optional, may not reload this specific config)
# Docker typically runs the main process as PID 1
docker exec redis-postgres-sync-service kill -HUP 1
```
*Note: Reloading might not pick up the change immediately, depending on how the service is coded. The next discovery cycle should use the updated value from the reloaded env.* 

**Method 2: Modify Environment Variable Directly (Recommended)**
```bash
# Update the variable and send SIGHUP to PID 1 (the typical main process in a container)
docker exec redis-postgres-sync-service /bin/sh -c \
  "export REDIS_DB_SEARCH_RANGE='0,2' && kill -HUP 1"
```
*This directly changes the environment for the process and is more likely to be picked up if the code re-reads the env var periodically. If the variable is only read at startup, a restart might still be necessary.* 

Check the container logs after applying the change to see if the search range updates:
```bash
docker logs redis-postgres-sync-service | grep "Searching Redis databases"
```

## Stream Monitoring

The setup includes a dedicated service for monitoring Redis streams:

1. Access stream monitoring logs:
   ```bash
   cd postgres_sync/docker
   docker-compose logs -f redis-stream-processor-service
   ```

2. The monitor provides:
   - Health status for each configured stream
   - Stream statistics (length, first/last entry IDs)
   - Consumer group information
   - Pending entries
   - Warning notifications for potential issues

### Monitoring Dashboard

You can monitor the services using Docker Compose logs:

```bash
cd postgres_sync/docker
docker-compose logs -f
```

## Database Schema

The service creates the following tables and indexes in PostgreSQL:

1.  **`redis_streams`** (Configurable via `STREAM_TABLE_NAME`)
    -   Stores stream entries
    -   Columns: `id`, `stream_key`, `message_id`, `data`, `timestamp`
    -   Unique Constraint: `(stream_key, message_id)`
    -   Indexes: `(stream_key, timestamp DESC)`, `(message_id)`
2.  **`redis_key_values`** (Configurable via `KEY_VALUE_TABLE_NAME`)
    -   Stores key-value pairs
    -   Columns: `id`, `redis_key`, `redis_value`, `redis_db`, `timestamp`
    -   Unique Constraint: `redis_key`
    -   Indexes: `(redis_key)`, `(redis_db)`

## Directory Structure

```
redis_pg_sync/
├── docker/                     # Docker configuration files
│   ├── docker-compose.yml      # Docker Compose configuration
│   ├── Dockerfile.sync         # Dockerfile for sync service
│   └── Dockerfile.monitor      # Dockerfile for stream monitor
├── env.conf                    # Configuration file
├── scripts/                    # Helper scripts
│   ├── manage_redis_with_sync_to_postgres.py  # Start/stop services
│   └── manage_sync.log         # Log file for management script
├── src/                        # Source code
│   ├── __init__.py             # Package initialization
│   ├── main.py                 # Application entry point with CLI support
│   ├── config/                 # Configuration management
│   │   ├── __init__.py
│   │   └── settings.py         # Environment variable configuration
│   ├── core/                   # Core functionality
│   │   ├── __init__.py
│   │   ├── exceptions.py       # Custom exception hierarchy
│   │   └── orchestrator.py     # Main synchronization orchestrator
│   ├── services/               # Service interfaces and implementations
│   │   ├── __init__.py
│   │   ├── interfaces.py       # Abstract base classes for services
│   │   ├── postgres_service.py # PostgreSQL operations
│   │   └── redis_service.py    # Redis operations
│   └── utils/                  # Utility modules
│       ├── __init__.py
│       ├── metrics.py          # Metrics collection
│       └── rate_limiter.py     # Token bucket rate limiting
```

## Logs

Logs are stored in the `logs` directory. Each time you start the services, a new log file is created with a timestamp.

## Troubleshooting

### Testing PostgreSQL Connection

You can check the connection to your PostgreSQL server from the sync service:

```bash
docker exec -it $(docker ps -q -f name=redis-postgres-sync-service) sh
```