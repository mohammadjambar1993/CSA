# Redis-to-Redis Synchronization Service

This service monitors Redis streams on a **source** Redis instance and replicates new messages to a **target** Redis instance, ensuring data consistency between the two.

## Features

- **Stream Replication**: Consumes messages from source streams using consumer groups and writes them to corresponding target streams.
- **Multi-DB Support**: Can discover and process streams across multiple databases on the source Redis instance.
- **Automatic Stream Discovery**: Scans source Redis databases for streams matching configurable patterns.
- **Robust Connection Handling**: Implements connection health checks, automatic recovery, and exponential backoff for both source and target Redis connections.
- **Startup Behavior**:
  - **Configuration Validation**: Checks critical configuration parameters (pool sizes, retries) at startup.
  - **Service Connectivity Checks**: Verifies reachability of source and target Redis instances before starting the main loop.
  - **Command-line Arguments**: Supports overriding configurations like log level (requires Docker command modification).
- **Dockerized**: Runs as a Docker container managed via `docker-compose`.

## Architecture

The service employs a modular architecture:

1.  **`SourceRedisManager`**: Handles connections to the source Redis instance(s). Manages stream discovery, consumer group creation, message reading, and acknowledgments. Includes health checks and retry logic.
2.  **`TargetRedisManager`**: Handles connections to the target Redis instance. Manages writing messages to target streams. Includes health checks and retry logic.
3.  **`RedisToRedisOrchestrator`**: Coordinates the overall synchronization process. Manages the lifecycle of discovery and stream processing tasks. Utilizes the source and target managers for Redis operations. Handles the service lifecycle (startup, shutdown).
4.  **`sync_redis_to_redis.py`**: The main entry point. Handles argument parsing, configuration validation, startup checks, logging setup, and initiates the orchestrator.

## Configuration (`env.conf`)

Configure the service by creating and editing the `redis_to_redis_sync/env.conf` file:

```
# === Source Redis Configuration ===
SOURCE_REDIS_HOST=source-redis
SOURCE_REDIS_PORT=6379
SOURCE_REDIS_PASSWORD=
SOURCE_REDIS_DB=0 # Default DB if not specified in stream name or search range

# Source Connection Management
SOURCE_REDIS_MAX_RETRIES=5
SOURCE_REDIS_BACKOFF_BASE=1.0
SOURCE_REDIS_BACKOFF_MAX=30.0
SOURCE_REDIS_HEALTH_CHECK_INTERVAL=60
SOURCE_REDIS_POOL_SIZE=10

# === Target Redis Configuration ===
TARGET_REDIS_HOST=target-redis
TARGET_REDIS_PORT=6379
TARGET_REDIS_PASSWORD=
TARGET_REDIS_DB=0 # Target DB for writing
TARGET_STREAM_PREFIX= # Optional: Prefix added to stream names on target (e.g., "synced:")

# Target Connection Management
TARGET_REDIS_MAX_RETRIES=5
TARGET_REDIS_BACKOFF_BASE=1.0
TARGET_REDIS_BACKOFF_MAX=30.0
TARGET_REDIS_HEALTH_CHECK_INTERVAL=60
TARGET_REDIS_POOL_SIZE=10

# === Stream Processing Configuration (Source Redis) ===
# Manually specify streams (optional, overrides discovery for these)
# Format: stream_key or dbX:stream_key (e.g., "stream1,db1:stream2")
REDIS_STREAMS=

REDIS_STREAMS_ENABLED=true # Enable/disable stream processing
STREAM_CONSUMER_GROUP=redis-sync-group
STREAM_CONSUMER_NAME=redis-sync-consumer # Base name, PID appended
STREAM_BATCH_SIZE=100 # Messages per read attempt
STREAM_POLL_INTERVAL=1 # Seconds block time for XREADGROUP

# === Stream Auto-Discovery (Source Redis) ===
STREAM_AUTO_DISCOVERY=true
STREAM_DISCOVERY_PATTERNS=*-stream,device:*:stream,biosignal-*
STREAM_DISCOVERY_INTERVAL=10 # Seconds between discovery scans
STREAM_AUTO_CREATE_CONSUMER_GROUPS=true # Create groups for discovered streams
# Range of DBs to scan on source Redis (e.g., "0-15", "0,1,5")
REDIS_DB_SEARCH_RANGE=0-15 

# === Logging ===
LOG_LEVEL=INFO # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s

# === Docker Configuration ===
REGISTRY=localhost:5000
TAG=latest
```

## Installation

1.  Clone the repository.
2.  Install dependencies (primarily for the management script):
    ```bash
    pip install -r requirements.txt
    ```
3.  Create and configure `redis_to_redis_sync/env.conf` based on your environment.
4.  Use the management script to build and start the services (see Usage section).

## Directory Structure

```
redis_to_redis_sync/
├── docker/
│   ├── docker-compose.yml      # Docker Compose configuration
│   └── Dockerfile              # Dockerfile for the sync service
├── env.conf                    # Configuration file (YOU create this)
├── scripts/
│   └── manage_redis_to_redis_sync.py  # Start/stop script
├── src/
│   ├── __init__.py
│   ├── sync_redis_to_redis.py     # Main service entry point
│   ├── source_redis_manager.py  # Source Redis interaction
│   ├── target_redis_manager.py  # Target Redis interaction
│   ├── orchestrator.py          # Main sync logic orchestration
│   ├── config.py                # Configuration loading
│   └── error_handling.py        # Error handling utilities
├── requirements.txt            # Python dependencies
├── README.md                   # This file
└── logs/                       # Log files (created at runtime)
```

## Usage

Use the Python management script located in the `redis_to_redis_sync/scripts` directory to manage the Docker services.

### Start the Service
```bash
cd redis_to_redis_sync/scripts

# Start all services (sync app, source-redis, target-redis defined in docker-compose)
# This builds the Docker image if necessary.
python manage_redis_to_redis_sync.py start

# Start only the sync service (connect to external Redis instances)
# Assumes external Redis instances are configured in env.conf
python manage_redis_to_redis_sync.py start --services redis-to-redis-sync

# Start sync service and source Redis only
python manage_redis_to_redis_sync.py start --services redis-to-redis-sync source-redis

# Start sync service and target Redis only
python manage_redis_to_redis_sync.py start --services redis-to-redis-sync target-redis
```
This script handles building the Docker image and starting the containers defined in `redis_to_redis_sync/docker/docker-compose.yml`, configured via `redis_to_redis_sync/env.conf`.

### Stop the Service
```bash
cd redis_to_redis_sync/scripts
python manage_redis_to_redis_sync.py stop
```
Stops all related containers started by the script and prompts about removing volumes.

### View Logs
```bash
# View logs for all services managed by docker-compose
cd redis_to_redis_sync/docker
docker-compose logs -f

# View logs for the sync service specifically
docker-compose logs -f redis-to-redis-sync

# Alternatively, check the management script's log file
less redis_to_redis_sync/scripts/manage_sync.log
```

### Command-line Options (for the sync service container)
While primarily managed via Docker and `env.conf`, the underlying Python script (`sync_redis_to_redis.py`) supports arguments like:
- `--log-level`: Overrides `LOG_LEVEL` from `env.conf` (e.g., `DEBUG`, `INFO`). Using this requires modifying the `command` section for the `redis-to-redis-sync` service in `docker-compose.yml`.

## How it Works

1.  **Startup**: The main script (`sync_redis_to_redis.py`) parses arguments, sets up logging, validates configuration, and creates the `RedisToRedisOrchestrator`.
2.  **Connectivity Checks**: Verifies connectivity to target Redis and at least one source Redis DB.
3.  **Initialization**: The `Orchestrator` initializes `SourceRedisManager` and `TargetRedisManager`.
4.  **Initial Consumer Groups**: `SourceRedisManager` sets up consumer groups for manually configured streams (`REDIS_STREAMS`).
5.  **Discovery Task**: If `STREAM_AUTO_DISCOVERY` is enabled, the `Orchestrator` starts a background task (`_discover_streams_periodically`) that periodically scans source Redis DBs, adds new unique streams to its internal processing list (`_current_streams`), and ensures their consumer groups are created via `SourceRedisManager`.
6.  **Processing Task Management**: The `Orchestrator`'s main `run` loop monitors the internal stream list (`_current_streams`). For any stream added (either initially or by discovery) that doesn't have an active processing task, it starts a dedicated `_process_single_stream` task.
7.  **Stream Processing Task (`_process_single_stream`)**: Each task runs a continuous loop for its assigned stream:
    a.  **Read (Source)**: Calls `SourceRedisManager.read_stream_messages`, which returns the message batch along with DB number and original key.
    b.  **Write (Target)**: Determines the target stream key (applying `TARGET_STREAM_PREFIX`) and calls `TargetRedisManager.write_messages`.
    c.  **Acknowledge (Source)**: If writing succeeded (returned IDs), calls `SourceRedisManager.ack_messages` using the correct stream name and IDs.
    d.  Sleeps briefly if no messages are read or if writes fail, before looping again.
8.  **Shutdown**: On `SIGINT`/`SIGTERM`, the `Orchestrator` cancels the discovery task and all active processing tasks, then calls the managers' `close` methods.

## Notes

-   Preserves message IDs during replication.
-   Network connectivity between the sync container and both Redis instances is crucial.

## Troubleshooting

-   **Connection Errors**: Double-check hostnames, ports, passwords, and network reachability for both Redis instances in `env.conf` and your Docker setup.
-   **Streams Not Discovered**: Verify `STREAM_AUTO_DISCOVERY=true`, check `STREAM_DISCOVERY_PATTERNS` and `REDIS_DB_SEARCH_RANGE` against actual stream names and locations on the source Redis.
-   **Messages Not Replicating**: Examine the `redis-to-redis-sync` container logs (`docker-compose logs -f redis-to-redis-sync`) for specific errors during reading, writing, or acknowledgment phases. 