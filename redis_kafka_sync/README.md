# Redis Kafka Sync Service

This service synchronizes data between Redis instances and Kafka. It supports two primary Redis roles:

- **Source Redis**: Local instance where data originates (streams, key-value changes)
- **Target Redis**: Remote instance serving as a reliable cache before sending to Kafka

The architecture ensures data is not lost if Kafka is temporarily unavailable by using the target Redis as a durable buffer.

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│             │      │             │      │             │
│ Source Redis│──────▶ Target Redis│──────▶    Kafka    │
│   (local)   │      │  (remote)   │      │             │
│             │      │             │      │             │
└─────────────┘      └─────────────┘      └─────────────┘
```

The service can operate in two primary modes:
1. **Stream Processing**: Reads from streams in source Redis, caches in target Redis, then sends to Kafka
2. **Direct Caching**: Directly accepts messages, caches them in target Redis, then sends to Kafka

## Directory Structure

The codebase follows a consistent structure:

```
redis_kafka_sync/
├── docker/                     # Docker configuration files
│   ├── docker-compose.no_swarm.yml # Docker Compose configuration (non-Swarm)
│   ├── docker-compose.swarm.yml # Docker Swarm configuration
│   └── Dockerfile              # Dockerfile for service
├── env.conf                    # Configuration file
├── scripts/                    # Helper scripts
│   ├── manage_redis_with_kafka_producer_sync.py # Start/stop/deploy services
│   └── convert_truststore.py   # Utility for SSL certificates
├── src/                        # Source code library
│   ├── reactive_producer.py    # Main orchestrator class (ReactiveKafkaProducer)
│   ├── kafka_manager.py        # Handles Kafka connection and producing
│   ├── redis_manager.py        # Handles Redis connections and operations
│   ├── key_format.py           # Utility functions for generating and parsing message keys
│   └── # (other potential helper modules)
└── tests/                      # Test files
```

## Prerequisites
- Python 3.9+ (due to asyncio usage)
- Docker and Docker Compose installed (for containerized deployment)
- Source Redis instance (local, for reading streams/data)
- Target Redis instance (remote, for caching)
- Kafka cluster (for message destination)

## Key Features

- **Dual Redis Support**: Separate configurations for source and target Redis instances
- **Stream Processing**: Reads messages from Redis streams in source Redis
- **Set-based Tracking**: Efficiently tracks unsynced keys in target Redis using Redis Sets
- **Asynchronous Operation**: Uses `asyncio`, `aioredis`, and `aiokafka` for non-blocking I/O
- **Robust Sync**: Cache-before-send pattern with explicit acknowledgments and retry mechanisms
- **Automatic Recovery**: Rebuilds unsynced key tracking on startup
- **Stream Discovery**: Automatically discovers streams in source Redis
- **Topic Mapping**: Maps streams to Kafka topics using pattern matching

## Configuration

Configuration is managed through environment variables, defined in the `env.conf` file:

### Source Redis Configuration (Local Instance)
```
# Source Redis Configuration (Local instance where data originates)
SOURCE_REDIS_HOST=localhost            # Local Redis host
SOURCE_REDIS_PORT=6379                 # Local Redis port
SOURCE_REDIS_PASSWORD=                 # Local Redis password (if any)
SOURCE_REDIS_DB_SEARCH_RANGE="0-15"    # For stream discovery
SOURCE_REDIS_ENCODING=utf-8            # Default encoding for string fields
```

### Target Redis Configuration (Remote Instance)
```
# Target Redis Configuration (Remote instance for caching before Kafka)
TARGET_REDIS_HOST=redis-cache          # Remote Redis host
TARGET_REDIS_PORT=6379                 # Remote Redis port
TARGET_REDIS_PASSWORD=                 # Remote Redis password (if any)
TARGET_REDIS_DB=0                      # Default DB for caching messages
TARGET_REDIS_TIME_TO_LIVE=3600         # TTL for cached messages
TARGET_REDIS_SYNC_TTL=604800           # TTL for sync markers (7 days)
TARGET_REDIS_POOL_SIZE=10              # Connection pool size
```

### Stream Processing Configuration
```
# Stream Processing (Reading from Source Redis)
STREAM_AUTO_DISCOVERY=false            # Set to true to enable
STREAM_DISCOVERY_PATTERNS=*-stream,device:*:stream
STREAM_DISCOVERY_INTERVAL=10           # Interval in seconds
STREAM_TARGET_TOPIC_MAP='{"device:*:stream": "device-data-topic", "*-stream": "default-stream-topic"}'
```

### Kafka Configuration
```
# Kafka Configuration (Target Destination)
KAFKA_BOOTSTRAP_SERVERS=remote-kafka:9092
KAFKA_TOPIC=default-sync-topic         # Default topic if not specified
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_MAX_REQUEST_SIZE=20971520
KAFKA_MAX_CONCURRENT_SENDS=100
```

## Usage

### Programmatic Usage (Using the Orchestrator)

```python
import asyncio
import json
from redis_kafka_sync.src.reactive_producer import ReactiveKafkaProducer

async def main():
    # Create the orchestrator
    orchestrator = ReactiveKafkaProducer()
    
    try:
        # Start (connects to source Redis, target Redis, and Kafka)
        await orchestrator.start()

        # Option 1: Process streams from source Redis
        stream_to_topic = {
            "device:123:stream": "device-data-topic",
            "*-analytics-stream": "analytics-topic"
        }
        processed = await orchestrator.process_source_streams(
            topic_mapping=stream_to_topic,
            stream_pattern="*-stream",
            source_db=0,
            batch_size=100
        )
        print(f"Processed {processed} messages from streams")
        
        # Option 2: Send a direct message (cached in target Redis)
        message_key = await orchestrator.send_message(
            topic="your-target-topic",
            message=json.dumps({"data": "example"}).encode('utf-8'),
            device_id="device123"
        )
        
        # Sync any unsynced messages in target Redis to Kafka
        synced = await orchestrator.sync_unsynced_keys(topic="default-topic")
        print(f"Synced {synced} previously unsynced messages")
        
    finally:
        # Graceful shutdown
        await orchestrator.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## How It Works

### Stream Processing Flow

1. **Discovery**: Scans source Redis for stream keys matching configured patterns
2. **Reading**: Reads messages from the streams using `XREAD`
3. **Processing**: Processes stream messages (converts to JSON format)
4. **Caching**: Stores processed messages in target Redis with TTL and marks them as unsynced
5. **Sending**: Asynchronously sends to Kafka
6. **Acknowledgment**: Upon Kafka confirmation, marks message as synced in target Redis and removes from unsynced set
7. **Cleanup**: Acknowledges processed messages in source Redis streams with `XDEL`

### Direct Message Flow

1. **Caching**: Stores messages in target Redis with TTL and marks as unsynced
2. **Sending**: Asynchronously sends to Kafka
3. **Acknowledgment**: Upon Kafka confirmation, marks message as synced in target Redis

### Unsynced Keys Recovery

1. **Tracking**: Uses Redis Set (`unsynced_keys_set`) in target Redis to efficiently track unsynced messages
2. **Recovery**: On startup, rebuilds unsynced tracking by scanning target Redis for keys without sync markers
3. **Periodic Sync**: Periodically checks for and resends unsynced messages to Kafka

## Deployment Examples

### Docker Compose (Local Development)

```bash
cd redis_kafka_sync/docker
docker-compose -f docker-compose.no_swarm.yml up -d
```

### Docker Swarm (Production)

```bash
cd redis_kafka_sync/scripts
python manage_redis_with_kafka_producer_sync.py swarm --action deploy --stack-name redis-kafka-sync-stack
```

## Notes
- **Source Redis vs Target Redis**: The source Redis is typically a local instance containing streams or key-value data to be synchronized. The target Redis is a remote cache used before data is sent to Kafka.
- **Key Tracking**: The service efficiently tracks which keys have been successfully synced to Kafka using a dedicated Redis Set in the target Redis.
- **Recovery**: If source Redis or Kafka temporarily fails, the service will retry and recover automatically.