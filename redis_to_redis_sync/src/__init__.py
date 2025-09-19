#!/usr/bin/env python3
"""
Redis to Redis Sync
------------------
A package for synchronizing Redis streams from a source to a target Redis instance.
"""

from config import *
from error_handling import *
from orchestrator import RedisToRedisOrchestrator
from source_redis_manager import SourceRedisManager
from target_redis_manager import TargetRedisManager

__all__ = [
    "config",
    "error_handling",
    "orchestrator",
    "source_redis_manager",
    "target_redis_manager",
    "sync_redis_with_redis",
]

__version__ = "0.1.0"
