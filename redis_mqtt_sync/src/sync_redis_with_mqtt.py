#!/usr/bin/env python3
"""
Redis MQTT Synchronization CLI Wrapper.

Simple entry point that runs the Redis to MQTT synchronization service.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Ensure module can be run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.main import main

if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
