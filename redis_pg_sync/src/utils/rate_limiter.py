#!/usr/bin/env python3
"""
Rate Limiter Module
-----------------
Rate limiter for Redis operations using token bucket algorithm.
"""

import asyncio
import logging
import time

logger = logging.getLogger("redis-pg-sync")


class RateLimiter:
    """
    Rate limiter for Redis operations using token bucket algorithm.

    This class implements a token bucket algorithm to limit the rate of
    Redis operations. It maintains a pool of tokens that are consumed
    for each operation and replenished at a fixed rate.

    Attributes:
        rate_limit: Maximum number of operations per second
        tokens: Current number of available tokens
        last_update: Timestamp of the last token update
        _lock: Asyncio lock for thread-safe operation
    """

    def __init__(self, rate_limit: int):
        """
        Initialize the rate limiter.

        Args:
            rate_limit: Maximum number of operations per second
        """
        if rate_limit < 1:
            logger.warning("Rate limit must be at least 1. Using default of 1.")
            rate_limit = 1

        self.rate_limit = rate_limit
        self.tokens: float = float(rate_limit)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """
        Acquire a token for operation.

        This method blocks until a token is available, implementing
        the token bucket algorithm. It updates the token count and
        handles waiting when tokens are exhausted.
        """
        async with self._lock:
            now = time.monotonic()
            time_passed = now - self.last_update
            self.tokens = min(
                self.rate_limit, self.tokens + time_passed * self.rate_limit
            )
            self.last_update = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate_limit
                logger.debug(f"Rate limit reached. Waiting {wait_time:.2f} seconds.")
                await asyncio.sleep(wait_time)
                self.tokens = 1

            self.tokens -= 1

    async def set_rate_limit(self, rate_limit: int):
        """
        Update the rate limit.

        Args:
            rate_limit: New maximum number of operations per second
        """
        if rate_limit < 1:
            logger.warning("Rate limit must be at least 1. Using default of 1.")
            rate_limit = 1

        async with self._lock:
            self.rate_limit = rate_limit
            # Adjust tokens proportionally to the new rate limit
            self.tokens = min(self.tokens, float(rate_limit))
