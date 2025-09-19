"""
Rate limiter utility for Redis MQTT Synchronization.

This module provides a token bucket rate limiter implementation.
"""

import asyncio
import time
from typing import Optional


class RateLimiter:
    """
    Token bucket rate limiter implementation.

    This class implements a token bucket algorithm for rate limiting.
    It allows bursts of activity up to the maximum bucket size while
    enforcing a long-term rate limit.
    """

    def __init__(
        self, rate: float, time_unit: float = 1.0, max_tokens: Optional[int] = None
    ):
        """
        Initialize the rate limiter.

        Args:
            rate: Rate limit (tokens per time_unit)
            time_unit: Time unit in seconds (default: 1.0 = per second)
            max_tokens: Maximum number of tokens in the bucket (defaults to rate)
        """
        self.rate = rate
        self.time_unit = time_unit
        self.max_tokens = max_tokens if max_tokens is not None else rate
        self.tokens = self.max_tokens
        self.last_time = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens from the bucket, waiting if necessary.

        Args:
            tokens: Number of tokens to acquire (default: 1)

        Returns:
            Time spent waiting in seconds
        """
        if tokens > self.max_tokens:
            raise ValueError(
                f"Cannot acquire {tokens} tokens (exceeds bucket size {self.max_tokens})"
            )

        start_time = time.time()

        async with self.lock:
            # First, add tokens based on time elapsed since last update
            current_time = time.time()
            elapsed = current_time - self.last_time
            self.tokens = min(
                self.max_tokens, self.tokens + elapsed * (self.rate / self.time_unit)
            )
            self.last_time = current_time

            # Calculate delay if not enough tokens available
            if tokens > self.tokens:
                # Not enough tokens, calculate wait time
                deficit = tokens - self.tokens
                wait_time = deficit * self.time_unit / self.rate

                # Update state as if we waited
                self.tokens = 0  # Consumed all available
                self.last_time = current_time + wait_time  # Future time after wait

                # Actually wait for required time
                await asyncio.sleep(wait_time)
            else:
                # Enough tokens, consume them
                self.tokens -= tokens
                wait_time = 0

        return time.time() - start_time
