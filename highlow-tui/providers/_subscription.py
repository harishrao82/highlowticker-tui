"""Shared subscription utilities for all WebSocket providers."""
import asyncio
import time
from typing import Callable, Awaitable, List, Dict


def wall_clock_counts(timestamps: List[float]) -> Dict[str, int]:
    """Count timestamps in rolling windows ending at now."""
    now = time.time()
    return {
        "30s": sum(1 for t in timestamps if t >= now - 30),
        "1m":  sum(1 for t in timestamps if t >= now - 60),
        "5m":  sum(1 for t in timestamps if t >= now - 300),
        "20m": sum(1 for t in timestamps if t >= now - 1200),
    }


class SubscriptionManager:
    """Rate-limited, chunked symbol subscription."""

    def __init__(
        self,
        subscribe_fn: Callable[[List[str]], Awaitable[None]],
        batch_size: int,
        rate_limit_per_sec: float,
    ) -> None:
        self._subscribe_fn = subscribe_fn
        self._batch_size = batch_size
        self._min_interval = 1.0 / rate_limit_per_sec

    async def subscribe(self, symbols: List[str]) -> None:
        """Submit all symbols in rate-limited batches."""
        for i in range(0, len(symbols), self._batch_size):
            batch = symbols[i : i + self._batch_size]
            await self._subscribe_fn(batch)
            if i + self._batch_size < len(symbols):
                await asyncio.sleep(self._min_interval)
