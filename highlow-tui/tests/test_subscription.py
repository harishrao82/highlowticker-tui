import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio
import time
import pytest
from unittest.mock import patch
from providers._subscription import SubscriptionManager, wall_clock_counts


def test_wall_clock_counts_includes_current_window():
    # Freeze time at 65s (5s into a 30s window, 5s into a 1m window)
    # 30s window start: 65 - (65%30) = 65 - 5 = 60.0
    # 1m window start:  65 - (65%60) = 65 - 5 = 60.0
    frozen_now = 65.0
    t_in = 62.0   # inside both 30s and 1m windows (>= 60.0)
    t_out = 59.0  # outside both 30s and 1m windows (< 60.0)

    with patch("providers._subscription.time") as mock_time:
        mock_time.time.return_value = frozen_now
        counts = wall_clock_counts([t_in, t_out])

    assert counts["30s"] == 1   # only t_in is in the 30s window
    assert counts["1m"] == 1    # only t_in is in the 1m window


def test_wall_clock_counts_empty():
    counts = wall_clock_counts([])
    assert counts == {"30s": 0, "1m": 0, "5m": 0, "20m": 0}


@pytest.mark.asyncio
async def test_subscription_manager_batches_symbols():
    received = []

    async def subscribe_fn(symbols):
        received.append(list(symbols))

    mgr = SubscriptionManager(subscribe_fn=subscribe_fn, batch_size=2, rate_limit_per_sec=1000)
    await mgr.subscribe(["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"])
    assert len(received) == 3
    assert received[0] == ["AAPL", "GOOGL"]
    assert received[1] == ["MSFT", "AMZN"]
    assert received[2] == ["TSLA"]


@pytest.mark.asyncio
async def test_subscription_manager_single_batch():
    received = []

    async def subscribe_fn(symbols):
        received.append(list(symbols))

    mgr = SubscriptionManager(subscribe_fn=subscribe_fn, batch_size=10, rate_limit_per_sec=1000)
    await mgr.subscribe(["AAPL", "MSFT"])
    assert len(received) == 1
    assert received[0] == ["AAPL", "MSFT"]
