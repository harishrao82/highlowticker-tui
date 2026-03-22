import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio
import time
import pytest
from unittest.mock import patch, MagicMock
from providers.coinbase_provider import CoinbaseProvider
from providers.base import DataProvider


def test_coinbase_satisfies_protocol():
    provider = CoinbaseProvider("key_name", "-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----", ["BTC-USD"])
    assert isinstance(provider, DataProvider)


def test_metadata():
    provider = CoinbaseProvider("key", "pem", ["BTC-USD"])
    meta = provider.get_metadata()
    assert meta["name"] == "Coinbase"
    assert meta["is_realtime"] is True


def test_handle_ticker_baseline_no_update():
    provider = CoinbaseProvider("key", "pem", ["BTC-USD"])
    result = provider._handle_ticker({"product_id": "BTC-USD", "price": "50000"})
    assert result is None  # first tick — no update


def test_handle_ticker_new_high_emits_update():
    provider = CoinbaseProvider("key", "pem", ["BTC-USD"])
    provider._handle_ticker({"product_id": "BTC-USD", "price": "50000"})
    result = provider._handle_ticker({"product_id": "BTC-USD", "price": "51000"})
    assert result is not None
    assert result["type"] == "HIGHLOW_UPDATE"
    assert "BTC-USD" in result["data"]["newHighs"]


def test_handle_ticker_new_low_emits_update():
    provider = CoinbaseProvider("key", "pem", ["BTC-USD"])
    provider._handle_ticker({"product_id": "BTC-USD", "price": "50000"})
    result = provider._handle_ticker({"product_id": "BTC-USD", "price": "49000"})
    assert result is not None
    assert "BTC-USD" in result["data"]["newLows"]


def test_handle_ticker_no_change_no_update():
    provider = CoinbaseProvider("key", "pem", ["BTC-USD"])
    provider._handle_ticker({"product_id": "BTC-USD", "price": "50000"})
    result = provider._handle_ticker({"product_id": "BTC-USD", "price": "50000"})
    assert result is None


def test_midnight_reset():
    provider = CoinbaseProvider("key", "pem", ["BTC-USD"])
    provider._handle_ticker({"product_id": "BTC-USD", "price": "50000"})
    # Set session_start to epoch (clearly before today's midnight) to force reset
    provider._session_start = 0.0
    result = provider._handle_ticker({"product_id": "BTC-USD", "price": "51000"})
    # After reset, BTC-USD is treated as first tick -> None
    assert result is None
    assert provider._session_highs == {}


@pytest.mark.asyncio
async def test_jwt_task_cancels_cleanly():
    provider = CoinbaseProvider("key", "pem", ["BTC-USD"])
    provider._stop_event = asyncio.Event()
    provider._stop_event.set()  # pre-signal stop

    provider._ws = MagicMock()
    provider._jwt_task = asyncio.create_task(provider._jwt_refresh_loop())
    await asyncio.sleep(0)
    provider._jwt_task.cancel()
    try:
        await provider._jwt_task
    except asyncio.CancelledError:
        pass
    # No exception = clean cancellation
