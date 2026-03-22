import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import patch
from core.app_config import get_equity_broker, get_crypto_broker, ConfigError


def test_get_equity_broker_returns_name():
    assert get_equity_broker({"equity": {"broker": "alpaca"}}) == "alpaca"


def test_get_crypto_broker_returns_name():
    assert get_crypto_broker({"crypto": {"broker": "coinbase"}}) == "coinbase"


def test_empty_config_returns_none():
    assert get_equity_broker({}) is None
    assert get_crypto_broker({}) is None


def test_missing_section_returns_none():
    assert get_equity_broker({"crypto": {"broker": "coinbase"}}) is None
    assert get_crypto_broker({"equity": {"broker": "alpaca"}}) is None


def test_invalid_equity_broker_raises():
    with pytest.raises(ConfigError, match="robinhood"):
        get_equity_broker({"equity": {"broker": "robinhood"}})


def test_invalid_crypto_broker_raises():
    with pytest.raises(ConfigError, match="binance"):
        get_crypto_broker({"crypto": {"broker": "binance"}})


def test_all_valid_equity_brokers():
    for broker in ("schwab", "alpaca", "webull", "tastytrade"):
        assert get_equity_broker({"equity": {"broker": broker}}) == broker
