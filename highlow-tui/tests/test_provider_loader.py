import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import patch
from core.provider_loader import _require_env, ProviderLoadError


def test_require_env_returns_values(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "mykey")
    monkeypatch.setenv("ALPACA_API_SECRET", "mysecret")
    result = _require_env("ALPACA_API_KEY", "ALPACA_API_SECRET", broker="alpaca", docs_url="https://example.com")
    assert result["ALPACA_API_KEY"] == "mykey"
    assert result["ALPACA_API_SECRET"] == "mysecret"


def test_require_env_raises_on_missing(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    with pytest.raises(ProviderLoadError, match="ALPACA_API_KEY"):
        _require_env("ALPACA_API_KEY", broker="alpaca", docs_url="https://example.com")
