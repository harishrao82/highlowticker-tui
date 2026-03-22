import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import AsyncIterator


class _MockProvider:
    async def connect(self) -> None: pass
    async def stream(self) -> AsyncIterator[dict]:
        return
        yield  # make it an async generator
    async def disconnect(self) -> None: pass
    def get_metadata(self) -> dict:
        return {"name": "mock", "refresh_rate": 1.0, "is_realtime": False}


def test_highlowttui_accepts_provider_and_banner():
    """HighLowTUI must accept equity_provider + license_banner without error."""
    from app import HighLowTUI
    provider = _MockProvider()
    app = HighLowTUI(equity_provider=provider, license_banner="Test banner")
    assert app._provider is provider
    assert app._license_banner == "Test banner"


def test_highlowtui_default_banner_is_empty():
    from app import HighLowTUI
    provider = _MockProvider()
    app = HighLowTUI(equity_provider=provider)
    assert app._license_banner == ""
