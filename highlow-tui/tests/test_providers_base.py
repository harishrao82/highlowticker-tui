import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from providers.base import DataProvider
from typing import AsyncIterator


class _FakeProvider:
    async def connect(self) -> None: pass
    async def stream(self) -> AsyncIterator[dict]:
        yield {}
    async def disconnect(self) -> None: pass
    def get_metadata(self) -> dict:
        return {"name": "fake", "refresh_rate": 1.0, "is_realtime": False}


class _BadProvider:
    pass  # missing all required methods


def test_protocol_structural_check():
    assert isinstance(_FakeProvider(), DataProvider)


def test_protocol_rejects_incomplete():
    assert not isinstance(_BadProvider(), DataProvider)


@pytest.mark.skip(reason="SchwabProvider is private (gitignored) — cannot be tested in open-source build")
def test_schwab_provider_satisfies_protocol():
    from providers.schwab_provider import SchwabProvider
    assert isinstance(SchwabProvider(), DataProvider)
