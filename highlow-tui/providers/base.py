"""DataProvider Protocol — every data source must satisfy this interface."""
from typing import Protocol, AsyncIterator, runtime_checkable


@runtime_checkable
class DataProvider(Protocol):
    async def connect(self) -> None:
        """Open connection / start polling. Called once before stream()."""
        ...

    async def stream(self) -> AsyncIterator[dict]:
        """Yield {"type": "HIGHLOW_UPDATE", "data": {...}} dicts indefinitely."""
        ...

    async def disconnect(self) -> None:
        """Clean up resources."""
        ...

    def get_metadata(self) -> dict:
        """Return {'name': str, 'refresh_rate': float, 'is_realtime': bool}."""
        ...
