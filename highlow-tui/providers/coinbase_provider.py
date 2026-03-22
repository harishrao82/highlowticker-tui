"""CoinbaseProvider — real-time crypto via Coinbase Advanced Trade WebSocket SDK."""
import asyncio
import json
import time
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional

from coinbase.websocket import WSClient

from providers._subscription import wall_clock_counts

CHANNEL      = "ticker_batch"
PRUNE_WINDOW = 1200   # seconds (20 min)


class CoinbaseProvider:
    """Streams real-time crypto quotes from Coinbase and tracks session highs/lows.

    Session = midnight UTC of the current calendar day.
    Auth is handled by the coinbase-advanced-py SDK (JWT signed internally).
    """

    def __init__(self, api_key_name: str, private_key_pem: str, symbols: List[str]) -> None:
        self._api_key_name    = api_key_name
        self._private_key_pem = private_key_pem
        self.symbols          = list(symbols)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._ws: Optional[WSClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._session_highs: Dict[str, float] = {}
        self._session_lows:  Dict[str, float] = {}
        self._baselines:     Dict[str, float] = {}   # first-tick price; not a high/low yet
        self._high_counts:   Dict[str, int]   = {}
        self._low_counts:    Dict[str, int]   = {}
        self._high_timestamps: List[float] = []
        self._low_timestamps:  List[float] = []
        self._current_prices:  Dict[str, float] = {}
        self._stop_event = asyncio.Event()
        self._session_start = self._midnight_utc()

    @staticmethod
    def _midnight_utc() -> float:
        now = datetime.now(timezone.utc)
        return now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    def _on_message(self, msg: str) -> None:
        """SDK callback — called from the SDK's internal thread."""
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._queue.put_nowait, msg)

    async def connect(self) -> None:
        self._stop_event.clear()
        self._loop = asyncio.get_event_loop()
        self._ws = WSClient(
            api_key=self._api_key_name,
            api_secret=self._private_key_pem,
            on_message=self._on_message,
        )
        await self._ws.open_async()
        await self._ws.ticker_batch_async(product_ids=self.symbols)

    async def stream(self) -> AsyncIterator[dict]:
        try:
            while not self._stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if msg.get("channel") != CHANNEL:
                    continue
                for event in msg.get("events", []):
                    for ticker in event.get("tickers", []):
                        update = self._handle_ticker(ticker)
                        if update:
                            yield update
        except Exception as e:
            import sys
            print(f"[CoinbaseProvider] stream error: {e}", file=sys.stderr)

    def _handle_ticker(self, ticker: dict) -> Optional[dict]:
        sym       = ticker.get("product_id", "")
        price_str = ticker.get("price", "")
        if not sym or not price_str:
            return None
        price = float(price_str)
        if not price:
            return None

        ts = time.time()

        # Reset at midnight UTC
        midnight = self._midnight_utc()
        if self._session_start < midnight:
            self._session_start = midnight
            self._session_highs.clear()
            self._session_lows.clear()
            self._baselines.clear()
            self._high_counts.clear()
            self._low_counts.clear()

        self._current_prices[sym] = price

        if sym not in self._baselines:
            # First tick for this symbol — establish baseline, no update yet
            self._baselines[sym]   = price
            self._high_counts[sym] = 0
            self._low_counts[sym]  = 0
            return None

        baseline = self._baselines[sym]
        updated = False
        ref_high = self._session_highs.get(sym, baseline)
        ref_low  = self._session_lows.get(sym, baseline)

        if price > ref_high:
            self._session_highs[sym] = price
            self._high_counts[sym]   = self._high_counts.get(sym, 0) + 1
            self._high_timestamps.append(ts)
            updated = True
        if price < ref_low:
            self._session_lows[sym] = price
            self._low_counts[sym]   = self._low_counts.get(sym, 0) + 1
            self._low_timestamps.append(ts)
            updated = True

        if not updated:
            return None

        cutoff = ts - PRUNE_WINDOW
        self._high_timestamps = [t for t in self._high_timestamps if t > cutoff]
        self._low_timestamps  = [t for t in self._low_timestamps  if t > cutoff]

        return {
            "type": "HIGHLOW_UPDATE",
            "data": {
                "newHighs": {s: c for s, c in self._high_counts.items() if c > 0},
                "newLows":  {s: c for s, c in self._low_counts.items()  if c > 0},
                "lastHigh": {s: self._session_highs.get(s, self._baselines[s]) for s in self._baselines},
                "lastLow":  {s: self._session_lows.get(s,  self._baselines[s]) for s in self._baselines},
                "week52Highs": [],
                "week52Lows":  [],
                "percentChange": {
                    s: round((self._current_prices[s] - self._baselines[s]) / self._baselines[s] * 100, 2)
                    for s in self._baselines
                    if s in self._current_prices and self._baselines[s]
                },
                "highCounts": wall_clock_counts(self._high_timestamps),
                "lowCounts":  wall_clock_counts(self._low_timestamps),
                "indexPrices": {"SPY": 0.0, "DIA": 0.0, "QQQ": 0.0},
            },
        }

    async def disconnect(self) -> None:
        self._stop_event.set()
        if self._ws:
            await self._ws.close_async()
            self._ws = None

    def get_metadata(self) -> dict:
        return {"name": "Coinbase", "refresh_rate": 0.0, "is_realtime": True}
