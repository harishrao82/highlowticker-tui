"""TradierProvider — real-time equity quotes via Tradier SSE streaming + REST seed.

On connect():
  1. Creates a streaming session (REST POST).
  2. Seeds session high/low baselines from REST quotes.
  3. Starts a background task that reads the SSE stream into a queue.

stream() drains the queue, processes trade/quote events, and yields
HIGHLOW_UPDATE dicts whenever a new session high or low is detected.

Required env var: TRADIER_ACCESS_TOKEN
Optional env var: TRADIER_ENDPOINT (default "https://api.tradier.com/v1")
"""
import asyncio
import json
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import AsyncIterator, Dict, List, Optional

_ET = ZoneInfo("America/New_York")

import httpx

from providers._subscription import wall_clock_counts
from providers._volume import VolumeTracker

REST_BASE   = "https://api.tradier.com/v1"
STREAM_BASE = "https://stream.tradier.com/v1"
PRUNE_WINDOW = 1200  # seconds (20 min)


class TradierProvider:
    """Streams real-time equity quotes from Tradier and tracks session highs/lows.

    Uses Tradier's SSE stream for real-time price updates (trades + quotes).
    REST quotes are fetched once on connect() to seed session high/low baselines.
    """

    def __init__(
        self,
        access_token: str,
        symbols: List[str],
        rest_base: str = REST_BASE,
        stream_base: str = STREAM_BASE,
    ) -> None:
        self._token = access_token
        self.symbols = list(symbols)
        self._rest_base = rest_base
        self._stream_base = stream_base
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        self._session_id: Optional[str] = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self._sse_task: Optional[asyncio.Task] = None
        self._reseed_task: Optional[asyncio.Task] = None

        # Session tracking
        self._session_highs: Dict[str, float] = {}
        self._session_lows:  Dict[str, float] = {}
        self._open_prices:   Dict[str, float] = {}
        self._current_prices: Dict[str, float] = {}
        self._high_counts: Dict[str, int] = {}
        self._low_counts:  Dict[str, int] = {}
        self._high_timestamps: List[float] = []
        self._low_timestamps:  List[float] = []
        self._vol_tracker = VolumeTracker()
        self._volume_spikes: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # DataProvider protocol
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._stop_event.clear()
        self._session_id = await self._create_stream_session()
        await self._seed_from_rest()
        self._sse_task    = asyncio.create_task(self._sse_reader())
        self._reseed_task = asyncio.create_task(self._market_open_reseed())

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
                update = self._handle_event(msg)
                if update:
                    yield update
        except Exception as e:
            print(f"[TradierProvider] stream error: {e}", file=sys.stderr)

    async def disconnect(self) -> None:
        self._stop_event.set()
        for task in (self._sse_task, self._reseed_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._sse_task = None
        self._reseed_task = None

    def get_metadata(self) -> dict:
        return {"name": "Tradier", "refresh_rate": 0.0, "is_realtime": True}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _create_stream_session(self) -> str:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{self._rest_base}/markets/events/session",
                headers=self._headers,
            )
            r.raise_for_status()
            return r.json()["stream"]["sessionid"]

    async def _seed_from_rest(self) -> None:
        """Fetch current quotes to seed session baselines before streaming starts."""
        batch_size = 200  # Tradier allows up to 200 symbols per request
        for i in range(0, len(self.symbols), batch_size):
            await self._fetch_quotes(self.symbols[i : i + batch_size])

    async def _fetch_quotes(self, symbols: List[str]) -> None:
        sym_str = ",".join(symbols)
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{self._rest_base}/markets/quotes",
                    headers=self._headers,
                    params={"symbols": sym_str, "greeks": "false"},
                )
        except Exception as e:
            print(f"[TradierProvider] REST seed error: {e}", file=sys.stderr)
            return

        if r.status_code != 200:
            print(f"[TradierProvider] REST quotes returned {r.status_code}", file=sys.stderr)
            return

        quotes = r.json().get("quotes", {}).get("quote", [])
        if isinstance(quotes, dict):
            quotes = [quotes]

        for q in quotes:
            sym = q.get("symbol", "")
            if not sym:
                continue
            last  = q.get("last") or q.get("close")
            high  = q.get("high") or last
            low   = q.get("low")  or last
            open_ = q.get("open") or last
            if last is None:
                continue
            self._open_prices[sym]    = float(open_)
            self._session_highs[sym]  = float(high)
            self._session_lows[sym]   = float(low)
            self._current_prices[sym] = float(last)
            self._high_counts.setdefault(sym, 0)
            self._low_counts.setdefault(sym, 0)

    @staticmethod
    def _next_market_open_ts() -> float:
        """Return UTC timestamp of the next 9:30 AM ET."""
        now = datetime.now(_ET)
        target = now.replace(hour=9, minute=30, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        return target.timestamp()

    async def _market_open_reseed(self) -> None:
        """Background task: at 9:30 AM ET each day, reset session state and reseed from REST.

        This ensures open prices and high/low baselines reflect the real regular-session
        open rather than pre-market data or the previous day's close.
        """
        try:
            while not self._stop_event.is_set():
                wait_secs = self._next_market_open_ts() - time.time()
                if wait_secs > 0:
                    await asyncio.sleep(wait_secs)
                if self._stop_event.is_set():
                    break

                print("[TradierProvider] 9:30 AM ET — reseeding session baselines", file=sys.stderr)
                # Reset all session state so the regular session starts clean
                self._high_counts.clear()
                self._low_counts.clear()
                self._high_timestamps.clear()
                self._low_timestamps.clear()
                self._volume_spikes.clear()
                self._vol_tracker = VolumeTracker()
                # Refetch quotes — Tradier will now have the real open price
                await self._seed_from_rest()

                # Sleep 70 s to land safely past 9:30 before looping to next day
                await asyncio.sleep(70)
        except asyncio.CancelledError:
            pass

    async def _sse_reader(self) -> None:
        """Background task: read Tradier SSE stream and push raw JSON lines to queue."""
        sym_str = ",".join(self.symbols)
        url = f"{self._stream_base}/markets/events"
        params = {
            "symbols": sym_str,
            "sessionid": self._session_id,
            "linebreak": "true",
            "filter": "trade,quote",
        }
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", url, headers=self._headers, params=params) as resp:
                    async for line in resp.aiter_lines():
                        if self._stop_event.is_set():
                            break
                        line = line.strip()
                        if line:
                            await self._queue.put(line)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if not self._stop_event.is_set():
                print(f"[TradierProvider] SSE reader error: {e}", file=sys.stderr)

    def _handle_event(self, msg: dict) -> Optional[dict]:
        event_type = msg.get("type")
        sym = msg.get("symbol", "")

        # Only process symbols we seeded (i.e., in our watchlist)
        if not sym or sym not in self._open_prices:
            return None

        ts = time.time()
        price: Optional[float] = None

        if event_type == "trade":
            raw = msg.get("price") or msg.get("last")
            if raw:
                price = float(raw)
            size = msg.get("size", 0)
            if size:
                ratio = self._vol_tracker.record(sym, float(size), ts)
                if ratio is not None:
                    if ratio > 1.0:
                        self._volume_spikes[sym] = ratio
                    else:
                        self._volume_spikes.pop(sym, None)

        elif event_type == "quote":
            bid = msg.get("bid")
            ask = msg.get("ask")
            if bid and ask:
                price = (float(bid) + float(ask)) / 2.0

        if not price:
            return None

        self._current_prices[sym] = price
        updated = False

        if price > self._session_highs.get(sym, price):
            self._session_highs[sym] = price
            self._high_counts[sym] = self._high_counts.get(sym, 0) + 1
            self._high_timestamps.append(ts)
            updated = True
        if price < self._session_lows.get(sym, price):
            self._session_lows[sym] = price
            self._low_counts[sym] = self._low_counts.get(sym, 0) + 1
            self._low_timestamps.append(ts)
            updated = True

        if not updated:
            return None

        cutoff = ts - PRUNE_WINDOW
        self._high_timestamps = [t for t in self._high_timestamps if t > cutoff]
        self._low_timestamps  = [t for t in self._low_timestamps  if t > cutoff]

        open_prices = self._open_prices
        return {
            "type": "HIGHLOW_UPDATE",
            "data": {
                "newHighs": {s: c for s, c in self._high_counts.items() if c > 0},
                "newLows":  {s: c for s, c in self._low_counts.items()  if c > 0},
                "lastHigh": dict(self._session_highs),
                "lastLow":  dict(self._session_lows),
                "week52Highs": [],
                "week52Lows":  [],
                "percentChange": {
                    s: round((self._current_prices[s] - open_prices[s]) / open_prices[s] * 100, 2)
                    for s in open_prices
                    if s in self._current_prices and open_prices[s]
                },
                "highCounts": wall_clock_counts(self._high_timestamps),
                "lowCounts":  wall_clock_counts(self._low_timestamps),
                "indexPrices": {
                    "SPY": self._current_prices.get("SPY", 0.0),
                    "DIA": self._current_prices.get("DIA", 0.0),
                    "QQQ": self._current_prices.get("QQQ", 0.0),
                },
                "volumeSpikes": dict(self._volume_spikes),
            },
        }
