"""Startup backfill: fetch the last 30 minutes of 1-min candles from the
Coinbase Advanced Trade public REST API and feed synthetic ticks into the
LiveFeatureEngine so it knows the TRUE current-window open price even when
the server starts mid-window.

What we recover (high fidelity):
  • The current 15-min window's open price (and high/low/range so far)
  • The previous 15-min window's full OHLC → prior_candle features
  • completed_1min_candles for every minute closed before we connected

What we approximate (will be wrong for ~5 min until live data dominates):
  • Per-tick flow imbalance — REST candles don't carry aggressor side. We
    assign a uniform aggressor direction per minute (sign of body) which is
    directionally correct but ignores within-minute structure.

If the REST call fails, the engine falls back to its `started_mid_window`
mode and recovers naturally at the next 15-min boundary.
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from src.model.live_engine import LiveFeatureEngine

log = logging.getLogger("btc_predictor.backfill")

CANDLES_URL = (
    "https://api.coinbase.com/api/v3/brokerage/market/products/"
    "{product_id}/candles?start={start}&end={end}&granularity=ONE_MINUTE"
)


def _fetch_candles(product_id: str, start_sec: int, end_sec: int) -> list[dict]:
    url = CANDLES_URL.format(product_id=product_id, start=start_sec, end=end_sec)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = resp.read()
    return json.loads(body).get("candles", [])


def _inject_synthetic_candle(engine: LiveFeatureEngine, c: dict) -> None:
    """Inject 4 synthetic ticks (O→L→H→C or O→H→L→C) for one 1-min candle."""
    start_sec = int(c["start"])
    o = float(c["open"])
    h = float(c["high"])
    l = float(c["low"])
    close = float(c["close"])
    vol = float(c["volume"])

    aggressor_buy = close > o
    # 4 ticks within the minute. Spread them at 0/15/30/50 seconds so the
    # engine never folds two synthetic ticks into the same minute boundary.
    if aggressor_buy:
        seq = [(0, o), (15, l), (30, h), (50, close)]
    else:
        seq = [(0, o), (15, h), (30, l), (50, close)]
    tick_qty = max(vol / 4.0, 1e-6)
    for offset, price in seq:
        ts_ns = (start_sec + offset) * 1_000_000_000
        engine.process_tick(ts_ns, price, tick_qty, aggressor_buy)


def backfill_engine(engine: LiveFeatureEngine, product_id: str = "BTC-USD") -> bool:
    """Seed the engine with the prior + current 15-min windows.

    Returns True on success, False if the REST call failed or returned no data.
    """
    now = datetime.now(timezone.utc)
    cur_win_dt = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    prev_win_dt = cur_win_dt - timedelta(minutes=15)
    start_sec = int(prev_win_dt.timestamp())
    end_sec = int(now.timestamp())

    try:
        candles = _fetch_candles(product_id, start_sec, end_sec)
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as e:
        log.warning("Coinbase REST backfill failed: %s — engine starts mid-window", e)
        return False

    if not candles:
        log.warning("Coinbase REST returned no candles — engine starts mid-window")
        return False

    # Sort ascending by start (API may return newest first).
    candles.sort(key=lambda c: int(c["start"]))

    cur_win_sec = int(cur_win_dt.timestamp())
    prev_win_sec = int(prev_win_dt.timestamp())
    prev_candles = [c for c in candles if prev_win_sec <= int(c["start"]) < cur_win_sec]
    cur_candles = [c for c in candles if int(c["start"]) >= cur_win_sec]

    for c in prev_candles + cur_candles:
        _inject_synthetic_candle(engine, c)

    # Belt-and-suspenders: clear the stale flag regardless of how the engine
    # set it during synthetic injection.
    engine.started_mid_window = False

    log.info(
        "Backfilled %d prior + %d current 1-min candles "
        "(window_open_px=%s, current_window_ns=%s, prior_candle=%s)",
        len(prev_candles), len(cur_candles),
        engine.window_open_px, engine.current_window_ns,
        engine.prior_candle is not None,
    )
    return True
