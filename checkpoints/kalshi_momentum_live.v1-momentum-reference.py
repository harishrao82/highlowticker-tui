#!/usr/bin/env python3
"""
kalshi_momentum_live.py — live BTC/ETH/SOL/XRP momentum trader on Kalshi.

Strategy (backtested edge from kalshi_btc_momentum_sim.py):
  - At window open (t=0), snap open_price from Coinbase WS.
  - At t=ENTRY_ELAPSED (default 300s / 5 min), compute
        delta = (price_now − open_price) / open_price * 100
  - If |delta| ≥ THRESHOLD_PCT, buy SHARES of the momentum side:
        delta > 0  →  buy YES
        delta < 0  →  buy NO
  - Hold to settlement, log P&L.

Risk: SHARES=1. Max loss per trade ≈ entry price (cents). Max 4 trades/window.

Trades are appended to ~/.kalshi_momentum_trades.jsonl (one JSON per line).

Run:   python kalshi_momentum_live.py
Stop:  Ctrl-C
"""
import asyncio
import base64
import json
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import stdev

import ccxt
import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console

import btc_vol_profile

load_dotenv()
console = Console()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COINS = {
    "BTC": dict(enabled=True, series="KXBTC15M", cb_product="BTC-USD",
                kraken="BTC/USD", bitstamp="BTC/USD"),
    "ETH": dict(enabled=True, series="KXETH15M", cb_product="ETH-USD",
                kraken="ETH/USD", bitstamp="ETH/USD"),
    # SOL re-enabled. Earlier +6.7¢ Kalshi-Poly gap was measured on a fixed-
    # threshold strategy; with dynamic thresholds + per-checkpoint ask caps,
    # structurally-rich asks are filtered out automatically. Downside per
    # trade is still bounded by the max ask cap × SHARES.
    "SOL": dict(enabled=True, series="KXSOL15M", cb_product="SOL-USD",
                kraken="SOL/USD", bitstamp=None),     # bitstamp has no SOL
    "XRP": dict(enabled=True, series="KXXRP15M", cb_product="XRP-USD",
                kraken="XRP/USD", bitstamp="XRP/USD"),
}

SHARES          = 1          # fallback when balance fetch fails
MIN_ENTRY_PRICE = 0.05       # skip if ask is this cheap or lower
MAX_ENTRY_PRICE = 0.90       # absolute hard ceiling (per-checkpoint caps below)
LIMIT_BUFFER    = 0.02       # aggressive half: place limit at ask + buffer (guaranteed fill)
PATIENT_OFFSET  = 0.02       # patient half: place limit at ask - offset (2¢ cheaper, maker-fee rebate)
PATIENT_MIN_PX  = 0.01       # floor for the patient half's limit price
MARKET_FRACTION = 0.10       # fraction of each fire that goes aggressive (guaranteed fill)
                             # rest goes to patient limits — observed maker fills save ~2¢
                             # per share + zero fees vs ~$0.02/share taker fee
PATIENT_TIMEOUT_SEC = 15     # auto-cancel the patient leg if not filled within
                             # this many seconds — otherwise a late fill lands
                             # long after the original momentum signal is stale

# Portfolio-tiered share sizing. At the start of every window we fetch the
# current Kalshi balance and pick the share size from the first tier whose
# threshold the balance clears. Tiers ordered high → low.
#
#   balance >= $500   →  15 shares/entry
#   $400 - $500       →  12
#   $220 - $400       →  10
#   $150 - $220       →   5
#   below $150        →   1
SHARE_TIERS: list[tuple[float, int]] = [
    (500.0, 15),
    (400.0, 12),
    (220.0, 10),
    (150.0,  5),
    (  0.0,  1),
]

def shares_for_portfolio(value: float) -> int:
    for threshold, n in SHARE_TIERS:
        if value >= threshold:
            return n
    return 1

# Set at each window start after fetching the Kalshi balance
current_window_shares: int = SHARES

# Multi-entry checkpoints: (elapsed_sec, vol_factor, max_entry_price).
#
# vol_factor scales the instantaneous threshold against realized BTC vol
# (stddev of prior 1h 1-min log returns, expressed as %). Scallops fires
# at roughly 1.0 × realized_vol historically (Pearson +0.342 corr measured
# over 10,897 trades).
#
# Effective threshold = clip(vol_factor * vol_pct, VOL_THR_FLOOR, VOL_THR_CEILING)
#
# So in a quiet night regime (vol ≈ 0.03%):
#   T=30s   thr = 0.5 * 0.03% = 0.015%   (catches small moves)
#   T=600s  thr = 1.5 * 0.03% = 0.045%
#
# In NY open regime (vol ≈ 0.08%):
#   T=30s   thr = 0.5 * 0.08% = 0.040%
#   T=600s  thr = 1.5 * 0.08% = 0.120%
#
# max_entry_price is unchanged — calibrated from backtest win rate at each T.
CHECKPOINTS = [
    ( 30, 0.5, 0.53),
    ( 60, 0.7, 0.59),
    (120, 1.0, 0.68),
    (180, 1.0, 0.71),
    (300, 1.2, 0.79),
    (420, 1.5, 0.85),
    (600, 1.5, 0.88),
]
VOL_THR_FLOOR   = 0.015     # minimum threshold (pct) — protects against noise
VOL_THR_CEILING = 0.25      # maximum threshold (pct) — cap in violent regimes
VOL_LOOKBACK_MIN = 60       # realized vol window length

# Per-coin cap adjustment — subtracted from every checkpoint's max_ask for
# that coin. Derived from realized per-coin win rates so that each coin's
# effective max entry stays within its EV-positive zone.
#
#   Coin realized win rate → max EV+ entry (≈ win_rate − 0.02 safety)
#   BTC   66% → 0.64   so baseline caps need −0.15 pull-back
#   ETH   85% → 0.83   no adjustment (baseline caps already ≤ 0.88)
#   SOL   72% → 0.70   −0.10
#   XRP   72% → 0.70   −0.10
COIN_CAP_ADJUSTMENT: dict[str, float] = {
    "BTC": -0.15,
    "ETH":  0.00,
    "SOL": -0.10,
    "XRP": -0.10,
}

# Effective vol at window open = max(profile_slot_vol, realized_last_hour).
# Profile covers time-of-week regime shifts (9:30 ET open, CME reopen, etc.)
# while realized captures current-moment spikes.
_vol_profile: dict = {}     # loaded at startup from btc_vol_profile

COOLDOWN_SEC = 180          # skip if same side fired within this many seconds

# Worst-case per window ≈ SHARES × max(max_px) × max_entries_per_window
# Backtest: mean ~2.2 entries/window × 4 coins ~= 9 entries average,
# max 7 entries per coin × 4 coins = 28 entries theoretical max.
# At 1 share × 0.88 × 28 → ~$25 theoretical max loss / window (very rare).
# Typical window stake: ~$1-3 notional across all coins.
# Goal: prove the edge is real cheaply, then scale up SHARES once verified.

TRADE_LOG = Path.home() / ".kalshi_momentum_trades.jsonl"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
ET_OFFSET   = timedelta(hours=-4)

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)


# ── Kalshi auth ──────────────────────────────────────────────────────────────

def _sign(method: str, path: str) -> dict:
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(
        msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       _api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }

def _headers(method: str, path: str) -> dict:
    return {**_sign(method, path), "Content-Type": "application/json"}

def _ws_auth_headers() -> dict:
    return _sign("GET", "/trade-api/ws/v2")


# ── Multi-exchange crypto prices ─────────────────────────────────────────────
#
# Kalshi BTC settles to the CME CF Benchmark — a volume-weighted average across
# Coinbase, Kraken, Bitstamp, LMAX and ItBit. To match that reference more
# closely (instead of just Coinbase), we collect prices from multiple venues
# and use the blended mean for delta calculation.
#
# Coinbase: WS subscription (sub-second)
# Kraken / Bitstamp: REST polling via ccxt every ~3s
#
# `exchange_prices[exchange][coin] = latest_price`
exchange_prices: dict[str, dict[str, float]] = {
    "coinbase": {},
    "kraken":   {},
    "bitstamp": {},
}

def blended_price(coin: str) -> float | None:
    """Median of available exchange prices for `coin`. None if no data.

    Median (not mean) to reject outlier exchanges — Kraken's BTC/USD in
    particular can sit at a persistent $20-$50 discount because its main
    BTC venue is BTC/USDT, not BTC/USD. Mean would let one laggard bias the
    signal; median picks the middle value which tracks the consensus.
    """
    pxs = [p for p in (ex.get(coin) for ex in exchange_prices.values())
           if p and p > 0]
    if not pxs:
        return None
    if len(pxs) == 1:
        return pxs[0]
    pxs.sort()
    n = len(pxs)
    if n % 2:
        return pxs[n // 2]
    return (pxs[n // 2 - 1] + pxs[n // 2]) / 2.0


async def _coinbase_price_feed() -> None:
    products         = [cfg["cb_product"] for cfg in COINS.values() if cfg["enabled"]]
    product_to_coin  = {cfg["cb_product"]: coin
                        for coin, cfg in COINS.items() if cfg["enabled"]}
    while True:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=20,
                                          open_timeout=10) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": products,
                    "channel": "ticker",
                }))
                console.print(f"[dim]Coinbase WS connected — {', '.join(products)}[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for ev in msg.get("events", []):
                        for t in ev.get("tickers", []):
                            p   = t.get("product_id", "")
                            px  = float(t.get("price", 0) or 0)
                            sym = product_to_coin.get(p)
                            if sym and px > 0:
                                exchange_prices["coinbase"][sym] = px
                                _notify_price_update()
        except Exception as e:
            console.print(f"[yellow]Coinbase WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)


async def _ccxt_poll_loop(name: str, exchange, sym_map: dict[str, str],
                          interval: float = 3.0) -> None:
    """Poll one ccxt exchange for all enabled coins, write into exchange_prices[name]."""
    loop = asyncio.get_event_loop()
    first_ok = False
    while True:
        for coin, symbol in sym_map.items():
            if not symbol:
                continue
            try:
                ticker = await loop.run_in_executor(None, exchange.fetch_ticker, symbol)
                px = ticker.get("last") or ticker.get("close")
                if px and float(px) > 0:
                    exchange_prices[name][coin] = float(px)
                    _notify_price_update()
                    if not first_ok:
                        console.print(f"[dim]{name} REST connected — {coin}=${float(px):,.2f}[/dim]")
                        first_ok = True
            except Exception:
                # Quietly skip transient errors so the poll loop keeps running
                pass
        await asyncio.sleep(interval)


_cb_historical = ccxt.coinbase()   # used for one-shot mid-window open fetches

async def _fetch_open_at(cb_product: str, ts: int) -> float | None:
    """Fetch the 1-min candle open price at or just after `ts` (unix seconds).

    Used when we join a window already in progress and need the historical
    coin price at the 15-min boundary. Returns None on failure.
    """
    loop = asyncio.get_event_loop()
    try:
        ohlcv = await loop.run_in_executor(
            None,
            lambda: _cb_historical.fetch_ohlcv(cb_product, "1m", ts * 1000, 2),
        )
        if ohlcv:
            return float(ohlcv[0][1])   # open of first candle
    except Exception as e:
        console.print(f"  [dim yellow]historical fetch {cb_product}: {e}[/dim yellow]")
    return None


async def _realized_vol_pct(cb_product: str, end_ts: int,
                            minutes: int = VOL_LOOKBACK_MIN) -> float:
    """Stddev of 1-min log returns over the prior `minutes` minutes,
    expressed as a percentage of price. Used to scale the momentum threshold.
    Returns 0 on failure (caller will fall back to VOL_THR_FLOOR).
    """
    loop = asyncio.get_event_loop()
    start_ms = (end_ts - minutes * 60) * 1000
    try:
        ohlcv = await loop.run_in_executor(
            None,
            lambda: _cb_historical.fetch_ohlcv(cb_product, "1m", start_ms, minutes + 5),
        )
        if not ohlcv or len(ohlcv) < 10:
            return 0.0
        closes = [float(r[4]) for r in ohlcv if r[4]]
        rets = []
        for i in range(1, len(closes)):
            if closes[i-1] > 0 and closes[i] > 0:
                rets.append(math.log(closes[i] / closes[i-1]))
        if len(rets) < 5:
            return 0.0
        return stdev(rets) * 100.0
    except Exception as e:
        console.print(f"  [dim yellow]vol fetch {cb_product}: {e}[/dim yellow]")
        return 0.0


async def _start_secondary_feeds() -> None:
    """Spawn ccxt polling loops for Kraken and Bitstamp."""
    kraken   = ccxt.kraken()
    bitstamp = ccxt.bitstamp()

    kraken_map = {
        coin: cfg.get("kraken")
        for coin, cfg in COINS.items() if cfg["enabled"] and cfg.get("kraken")
    }
    bitstamp_map = {
        coin: cfg.get("bitstamp")
        for coin, cfg in COINS.items() if cfg["enabled"] and cfg.get("bitstamp")
    }

    if kraken_map:
        asyncio.create_task(_ccxt_poll_loop("kraken", kraken, kraken_map))
    if bitstamp_map:
        asyncio.create_task(_ccxt_poll_loop("bitstamp", bitstamp, bitstamp_map))


# ── Kalshi yes/no asks (WS) ──────────────────────────────────────────────────

# kalshi_state[sym] = {ticker, yes_ask, no_ask, open_price, triggered, trade, ...}
kalshi_state:   dict[str, dict] = {}
_ticker_to_sym: dict[str, str]  = {}
current_window_start_ts: int = 0     # set by run_window so status loop can compute elapsed

STATUS_INTERVAL_SEC = float(os.environ.get("STATUS_INTERVAL", "10"))  # how often the live status line prints
STATUS_LOOKBACK_SEC = float(os.environ.get("STATUS_LOOKBACK", "10"))  # delta-of-delta look-back window
STATUS_REST_REFRESH_SEC = 10         # min seconds between REST ask refreshes (rate-limit safety)
EVAL_DEBOUNCE_SEC   = 0.5            # min seconds between evaluator runs

# Snapshot log (one JSON object per coin per status tick) for offline analysis
STATUS_SNAPSHOT_LOG = Path.home() / ".kalshi_status_snapshots.jsonl"

# Event-driven evaluator: price feeds .set() this every time they get new data,
# the evaluator loop waits on it. None when no window is active.
_price_update_event: asyncio.Event | None = None

def _notify_price_update() -> None:
    """Called by every price feed after writing new data to state."""
    if _price_update_event is not None:
        _price_update_event.set()

async def _kalshi_ws_feed() -> None:
    _msg_id = 0
    def _next() -> int:
        nonlocal _msg_id
        _msg_id += 1
        return _msg_id

    subscribed: set[str] = set()

    while True:
        resub_task = None
        try:
            async with websockets.connect(
                KALSHI_WS, additional_headers=_ws_auth_headers(),
                ping_interval=20, open_timeout=10,
            ) as ws:
                console.print("[dim]Kalshi WS connected[/dim]")
                subscribed.clear()

                async def _subscribe_new() -> None:
                    new = set(_ticker_to_sym.keys()) - subscribed
                    if new:
                        await ws.send(json.dumps({
                            "id": _next(), "cmd": "subscribe",
                            "params": {
                                "channels":       ["ticker"],
                                "market_tickers": list(new),
                            },
                        }))
                        subscribed.update(new)
                        console.print(f"[dim]Kalshi WS  subscribed {len(new)} new tickers[/dim]")

                # Periodic resub task — runs in parallel with the message reader
                # so new windows' tickers get subscribed within 2s of being added,
                # even when no messages are flowing.
                async def _resub_loop() -> None:
                    while True:
                        try:
                            await _subscribe_new()
                        except Exception:
                            return
                        await asyncio.sleep(2)

                await _subscribe_new()
                resub_task = asyncio.create_task(_resub_loop())

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") != "ticker":
                        continue
                    d   = msg.get("msg", msg)
                    t   = d.get("market_ticker", "")
                    ya  = float(d.get("yes_ask_dollars", 0) or 0)
                    yb  = float(d.get("yes_bid_dollars", 0) or 0)
                    na  = round(1.0 - yb, 4) if yb > 0 else 0.0
                    sym = _ticker_to_sym.get(t)
                    if sym and sym in kalshi_state:
                        if 0 < ya < 1:
                            kalshi_state[sym]["yes_ask"] = ya
                        if 0 < na < 1:
                            kalshi_state[sym]["no_ask"] = na
                        _notify_price_update()

        except Exception as e:
            console.print(f"[yellow]Kalshi WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)
        finally:
            if resub_task and not resub_task.done():
                resub_task.cancel()


# ── Kalshi REST helpers ──────────────────────────────────────────────────────

async def _poll_asks_once(client: httpx.AsyncClient, ticker: str) -> tuple[float, float]:
    """REST poll for the current order-book asks.

    Uses the same convention as the Kalshi WebSocket feed and btc_recorder.py:
    derive no_ask from yes_bid via `na = 1 - yes_bid`. Boundary values
    (yes_ask=0 or yes_bid=0) indicate an empty book — return (0,0) so the
    caller can treat them as 'not yet ready' rather than overwriting state
    with no_ask=1.00 placeholder values.
    """
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            m   = r.json().get("market", {})
            ya  = float(m.get("yes_ask_dollars") or 0)
            yb  = float(m.get("yes_bid_dollars") or 0)
            # Empty / boundary book — treat as not-ready
            if ya <= 0 or ya >= 1 or yb <= 0 or yb >= 1:
                return 0.0, 0.0
            na  = round(1.0 - yb, 4)
            return ya, na
    except Exception as e:
        console.print(f"  [dim yellow]poll asks {ticker}: {e}[/dim yellow]")
    return 0.0, 0.0


async def _fetch_result(client: httpx.AsyncClient, ticker: str) -> str:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            return r.json().get("market", {}).get("result", "") or ""
    except Exception as e:
        console.print(f"  [dim yellow]result fetch {ticker}: {e}[/dim yellow]")
    return ""


async def _cancel_single(client: httpx.AsyncClient, order_id: str,
                         tag: str = "") -> bool:
    """DELETE a single Kalshi order. Returns True if cancellation was accepted
    (200 / 202). False if already filled (404) or other error.
    """
    if not order_id:
        return False
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    try:
        r = await client.request(
            "DELETE",
            f"{KALSHI_BASE}/portfolio/orders/{order_id}",
            headers=_headers("DELETE", path), timeout=8,
        )
        if r.status_code in (200, 202):
            console.print(f"  [dim]cancel {tag or order_id[:8]} ok[/dim]")
            return True
        if r.status_code == 404:
            # Already filled or already cancelled — silent no-op
            return False
        console.print(f"  [yellow]cancel {tag or order_id[:8]} "
                      f"{r.status_code}: {r.text[:100]}[/yellow]")
    except Exception as e:
        console.print(f"  [dim yellow]cancel {tag or order_id[:8]}: {e}[/dim yellow]")
    return False


async def _cancel_patient_after(client: httpx.AsyncClient, order_id: str,
                                tag: str,
                                delay_sec: float = PATIENT_TIMEOUT_SEC) -> None:
    """Background task: wait `delay_sec`, then attempt to cancel the patient
    order. If it already filled by then, the DELETE silently no-ops."""
    await asyncio.sleep(delay_sec)
    await _cancel_single(client, order_id, tag=tag)


async def _fetch_floor(client: httpx.AsyncClient, ticker: str) -> float | None:
    """Fetch Kalshi's floor_strike for a market. This is the exact price
    reference Kalshi uses to settle the market — closer to our delta
    computation than any external spot feed (matches the CFB internal
    reference which appears to lock ~2 minutes before window open)."""
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            fs = r.json().get("market", {}).get("floor_strike")
            if fs:
                return float(fs)
    except Exception as e:
        console.print(f"  [dim yellow]floor fetch {ticker}: {e}[/dim yellow]")
    return None


async def _fetch_balance(client: httpx.AsyncClient) -> float | None:
    """Fetch total Kalshi portfolio value (cash + open positions) in dollars.
    Uses `balance` + `portfolio_value` fields (both in cents). Returns None
    on error."""
    path = "/trade-api/v2/portfolio/balance"
    try:
        r = await client.get(f"{KALSHI_BASE}/portfolio/balance",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            j = r.json()
            cash      = float(j.get("balance") or 0)          # cents
            positions = float(j.get("portfolio_value") or 0)  # cents
            total = (cash + positions) / 100.0
            return total
    except Exception as e:
        console.print(f"  [dim yellow]balance fetch: {e}[/dim yellow]")
    return None


async def _place_buy(client: httpx.AsyncClient, ticker: str, side: str,
                     price: float, shares: int, tag: str) -> dict | None:
    """Limit buy at `price`. Returns order dict on success else None."""
    yes_price = f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"
    coid = str(uuid.uuid4())
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps({
                "ticker":            ticker,
                "action":            "buy",
                "side":              side,
                "count":             shares,
                "type":              "limit",
                "yes_price_dollars": yes_price,
                "client_order_id":   coid,
            }),
            timeout=10,
        )
        if r.status_code in (200, 201):
            order = r.json().get("order", {}) or {}
            oid   = order.get("order_id", "")
            console.print(f"  [bold green]{tag}  BUY {side.upper()} "
                          f"{shares}ct @ {price:.2f}  {oid[:8]}[/bold green]")
            return {"order_id": oid, "client_order_id": coid, "raw": order}
        else:
            console.print(f"  [red]{tag}  order failed {r.status_code}: {r.text[:150]}[/red]")
    except Exception as e:
        console.print(f"  [red]{tag}  order error: {e}[/red]")
    return None


# ── Ticker + timing helpers ──────────────────────────────────────────────────

def _seconds_until_next_window() -> float:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    cur    = now_et.replace(minute=mins, second=0, microsecond=0)
    return (cur + timedelta(minutes=15) - now_et).total_seconds()

def _current_window_start_utc() -> datetime:
    now = datetime.now(timezone.utc)
    mins = (now.minute // 15) * 15
    return now.replace(minute=mins, second=0, microsecond=0)

def _ticker_for(series: str, close_utc: datetime) -> str:
    close_et = close_utc + ET_OFFSET
    return (series + "-"
            + close_et.strftime("%y%b%d%H%M").upper()
            + "-" + close_et.strftime("%M"))


# ── Trade log ────────────────────────────────────────────────────────────────

def _log_trade(entry: dict) -> None:
    try:
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        console.print(f"[yellow]trade log write error: {e}[/yellow]")


# ── Running P&L (this session only) ─────────────────────────────────────────

session_stats = {"trades": 0, "wins": 0, "losses": 0, "pending": 0,
                 "gross_pnl": 0.0}


# ── Live status loop ─────────────────────────────────────────────────────────
#
# Prints one block every STATUS_INTERVAL_SEC showing per-coin live state.
# Also refreshes Kalshi yes/no asks via REST each tick — Kalshi WS only emits
# events when the book changes, so REST polling keeps the displayed values
# from going stale on quiet markets.

def _fmt_ask(v) -> str:
    """Show '?' for clearly-uninitialized ask values."""
    if v is None or v <= 0 or v >= 1.0:
        return "?"
    return f"{v:.2f}"


async def _status_loop() -> None:
    last_rest_refresh = 0.0
    # Per-coin rolling history: list of (ts, delta_pct, yes_ask, no_ask).
    # Used to compute look-back deltas (leading-indicator check).
    history: dict[str, list[tuple[float, float, float, float]]] = {
        sym: [] for sym in COINS
    }
    LOOKBACK_SEC = STATUS_LOOKBACK_SEC

    def _snapshot_log(rec: dict) -> None:
        try:
            with open(STATUS_SNAPSHOT_LOG, "a") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception:
            pass

    async with httpx.AsyncClient() as client:
        while True:
            await asyncio.sleep(STATUS_INTERVAL_SEC)
            if not kalshi_state or current_window_start_ts == 0:
                continue
            elapsed = int(time.time() - current_window_start_ts)
            if elapsed < 0 or elapsed > 900:
                continue

            # Rate-limited REST refresh: always run at ≥ 10s interval,
            # regardless of how fast the display ticks.
            now_ts = time.time()
            if now_ts - last_rest_refresh >= STATUS_REST_REFRESH_SEC:
                last_rest_refresh = now_ts
                async def _refresh(sym: str) -> None:
                    ya, na = await _poll_asks_once(client, kalshi_state[sym]["ticker"])
                    if 0 < ya < 1: kalshi_state[sym]["yes_ask"] = ya
                    if 0 < na < 1: kalshi_state[sym]["no_ask"]  = na
                try:
                    await asyncio.gather(*[_refresh(s) for s in kalshi_state])
                except Exception:
                    pass

            ts = datetime.now().strftime("%H:%M:%S")
            lines = [f"[dim][{ts}][/dim] [bold]+{elapsed:>3}s[/bold]"]
            for sym, st in kalshi_state.items():
                now_p  = blended_price(sym)
                open_p = st.get("open_price")
                ya     = st.get("yes_ask")
                na     = st.get("no_ask")

                ya_str = _fmt_ask(ya)
                na_str = _fmt_ask(na)

                floor_p = st.get("floor_strike")
                if open_p and now_p:
                    delta = (now_p - open_p) / open_p * 100.0
                    d_col = "green" if delta > 0 else "red" if delta < 0 else "white"
                    # ITM = how far above/below Kalshi's strike we currently are
                    itm = None
                    if floor_p:
                        itm = (now_p - floor_p) / floor_p * 100.0
                    cb_p = exchange_prices["coinbase"].get(sym)
                    kr_p = exchange_prices["kraken"].get(sym)
                    bs_p = exchange_prices["bitstamp"].get(sym)
                    ex_pxs = [p for p in (cb_p, kr_p, bs_p) if p]
                    if len(ex_pxs) >= 2 and now_p > 0:
                        spread_pct = (max(ex_pxs) - min(ex_pxs)) / now_p * 100.0
                        spread_str = f"{spread_pct*100:.1f}bps"
                        spread_bps = round(spread_pct * 100, 2)
                    else:
                        spread_str = "—"
                        spread_bps = None

                    # Push to history + prune old (keep ~60s)
                    h = history.setdefault(sym, [])
                    h.append((now_ts, delta, ya or 0.0, na or 0.0))
                    cutoff = now_ts - 60
                    while h and h[0][0] < cutoff:
                        h.pop(0)

                    # Look back ~LOOKBACK_SEC seconds
                    target_ts = now_ts - LOOKBACK_SEC
                    prior = None
                    for entry in h:
                        if entry[0] <= target_ts:
                            prior = entry
                        else:
                            break
                    if prior is not None:
                        d_dd = delta - prior[1]
                        d_ya = (ya or 0.0) - prior[2]
                        d_na = (na or 0.0) - prior[3]
                        dd_col = "green" if d_dd > 0 else "red" if d_dd < 0 else "dim"
                        ya_col = "green" if d_ya > 0 else "red" if d_ya < 0 else "dim"
                        na_col = "green" if d_na > 0 else "red" if d_na < 0 else "dim"
                        change_str = (
                            f"  [dim]{int(LOOKBACK_SEC)}s:[/dim] "
                            f"[{dd_col}]ΔΔ{d_dd:+.3f}%[/{dd_col}] "
                            f"[{ya_col}]Δyes{d_ya:+.2f}[/{ya_col}] "
                            f"[{na_col}]Δno{d_na:+.2f}[/{na_col}]"
                        )
                    else:
                        d_dd = d_ya = d_na = None
                        change_str = f"  [dim]{int(LOOKBACK_SEC)}s: —[/dim]"

                    if itm is not None:
                        itm_col = "green" if itm > 0 else "red" if itm < 0 else "dim"
                        itm_str = f"  [dim]ITM[/dim] [{itm_col}]{itm:+.3f}%[/{itm_col}]"
                    else:
                        itm_str = ""

                    lines.append(
                        f"  {sym:3}  open ${open_p:>10,.2f}  now ${now_p:>10,.2f}  "
                        f"[{d_col}]Δ{delta:+.3f}%[/{d_col}]{itm_str}  "
                        f"K yes={ya_str}/no={na_str}  "
                        f"[dim]ex_spread {spread_str}[/dim]"
                        f"{change_str}"
                    )

                    # Persist a structured snapshot for offline analysis
                    _snapshot_log({
                        "ts":            round(now_ts, 3),
                        "elapsed":       elapsed,
                        "coin":          sym,
                        "ticker":        st.get("ticker"),
                        "open_price":    round(open_p, 6),
                        "floor_strike":  round(floor_p, 6) if floor_p else None,
                        "now_price":     round(now_p, 6),
                        "delta_pct":     round(delta, 6),
                        "itm_pct":       round(itm, 6) if itm is not None else None,
                        "coinbase_px":   cb_p,
                        "kraken_px":     kr_p,
                        "bitstamp_px":   bs_p,
                        "ex_spread_bps": spread_bps,
                        "yes_ask":       (ya if ya and 0 < ya < 1 else None),
                        "no_ask":        (na if na and 0 < na < 1 else None),
                        "lookback_sec":  LOOKBACK_SEC,
                        "dd_lookback":   round(d_dd, 6) if d_dd is not None else None,
                        "dyes_lookback": round(d_ya, 4) if d_ya is not None else None,
                        "dno_lookback":  round(d_na, 4) if d_na is not None else None,
                    })
                elif now_p:
                    lines.append(
                        f"  {sym:3}  open=?               now ${now_p:>10,.2f}  "
                        f"Δ=?         K yes={ya_str}/no={na_str}"
                    )
                else:
                    lines.append(f"  {sym:3}  [dim]no price data[/dim]")
            console.print("\n".join(lines))


# ── Per-window runner ────────────────────────────────────────────────────────

async def run_window(client: httpx.AsyncClient, window_num: int,
                     window_start_utc: datetime) -> None:
    global current_window_start_ts, current_window_shares
    close_utc        = window_start_utc + timedelta(minutes=15)
    window_start_ts  = int(window_start_utc.timestamp())
    current_window_start_ts = window_start_ts
    label            = (window_start_utc + ET_OFFSET).strftime("%H:%M ET")

    console.rule(f"[bold cyan]━━ Window {window_num}  {label} ━━[/bold cyan]")

    # Fetch Kalshi cash balance and pick shares/entry tier for this window
    balance = await _fetch_balance(client)
    if balance is not None:
        current_window_shares = shares_for_portfolio(balance)
        console.print(
            f"  [dim]portfolio ${balance:,.2f} → shares/entry = "
            f"[bold]{current_window_shares}[/bold][/dim]"
        )
    else:
        console.print(
            f"  [yellow]balance fetch failed — keeping last shares/entry = "
            f"{current_window_shares}[/yellow]"
        )

    # Fresh state for this window
    kalshi_state.clear()
    _ticker_to_sym.clear()
    for sym, cfg in COINS.items():
        if not cfg["enabled"]:
            continue
        ticker = _ticker_for(cfg["series"], close_utc)
        kalshi_state[sym] = {
            "ticker":          ticker,
            "yes_ask":         0.0,
            "no_ask":          0.0,
            "open_price":      None,     # blended spot at window open — for momentum delta
            "floor_strike":    None,     # Kalshi's settlement reference — for ITM display
            "trades":          [],       # list of dicts, one per fired entry
            "last_fired_yes":  -10_000,  # elapsed_sec of last yes fire
            "last_fired_no":   -10_000,  # elapsed_sec of last no fire
        }
        _ticker_to_sym[ticker] = sym
        console.print(f"  [dim]{sym}[/dim]  {ticker}")

    # Seed asks via one REST poll each (WS updates will follow)
    async def _seed(sym: str) -> None:
        ya, na = await _poll_asks_once(client, kalshi_state[sym]["ticker"])
        if ya > 0: kalshi_state[sym]["yes_ask"] = ya
        if na > 0: kalshi_state[sym]["no_ask"]  = na
    await asyncio.gather(*[_seed(s) for s in kalshi_state])

    # Snap two references per coin:
    #   (a) open_price = blended spot at window open  → for momentum delta
    #       (matches old behavior; clean "in-window move only" signal)
    #   (b) floor_strike = Kalshi's REST value        → for ITM display
    #       (what Kalshi actually settles on; informational)
    async def _snap_one(sym: str) -> None:
        st = kalshi_state[sym]

        # Momentum reference: snap current blended spot. If we joined mid-window,
        # fetch the historical 1-min candle at window_start_ts so the "open" is
        # actually the window's open, not a late blended value.
        now_ts_ = int(time.time())
        if now_ts_ - window_start_ts <= 10:
            # On-time — use current blended
            px = blended_price(sym)
            source_label = "live blended"
        else:
            # Mid-window join — fetch historical
            cb = COINS[sym]["cb_product"]
            px = await _fetch_open_at(cb, window_start_ts)
            source_label = "historical 1m candle"
            if not px:
                px = blended_price(sym)
                source_label = "fallback live"
        if px:
            st["open_price"] = px

        # Settlement reference: Kalshi floor_strike (independent fetch)
        fs = await _fetch_floor(client, st["ticker"])
        if fs:
            st["floor_strike"] = fs

        # Log both for this coin
        if px and fs:
            itm_pct = (px - fs) / fs * 100
            itm_col = "green" if itm_pct >= 0 else "red"
            console.print(
                f"  [dim]{sym} open ${px:,.2f}  ({source_label})  "
                f"floor ${fs:,.2f}  "
                f"ITM [{itm_col}]{itm_pct:+.3f}%[/{itm_col}][/dim]"
            )
        elif px:
            console.print(
                f"  [yellow]{sym} open ${px:,.2f}  ({source_label})  "
                f"floor_strike fetch failed[/yellow]"
            )
        else:
            console.print(f"  [red]{sym} no open price — will skip trigger[/red]")

    await asyncio.gather(*[_snap_one(s) for s in kalshi_state])

    # Compute per-coin effective vol = max(profile_slot, realized_1h).
    # Profile catches time-of-week regime shifts, realized catches spikes.
    vol_by_coin:     dict[str, float] = {}
    realized_by_coin: dict[str, float] = {}
    profile_by_coin: dict[str, float] = {}
    async def _compute_vol(sym: str) -> None:
        cb = COINS[sym]["cb_product"]
        realized = await _realized_vol_pct(cb, window_start_ts)
        slot_dt  = datetime.fromtimestamp(window_start_ts, tz=timezone.utc)
        slot     = btc_vol_profile.expected_vol(_vol_profile, sym, slot_dt)
        effective = max(realized, slot) if (realized or slot) else 0.0
        realized_by_coin[sym] = realized
        profile_by_coin[sym]  = slot
        vol_by_coin[sym]      = effective
    await asyncio.gather(*[_compute_vol(s) for s in kalshi_state])

    console.print(f"\n  [bold]Vol profile + dynamic thresholds:[/bold]")
    for sym in kalshi_state:
        rv = realized_by_coin.get(sym, 0.0)
        pv = profile_by_coin.get(sym, 0.0)
        ev = vol_by_coin.get(sym, 0.0)
        src = "realized" if rv >= pv and rv > 0 else "profile" if pv > 0 else "floor"
        if ev <= 0:
            console.print(f"    [yellow]{sym}  no vol data — floor {VOL_THR_FLOOR}%[/yellow]")
            continue
        adj = COIN_CAP_ADJUSTMENT.get(sym, 0.0)
        thr_row = []
        cap_row = []
        for T, factor, base_cap in CHECKPOINTS:
            thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * ev))
            eff_cap = base_cap + adj
            thr_row.append(f"T{T}={thr:.3f}%")
            cap_row.append(f"T{T}={eff_cap:.2f}")
        adj_str = f" cap_adj={adj:+.2f}" if adj else ""
        console.print(
            f"    [dim]{sym}  realized={rv:.4f}%  profile={pv:.4f}%  "
            f"eff={ev:.4f}% ({src}){adj_str}[/dim]"
        )
        console.print(f"       [dim]thr: {' '.join(thr_row)}[/dim]")
        if adj:
            console.print(f"       [dim]cap: {' '.join(cap_row)}[/dim]")

    # ── Per-coin evaluator (fires when signal + cooldown allow) ────────────
    # Threshold is dynamic: factor × realized_vol (clipped to floor/ceiling).
    async def _eval_checkpoint(sym: str, T_eff: int, factor: float,
                               max_px: float, elapsed_now: int) -> None:
        st     = kalshi_state[sym]
        open_p = st["open_price"]
        now_p  = blended_price(sym)

        if not open_p or not now_p:
            return

        delta = (now_p - open_p) / open_p * 100.0

        # Dynamic threshold from realized vol
        vol = vol_by_coin.get(sym, 0.0)
        thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * vol)) if vol > 0 else VOL_THR_FLOOR

        if abs(delta) < thr:
            return

        side = "yes" if delta > 0 else "no"

        # Cooldown: skip if same side fired within COOLDOWN_SEC (using real elapsed)
        last = st["last_fired_yes"] if side == "yes" else st["last_fired_no"]
        if elapsed_now - last < COOLDOWN_SEC:
            return

        cur_ask = st["yes_ask"] if side == "yes" else st["no_ask"]
        if cur_ask <= 0 or cur_ask >= 1:
            return
        if cur_ask < MIN_ENTRY_PRICE:
            return
        # Per-coin cap adjustment — tighter for historically-lower win-rate coins
        eff_cap = max_px + COIN_CAP_ADJUSTMENT.get(sym, 0.0)
        if cur_ask > eff_cap or cur_ask > MAX_ENTRY_PRICE:
            return

        shares_this = current_window_shares

        # Split logic: shares > 1 → MARKET_FRACTION aggressive (guaranteed fill) +
        # rest patient (limit 2¢ below ask, earns maker rebate). Always keep
        # at least 1 share on the aggressive leg so we never miss the window
        # if all patient limits fail to fill.
        if shares_this > 1:
            n_agg = max(1, int(round(shares_this * MARKET_FRACTION)))
            n_pat = shares_this - n_agg
        else:
            n_agg = 1
            n_pat = 0

        agg_px     = round(min(cur_ask + LIMIT_BUFFER, eff_cap), 2)
        patient_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)

        console.print(
            f"  [bold cyan]T={elapsed_now:>3}s  {sym}  open=${open_p:,.2f} "
            f"now=${now_p:,.2f}  Δ={delta:+.3f}% ≥ {thr:.3f}% "
            f"(vol={vol:.3f}% × {factor}) → buy {side.upper()} "
            f"(ask={cur_ask:.2f}, cap={eff_cap:.2f})  "
            f"split: {n_agg}@{agg_px:.2f} + {n_pat}@{patient_px:.2f}[/bold cyan]"
        )

        async def _record(order: dict | None, n_shares: int,
                          limit_px_used: float, role: str) -> None:
            if not order:
                return
            st["trades"].append({
                "T_checkpoint":     T_eff,
                "elapsed_actual":   elapsed_now,
                "vol_pct":          vol,
                "factor":           factor,
                "effective_thr":    thr,
                "max_px":           max_px,
                "eff_cap":          eff_cap,
                "side":             side,
                "delta_pct":        delta,
                "open_price":       open_p,
                "trigger_price":    now_p,
                "ask_at_trigger":   cur_ask,
                "limit_price_sent": limit_px_used,
                "shares":           n_shares,
                "role":             role,     # "market" or "patient"
                "order_id":         order["order_id"],
                "client_order_id":  order["client_order_id"],
                "placed_at":        datetime.now().isoformat(),
            })
            session_stats["pending"] += 1

        # Aggressive / guaranteed-fill leg
        agg_order = await _place_buy(client, st["ticker"], side, agg_px,
                                     n_agg, f"{sym}@T{elapsed_now}/M")
        await _record(agg_order, n_agg, agg_px, "market")

        # Patient 2¢-below leg (only if > 1 share total).
        # Schedule an auto-cancel after PATIENT_TIMEOUT_SEC so that if the
        # book doesn't come back to our level in time, we bail — we don't
        # want fills landing long after the original signal is stale.
        pat_order = None
        if n_pat > 0:
            pat_order = await _place_buy(client, st["ticker"], side,
                                         patient_px, n_pat,
                                         f"{sym}@T{elapsed_now}/L")
            await _record(pat_order, n_pat, patient_px, "patient")
            if pat_order:
                asyncio.create_task(_cancel_patient_after(
                    client,
                    pat_order["order_id"],
                    tag=f"{sym}@T{elapsed_now}/L",
                ))

        # Cooldown updates on any successful fire
        if agg_order or pat_order:
            if side == "yes":
                st["last_fired_yes"] = elapsed_now
            else:
                st["last_fired_no"] = elapsed_now

    def _effective_checkpoint(elapsed: int) -> tuple[int, float, float] | None:
        """Return the most recent (T, factor, max_px) whose T is <= elapsed."""
        best = None
        for T, factor, cap in CHECKPOINTS:
            if elapsed >= T:
                best = (T, factor, cap)
            else:
                break
        return best

    # ── Event-driven continuous evaluator ─────────────────────────────────
    # The evaluator waits on `_price_update_event`. Each price feed (Coinbase
    # WS / Kraken / Bitstamp / Kalshi WS) .set()s the event after writing new
    # data, which wakes us up. On each wake we find the effective checkpoint
    # and evaluate every coin. Debounced so we don't spam on noisy feeds.
    global _price_update_event
    _price_update_event = asyncio.Event()
    try:
        last_announced_cp: tuple[int, float, float] | None = None
        last_eval_ts = 0.0

        while True:
            now = time.time()
            elapsed = int(now - window_start_ts)
            if elapsed >= 900:
                break

            effective = _effective_checkpoint(elapsed)
            if effective is not None:
                if effective != last_announced_cp:
                    last_announced_cp = effective
                    T, factor, cap = effective
                    ts_s = datetime.now().strftime("%H:%M:%S")
                    # Show effective threshold range across coins at this factor
                    thrs = []
                    for s in kalshi_state:
                        v = vol_by_coin.get(s, 0)
                        if v > 0:
                            thrs.append(max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * v)))
                    thr_str = (f"thr {min(thrs):.3f}–{max(thrs):.3f}%"
                               if thrs else f"thr floor {VOL_THR_FLOOR}%")
                    console.print(
                        f"\n[bold]  [{ts_s}] entering T≥{T}s zone  "
                        f"(factor={factor}, {thr_str}, max ask {cap:.2f})[/bold]"
                    )

                if now - last_eval_ts >= EVAL_DEBOUNCE_SEC:
                    last_eval_ts = now
                    T, factor, cap = effective
                    await asyncio.gather(*[
                        _eval_checkpoint(s, T, factor, cap, elapsed)
                        for s in kalshi_state
                    ])

            # Wait for any price feed to push a new value (or 1s timeout)
            try:
                _price_update_event.clear()
                await asyncio.wait_for(_price_update_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    finally:
        _price_update_event = None

    # Wait until window close + small buffer for result
    close_ts = window_start_ts + 895
    while time.time() < close_ts:
        await asyncio.sleep(5)
    await asyncio.sleep(8)   # let Kalshi finalize result

    # Resolve winners + log each trade
    console.print(f"\n[bold]  Window {window_num} results[/bold]")
    for sym, st in kalshi_state.items():
        if not st["trades"]:
            continue
        result = await _fetch_result(client, st["ticker"])
        coin_total_pnl = 0.0
        for trade in st["trades"]:
            entry = trade["ask_at_trigger"]
            side  = trade["side"]

            shares_trade = trade.get("shares", SHARES)
            log_entry = {
                "ts_utc":          datetime.now(timezone.utc).isoformat(),
                "window":          st["ticker"],
                "coin":            sym,
                "window_start_ts": window_start_ts,
                "T_checkpoint":    trade["T_checkpoint"],
                "elapsed_actual":  trade["elapsed_actual"],
                "vol_pct":         trade.get("vol_pct"),
                "factor":          trade.get("factor"),
                "effective_thr":   trade.get("effective_thr"),
                "open_price":      trade["open_price"],
                "trigger_price":   trade["trigger_price"],
                "delta_pct":       round(trade["delta_pct"], 4),
                "side":            side,
                "limit_price_sent":trade["limit_price_sent"],
                "ask_at_trigger":  entry,
                "shares":          shares_trade,
                "order_id":        trade["order_id"],
                "winner":          result,
                "won":             None,
                "pnl":             None,
            }

            session_stats["pending"] -= 1

            if result in ("yes", "no"):
                won = (side == result)
                pnl = (1.0 - entry) * shares_trade if won else -entry * shares_trade
                log_entry["won"] = won
                log_entry["pnl"] = round(pnl, 4)
                coin_total_pnl += pnl

                mark = "[green]WIN[/green]" if won else "[red]LOSS[/red]"
                console.print(
                    f"    {sym} T={trade['elapsed_actual']:>3}s  {side.upper()}@{entry:.2f}  "
                    f"result={result.upper()}  {mark}  P&L ${pnl:+.2f}"
                )

                session_stats["trades"]   += 1
                session_stats["wins"]     += int(won)
                session_stats["losses"]   += int(not won)
                session_stats["gross_pnl"] = round(session_stats["gross_pnl"] + pnl, 4)
            else:
                console.print(f"    {sym} T={trade['elapsed_actual']:>3}s  {side.upper()}@{entry:.2f}  "
                              f"[yellow]result=?[/yellow]")

            _log_trade(log_entry)

        if result in ("yes", "no") and len(st["trades"]) > 1:
            col = "green" if coin_total_pnl >= 0 else "red"
            console.print(
                f"    [bold]{sym} window total: "
                f"[{col}]${coin_total_pnl:+.2f}[/{col}] "
                f"({len(st['trades'])} entries)[/bold]"
            )

    # Session running totals
    s = session_stats
    if s["trades"] > 0:
        wr = s["wins"] / s["trades"] * 100
        col = "green" if s["gross_pnl"] >= 0 else "red"
        console.print(
            f"\n  [bold]Session[/bold]  trades {s['trades']}  "
            f"wins {s['wins']}  losses {s['losses']}  "
            f"win% {wr:.0f}%  "
            f"pending {s['pending']}  "
            f"gross P&L [{col}]${s['gross_pnl']:+.2f}[/{col}]"
        )


# ── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]Kalshi Momentum Live  (multi-entry, EV-capped, vol-adaptive)[/bold cyan]")
    enabled_coins = [s for s, c in COINS.items() if c["enabled"]]
    console.print(f"  enabled coins: {', '.join(enabled_coins)}  "
                  f"(disabled: {', '.join(s for s,c in COINS.items() if not c['enabled']) or 'none'})")
    console.print(f"  price source: Coinbase WS + Kraken REST + Bitstamp REST  "
                  f"(blended → matches Kalshi CFB more closely)")
    console.print(f"  dynamic threshold = clip(factor × max(profile_slot, realized_1h), "
                  f"{VOL_THR_FLOOR}%, {VOL_THR_CEILING}%)")
    console.print(f"  vol profile = btc_vol_profile.load_or_build (21d history, UTC dow×hour bins)")
    console.print(f"  checkpoints (T / vol-factor / max-ask):")
    for T, factor, max_px in CHECKPOINTS:
        console.print(f"    T={T:>3}s   factor={factor}   ask ≤ {max_px:.2f}")
    console.print(
        f"  shares/entry tiered by Kalshi balance at each window open:"
    )
    for threshold, n in SHARE_TIERS:
        if threshold > 0:
            console.print(f"    balance ≥ ${threshold:>6.0f}  →  {n} shares/entry")
        else:
            console.print(f"    balance <  ${SHARE_TIERS[-2][0]:>6.0f}  →  {n} shares/entry")
    console.print(
        f"  cooldown: {COOLDOWN_SEC}s between same-side fires on same coin"
    )
    console.print(
        f"  price range filter: [{MIN_ENTRY_PRICE:.2f}, {MAX_ENTRY_PRICE:.2f}]  "
        f"limit at ask + {LIMIT_BUFFER:.2f}"
    )
    console.print(f"  coins: {', '.join(s for s, c in COINS.items() if c['enabled'])}")
    console.print(f"  trade log: {TRADE_LOG}\n")

    async with httpx.AsyncClient() as client:
        # Load time-of-week vol profile (builds from Coinbase on first run,
        # cached to ~/.btc_vol_profile.json for 24h after)
        global _vol_profile
        profile_coins = {
            coin: cfg["cb_product"]
            for coin, cfg in COINS.items() if cfg["enabled"]
        }
        _vol_profile = await btc_vol_profile.load_or_build(
            profile_coins,
            log=lambda m: console.print(f"[dim]{m}[/dim]"),
        )

        # Start background feeds
        asyncio.create_task(_coinbase_price_feed())
        await _start_secondary_feeds()
        asyncio.create_task(_kalshi_ws_feed())
        asyncio.create_task(_status_loop())

        # Warm up crypto price feed
        console.print("[dim]Warming up price feeds (Coinbase WS + Kraken/Bitstamp REST)…[/dim]")
        for _ in range(20):
            if all(blended_price(s) for s in COINS if COINS[s]["enabled"]):
                break
            await asyncio.sleep(0.5)
        snap = "  ".join(
            f"{s}=${blended_price(s):,.2f}"
            for s in COINS if COINS[s]["enabled"] and blended_price(s)
        )
        console.print(f"[dim]Initial blended prices: {snap}[/dim]\n")

        # Start in the CURRENT in-progress window immediately (no wait for
        # next boundary). run_window will fetch historical open price if
        # we're already past the window start.
        window_num = 1
        while True:
            window_start_utc = _current_window_start_utc()
            try:
                await run_window(client, window_num, window_start_utc)
            except Exception as e:
                console.print(f"[red]window {window_num} crashed: {e}[/red]")
                await asyncio.sleep(2)
            window_num += 1


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
