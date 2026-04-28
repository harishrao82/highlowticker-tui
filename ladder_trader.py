#!/usr/bin/env python3
"""
ladder_trader.py — momentum ladder with patient-fill exit.

Uses the same signal as kalshi_momentum_live.py (delta crosses vol-scaled
threshold), but manages position differently:

  1. Signal fires → buy 1 market + 1 limit (ask-2¢)
  2. Wait 5s:
     - Limit unfilled → momentum confirmed → cancel limit, add 1 more round
     - Limit filled → momentum fading → SELL all shares, done for this window
  3. Repeat until MAX_SHARES_PER_COIN or window ends
  4. If still holding at window end, let it settle (no sell)

Safety:
  - Hard cap: MAX_SHARES_PER_COIN per coin per window (default 20)
  - All open orders cancelled at window end
  - Every order ID tracked

Run:   python ladder_trader.py
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COINS = {
    "BTC": dict(enabled=True, series="KXBTC15M", cb_product="BTC-USD",
                kraken="BTC/USD", bitstamp="BTC/USD"),
    "ETH": dict(enabled=True, series="KXETH15M", cb_product="ETH-USD",
                kraken="ETH/USD", bitstamp="ETH/USD"),
    "SOL": dict(enabled=True, series="KXSOL15M", cb_product="SOL-USD",
                kraken="SOL/USD", bitstamp=None),
    "XRP": dict(enabled=True, series="KXXRP15M", cb_product="XRP-USD",
                kraken="XRP/USD", bitstamp="XRP/USD"),
}

MAX_SHARES_PER_COIN = int(os.environ.get("LADDER_MAX_COIN", "20"))
MAX_SHARES_TOTAL    = int(os.environ.get("LADDER_MAX_TOTAL", "40"))
LADDER_WAIT_SEC     = int(os.environ.get("LADDER_WAIT", "5"))
LADDER_OFFSET     = 0.02    # patient limit at ask - offset
LADDER_BUFFER     = 0.02    # market order at ask + buffer
SELL_BUFFER       = 0.02    # sell at bid - buffer for guaranteed fill
MIN_ENTRY_PRICE   = 0.45
MAX_ENTRY_PRICE   = 0.90
COOLDOWN_SEC      = 180

# Same checkpoints as the main trader
CHECKPOINTS = [
    ( 30, 0.5, 0.53),
    ( 60, 0.7, 0.59),
    (120, 1.0, 0.68),
    (180, 1.0, 0.71),
    (300, 1.2, 0.79),
    (420, 1.5, 0.85),
    (600, 1.5, 0.88),
]
COIN_CAP_ADJUSTMENT = {"BTC": -0.15, "ETH": 0.00, "SOL": -0.10, "XRP": -0.10}
VOL_THR_FLOOR   = 0.015
VOL_THR_CEILING = 0.25
VOL_LOOKBACK_MIN = 60
MAX_EVAL_GAP_SEC = 60

TRADE_LOG = Path.home() / ".ladder_trades.jsonl"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
ET_OFFSET   = timedelta(hours=-4)

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)


# ── Auth ─────────────────────────────────────────────────
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


# ── Timing ───────────────────────────────────────────────
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


# ── Multi-exchange prices (same as main trader) ─────────
exchange_prices: dict[str, dict[str, float]] = {
    "coinbase": {}, "kraken": {}, "bitstamp": {},
}

def blended_price(sym: str) -> float | None:
    pxs = [src.get(sym) for src in exchange_prices.values() if src.get(sym)]
    if not pxs:
        return None
    pxs.sort()
    return pxs[len(pxs) // 2] if len(pxs) >= 2 else pxs[0]

_cb_key = os.environ.get("COINBASE_API_KEY_USERNAME", "")
_cb_secret = os.environ.get("COINBASE_API_PRIVATE_KEY", "")

async def _coinbase_price_feed() -> None:
    products = [c["cb_product"] for c in COINS.values() if c["enabled"]]
    product_to_coin = {c["cb_product"]: coin
                       for coin, c in COINS.items() if c["enabled"]}
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
                    for evt in msg.get("events", []):
                        for t in evt.get("tickers", []):
                            pid = t.get("product_id", "")
                            px  = float(t.get("price", 0) or 0)
                            sym = product_to_coin.get(pid)
                            if sym and px > 0:
                                exchange_prices["coinbase"][sym] = px
        except Exception as e:
            console.print(f"[yellow]Coinbase WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)

async def _ccxt_poll_loop() -> None:
    kr = ccxt.kraken()
    bs = ccxt.bitstamp()
    while True:
        for sym, cfg in COINS.items():
            if not cfg["enabled"]:
                continue
            try:
                t = kr.fetch_ticker(cfg["kraken"])
                if t and t.get("last"):
                    exchange_prices["kraken"][sym] = float(t["last"])
            except Exception:
                pass
            if cfg.get("bitstamp"):
                try:
                    t = bs.fetch_ticker(cfg["bitstamp"])
                    if t and t.get("last"):
                        exchange_prices["bitstamp"][sym] = float(t["last"])
                except Exception:
                    pass
        await asyncio.sleep(5)


# ── Order helpers ────────────────────────────────────────
async def _place_order(client: httpx.AsyncClient, ticker: str,
                       action: str, side: str, price: float,
                       shares: int, tag: str) -> dict | None:
    if shares <= 0:
        return None
    yes_price = f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"
    coid = str(uuid.uuid4())
    try:
        sent_at = time.time()
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps({
                "ticker":            ticker,
                "action":            action,
                "side":              side,
                "count":             shares,
                "type":              "limit",
                "yes_price_dollars": yes_price,
                "client_order_id":   coid,
            }),
            timeout=10,
        )
        ack_at = time.time()
        if r.status_code in (200, 201):
            order = r.json().get("order", {}) or {}
            oid   = order.get("order_id", "")
            rtt   = round((ack_at - sent_at) * 1000)
            verb = "BUY" if action == "buy" else "SELL"
            console.print(f"  [bold green]{tag}  {verb} {side.upper()} "
                          f"{shares}ct @ {price:.2f}  {oid[:8]}  "
                          f"[dim]{rtt}ms[/dim][/bold green]")
            return {"order_id": oid, "client_order_id": coid,
                    "sent_at": sent_at, "ack_at": ack_at}
        else:
            console.print(f"  [red]{tag}  failed {r.status_code}: "
                          f"{r.text[:150]}[/red]")
    except Exception as e:
        console.print(f"  [red]{tag}  error: {e}[/red]")
    return None


async def _cancel(client: httpx.AsyncClient, order_id: str,
                  tag: str = "") -> bool:
    if not order_id:
        return False
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    try:
        r = await client.request(
            "DELETE", f"{KALSHI_BASE}/portfolio/orders/{order_id}",
            headers=_headers("DELETE", path), timeout=8,
        )
        if r.status_code in (200, 202):
            console.print(f"  [dim]cancel {tag or order_id[:8]} ok[/dim]")
            return True
        if r.status_code == 404:
            return False
    except Exception:
        pass
    return False


async def _check_filled(client: httpx.AsyncClient,
                        order_id: str) -> tuple[bool, int]:
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    try:
        r = await client.get(
            f"{KALSHI_BASE}/portfolio/orders/{order_id}",
            headers=_headers("GET", path), timeout=5,
        )
        if r.status_code == 200:
            order = r.json().get("order", {})
            fc = int(float(order.get("fill_count_fp", 0) or 0))
            return (fc > 0, fc)
    except Exception:
        pass
    return (False, 0)


async def _get_asks(client: httpx.AsyncClient,
                    ticker: str) -> tuple[float, float]:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(
            f"{KALSHI_BASE}/markets/{ticker}",
            headers=_headers("GET", path), timeout=5,
        )
        if r.status_code == 200:
            m = r.json().get("market", {})
            ya = float(m.get("yes_ask_dollars") or 0)
            yb = float(m.get("yes_bid_dollars") or 0)
            na = 1.0 - yb if yb > 0 else 0
            return (ya, na)
    except Exception:
        pass
    return (0, 0)


def _log(entry: dict) -> None:
    try:
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


# ── Vol helpers ──────────────────────────────────────────
_vol_profile: dict = {}
_price_history: dict[str, list[tuple[float, float]]] = {}

def _realized_vol_pct(sym: str) -> float:
    h = _price_history.get(sym, [])
    cutoff = time.time() - VOL_LOOKBACK_MIN * 60
    recent = [(t, p) for t, p in h if t >= cutoff]
    if len(recent) < 10:
        return 0.0
    prices = [p for _, p in recent]
    log_rets = [math.log(prices[i] / prices[i - 1])
                for i in range(1, len(prices)) if prices[i - 1] > 0]
    return stdev(log_rets) * 100 if len(log_rets) >= 5 else 0.0


# ── Main window logic ───────────────────────────────────
async def main() -> None:
    console.print("[bold cyan]Ladder Trader — momentum entry, patient-fill exit[/bold cyan]")
    console.print(f"  max shares: {MAX_SHARES_PER_COIN}/coin, {MAX_SHARES_TOTAL}/window total")
    console.print(f"  ladder wait: {LADDER_WAIT_SEC}s between rounds")
    console.print(f"  exit trigger: patient limit fills")
    console.print(f"  trade log: {TRADE_LOG}")
    console.print()

    # Load vol profile
    global _vol_profile
    profile_coins = {c: cfg["cb_product"] for c, cfg in COINS.items() if cfg["enabled"]}
    _vol_profile = await btc_vol_profile.load_or_build(
        profile_coins,
        log=lambda m: console.print(f"[dim]{m}[/dim]"),
    )

    async with httpx.AsyncClient() as client:
        # Start price feeds
        asyncio.create_task(_coinbase_price_feed())
        asyncio.create_task(_ccxt_poll_loop())
        await asyncio.sleep(3)  # let feeds populate

        window_num = 0
        session_stats = {"entries": 0, "sells": 0, "held": 0}

        while True:
            wait = _seconds_until_next_window()
            if wait > 5:
                console.print(f"[dim]next window in {wait:.0f}s …[/dim]")
                await asyncio.sleep(max(wait - 3, 1))

            while _seconds_until_next_window() > 1:
                await asyncio.sleep(0.5)

            window_num += 1
            ws_start = _current_window_start_utc()
            close_utc = ws_start + timedelta(minutes=15)
            window_start_ts = ws_start.timestamp()
            window_et = (ws_start + ET_OFFSET).strftime("%H:%M ET")

            console.print(f"\n{'━'*60}")
            console.print(f"[bold]Window {window_num}  {window_et}[/bold]")

            # Build tickers
            tickers = {}
            for coin, cfg in COINS.items():
                if cfg["enabled"]:
                    tickers[coin] = _ticker_for(cfg["series"], close_utc)

            # Wait for Coinbase WS to deliver at least one tick
            for _ in range(10):
                if exchange_prices["coinbase"]:
                    break
                await asyncio.sleep(1)
            await asyncio.sleep(2)

            open_prices = {}
            for sym in tickers:
                bp = blended_price(sym)
                if bp:
                    open_prices[sym] = bp
                    _price_history.setdefault(sym, []).append((time.time(), bp))

            # Show which sources are live
            src_count = sum(1 for s in exchange_prices.values() if s)
            cb_ok = "✓" if exchange_prices["coinbase"] else "✗"
            console.print(f"  open: {', '.join(f'{s}=${p:,.2f}' for s,p in open_prices.items())}  "
                          f"[dim](cb={cb_ok} {src_count}/3 sources)[/dim]")

            # Compute vol per coin
            now_utc = datetime.now(timezone.utc)
            vol_by_coin = {}
            for sym in tickers:
                realized = _realized_vol_pct(sym)
                profile_slot = 0.0
                coin_profile = _vol_profile.get(sym, {})
                if coin_profile:
                    dow = now_utc.weekday()
                    hour = now_utc.hour
                    profile_slot = coin_profile.get(str(dow), {}).get(str(hour), 0.0)
                vol_by_coin[sym] = max(realized, profile_slot) if (realized or profile_slot) else 0.0

            # Per-coin state for this window
            coin_state = {}
            for sym in tickers:
                coin_state[sym] = {
                    "ticker": tickers[sym],
                    "open_price": open_prices.get(sym),
                    "side": None,
                    "shares_bought": 0,
                    "total_cost": 0.0,
                    "open_orders": set(),
                    "ladder_round": 0,
                    "done": False,
                    "last_fired": -999,
                    "sold": False,
                }

            # Shared counter across all coins this window
            window_total_shares = {"n": 0}

            # ── Event-driven evaluator + ladder ──────────
            async def _run_coin(sym: str) -> None:
                st = coin_state[sym]
                open_p = st["open_price"]
                if not open_p:
                    return

                while not st["done"]:
                    elapsed = int(time.time() - window_start_ts)
                    if elapsed > 780:
                        break

                    now_p = blended_price(sym)
                    if not now_p:
                        await asyncio.sleep(1)
                        continue

                    # Record price for vol calculation
                    _price_history.setdefault(sym, []).append((time.time(), now_p))

                    # If we're already in a position, this is handled
                    # by the ladder loop below. Here we just detect the
                    # initial entry signal.
                    if st["side"] is not None:
                        await asyncio.sleep(1)
                        continue

                    delta = (now_p - open_p) / open_p * 100.0
                    vol = vol_by_coin.get(sym, 0.0)

                    # Find best (most recent) checkpoint that has passed.
                    # No staleness guard here — the ladder only fires once
                    # per coin per window, so we want to catch any crossing.
                    best_cp = None
                    for T, factor, max_px in CHECKPOINTS:
                        if elapsed >= T:
                            best_cp = (T, factor, max_px)
                    if not best_cp:
                        await asyncio.sleep(1)
                        continue

                    T_eff, factor, max_px = best_cp
                    thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * vol)) if vol > 0 else VOL_THR_FLOOR

                    if abs(delta) < thr:
                        # Log every 30s so we can see what's happening
                        if elapsed % 30 == 0:
                            console.print(
                                f"  [dim]{sym}  T={elapsed}s  Δ={delta:+.3f}%  "
                                f"thr={thr:.3f}% (vol={vol:.3f}%×{factor})  "
                                f"gap={abs(delta)-thr:+.3f}%[/dim]")
                        await asyncio.sleep(1)
                        continue

                    side = "yes" if delta > 0 else "no"

                    # Cooldown
                    if elapsed - st["last_fired"] < COOLDOWN_SEC:
                        await asyncio.sleep(1)
                        continue

                    # Get current ask
                    ya, na = await _get_asks(client, st["ticker"])
                    cur_ask = ya if side == "yes" else na
                    if cur_ask <= 0 or cur_ask >= 1 or cur_ask < MIN_ENTRY_PRICE:
                        await asyncio.sleep(1)
                        continue

                    eff_cap = max_px + COIN_CAP_ADJUSTMENT.get(sym, 0.0)
                    if cur_ask > eff_cap or cur_ask > MAX_ENTRY_PRICE:
                        await asyncio.sleep(1)
                        continue

                    # ── Signal fires → start ladder ──────
                    st["side"] = side
                    st["last_fired"] = elapsed
                    console.print(
                        f"\n  [bold cyan]{sym}  T={elapsed}s  Δ={delta:+.3f}% ≥ {thr:.3f}%  "
                        f"→ LADDER {side.upper()}  ask={cur_ask:.2f}[/bold cyan]")

                    _log({"ts": time.time(), "action": "signal",
                          "coin": sym, "ticker": st["ticker"],
                          "elapsed": elapsed, "delta": round(delta, 4),
                          "threshold": round(thr, 4), "side": side,
                          "ask": cur_ask, "vol": round(vol, 4),
                          "window_start_ts": window_start_ts})

                    # ── Ladder loop ──────────────────────
                    while (not st["done"]
                           and st["shares_bought"] < MAX_SHARES_PER_COIN
                           and window_total_shares["n"] < MAX_SHARES_TOTAL):
                        elapsed = int(time.time() - window_start_ts)
                        if elapsed > 780:
                            break

                        st["ladder_round"] += 1
                        rnd = st["ladder_round"]

                        # Refresh ask
                        ya, na = await _get_asks(client, st["ticker"])
                        cur_ask = ya if side == "yes" else na
                        if cur_ask <= 0 or cur_ask >= 1:
                            await asyncio.sleep(LADDER_WAIT_SEC)
                            continue

                        mkt_px = round(min(cur_ask + LADDER_BUFFER, 0.95), 2)
                        lim_px = round(max(cur_ask - LADDER_OFFSET, 0.01), 2)

                        # Place market (1 share)
                        mkt_order = await _place_order(
                            client, st["ticker"], "buy", side, mkt_px, 1,
                            f"{sym}/R{rnd}/M")
                        if mkt_order:
                            st["open_orders"].add(mkt_order["order_id"])
                            st["shares_bought"] += 1
                            st["total_cost"] += cur_ask
                            window_total_shares["n"] += 1
                            session_stats["entries"] += 1
                            _log({"ts": time.time(), "action": "ladder_market",
                                  "coin": sym, "ticker": st["ticker"],
                                  "round": rnd, "price": mkt_px, "ask": cur_ask,
                                  "sent_at": mkt_order.get("sent_at"),
                                  "ack_at": mkt_order.get("ack_at"),
                                  "shares_held": st["shares_bought"],
                                  "elapsed": int(time.time() - window_start_ts),
                                  "window_start_ts": window_start_ts})

                        # Place limit probe (1 share) if room
                        lim_order = None
                        if (st["shares_bought"] < MAX_SHARES_PER_COIN
                                and window_total_shares["n"] < MAX_SHARES_TOTAL):
                            lim_order = await _place_order(
                                client, st["ticker"], "buy", side, lim_px, 1,
                                f"{sym}/R{rnd}/L")
                            if lim_order:
                                st["open_orders"].add(lim_order["order_id"])

                        # Poll every second to measure exact fill timing
                        lim_placed_at = time.time()
                        fill_gap = None
                        filled = False
                        if lim_order:
                            for tick in range(LADDER_WAIT_SEC):
                                await asyncio.sleep(1)
                                filled, _ = await _check_filled(client, lim_order["order_id"])
                                if filled:
                                    fill_gap = round(time.time() - lim_placed_at, 1)
                                    break
                        else:
                            await asyncio.sleep(LADDER_WAIT_SEC)

                        if lim_order and filled:
                            st["shares_bought"] += 1
                            st["total_cost"] += lim_px
                            window_total_shares["n"] += 1
                            st["open_orders"].discard(lim_order["order_id"])

                            # Instant fill (<2s) = deep book, strong signal → keep going
                            # Slow fill (≥2s) = momentum fading → sell
                            if fill_gap is not None and fill_gap < 2.0:
                                console.print(
                                    f"  [bold green]{sym}  R{rnd}  LIMIT FILLED "
                                    f"@ {lim_px:.2f} in {fill_gap}s — instant fill, "
                                    f"book is deep → continue  "
                                    f"({st['shares_bought']} shares)[/bold green]")
                                _log({"ts": time.time(), "action": "instant_fill_continue",
                                      "coin": sym, "ticker": st["ticker"],
                                      "round": rnd, "lim_price": lim_px,
                                      "fill_gap_sec": fill_gap,
                                      "shares_held": st["shares_bought"],
                                      "elapsed": int(time.time() - window_start_ts),
                                      "window_start_ts": window_start_ts})
                                continue  # keep laddering

                            console.print(
                                f"  [bold yellow]{sym}  R{rnd}  LIMIT FILLED "
                                f"@ {lim_px:.2f} in {fill_gap}s — momentum fading "
                                f"→ SELL ALL ({st['shares_bought']} shares)[/bold yellow]")

                            _log({"ts": time.time(), "action": "exit_signal",
                                  "coin": sym, "ticker": st["ticker"],
                                  "round": rnd, "lim_price": lim_px,
                                  "fill_gap_sec": fill_gap,
                                  "shares_held": st["shares_bought"],
                                  "elapsed": int(time.time() - window_start_ts),
                                  "window_start_ts": window_start_ts})

                            # Sell all shares
                            sell_ya, sell_na = await _get_asks(client, st["ticker"])
                            if side == "yes":
                                bid = 1.0 - sell_na if sell_na > 0 else 0
                            else:
                                bid = 1.0 - sell_ya if sell_ya > 0 else 0
                            sell_px = round(max(bid - SELL_BUFFER, 0.01), 2)

                            sell_order = await _place_order(
                                client, st["ticker"], "sell", side,
                                sell_px, st["shares_bought"],
                                f"{sym}/R{rnd}/SELL")
                            if sell_order:
                                st["open_orders"].add(sell_order["order_id"])
                                revenue = sell_px * st["shares_bought"]
                                pnl = revenue - st["total_cost"]
                                st["sold"] = True
                                session_stats["sells"] += 1
                                console.print(
                                    f"  [bold]{sym}  SOLD {st['shares_bought']}sh "
                                    f"@ {sell_px:.2f}  cost=${st['total_cost']:.2f}  "
                                    f"P&L ${pnl:+.2f}[/bold]")
                                _log({"ts": time.time(), "action": "sell",
                                      "coin": sym, "ticker": st["ticker"],
                                      "sell_price": sell_px,
                                      "shares": st["shares_bought"],
                                      "cost": round(st["total_cost"], 4),
                                      "revenue": round(revenue, 4),
                                      "pnl": round(pnl, 4),
                                      "rounds": rnd,
                                      "elapsed": int(time.time() - window_start_ts),
                                      "fill_gap_sec": fill_gap,
                                      "sent_at": sell_order.get("sent_at"),
                                      "ack_at": sell_order.get("ack_at"),
                                      "window_start_ts": window_start_ts})

                            st["done"] = True
                            break

                        elif lim_order and not filled:
                            # Momentum holding — cancel limit, continue
                            await _cancel(client, lim_order["order_id"],
                                          f"{sym}/R{rnd}/L")
                            st["open_orders"].discard(lim_order["order_id"])
                            console.print(
                                f"  [dim]{sym}  R{rnd}  momentum holding  "
                                f"{st['shares_bought']}sh  ask={cur_ask:.2f}[/dim]")

                    break  # exit the outer signal-detection loop

                # ── Window end cleanup ───────────────────
                if st["open_orders"]:
                    for oid in list(st["open_orders"]):
                        await _cancel(client, oid, f"{sym}/cleanup")

                if st["shares_bought"] > 0 and not st["sold"]:
                    session_stats["held"] += 1
                    console.print(
                        f"  [dim]{sym}  holding {st['shares_bought']}sh to settlement  "
                        f"cost=${st['total_cost']:.2f}[/dim]")
                    _log({"ts": time.time(), "action": "hold_to_settlement",
                          "coin": sym, "ticker": st["ticker"],
                          "shares": st["shares_bought"],
                          "cost": round(st["total_cost"], 4),
                          "rounds": st["ladder_round"],
                          "side": st["side"],
                          "window_start_ts": window_start_ts})

            # Run all coins in parallel
            tasks = [_run_coin(sym) for sym in tickers]
            await asyncio.gather(*tasks, return_exceptions=True)

            # Session summary
            s = session_stats
            console.print(
                f"\n  [bold]Session[/bold]  entries={s['entries']}  "
                f"sells={s['sells']}  held_to_settle={s['held']}")

            # Wait for window to close
            remaining = 900 - int(time.time() - window_start_ts)
            if remaining > 0:
                await asyncio.sleep(remaining + 5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold]stopped[/bold]")
