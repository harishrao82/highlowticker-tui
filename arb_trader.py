#!/usr/bin/env python3
"""
arb_trader.py — Cross-coin correlated arb on Kalshi.

Watches BTC, ETH, XRP 15-min windows (SOL excluded).
When ALL coins point the same direction AND one pair has hi+lo < THRESHOLD:
  - Post maker resting limit on hi side at bid (1 - lo_ask - 1¢) → 0% fee
  - Post maker resting limit on lo side at bid (1 - hi_ask - 1¢) → 0% fee
  - 10 shares each leg

One active pair per window at a time. Orders rest until filled or window closes.

Run:   python arb_trader.py
Stop:  Ctrl-C  (open orders cancelled on exit)
"""
import asyncio
import base64
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()
console = Console()

# ── Config ────────────────────────────────────────────────────────────────────
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
ET_OFFSET   = timedelta(hours=-4)

SHARES    = 10      # contracts per leg (starting point, adjusts ±5 each window)
THRESHOLD = 0.85    # only enter when hi+lo < this
MAX_COST  = 0.90    # max combined limit price willing to post
FEE_RATE  = 0.07    # Kalshi 7% fee on profits at settlement
MIN_LO    = 0.05    # lo leg must have at least 5% probability — avoids 99%/1% situations

COINS: dict[str, str] = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "XRP": "KXXRP15M",
}

# ── Kalshi auth ───────────────────────────────────────────────────────────────
_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

def _kalshi_headers(method: str, path: str) -> dict:
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": _api_key, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "Content-Type": "application/json"}

def _ws_auth_headers() -> dict:
    ts  = str(round(time.time() * 1000))
    msg = ts + "GET" + "/trade-api/ws/v2"
    sig = _private_key.sign(msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": _api_key, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}

# ── Ticker helpers ────────────────────────────────────────────────────────────

def _window_start_ts() -> int:
    return (int(time.time()) // 900) * 900

def _window_start_et(offset: int = 0) -> datetime:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    w = now_et.replace(minute=mins, second=0, microsecond=0)
    return w + timedelta(minutes=15 * offset)

def _ticker_for(series: str, w: datetime) -> str:
    close = w + timedelta(minutes=15)
    return series + "-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")

# ── Shared state ──────────────────────────────────────────────────────────────
# per-coin: yes_ask, no_ask, ticker
prices: dict[str, dict] = {
    sym: {"yes_ask": 0.0, "no_ask": 0.0, "ticker": ""}
    for sym in COINS
}

_ticker_to_sym: dict[str, str] = {}
_current_window_ts: int = 0

# Active resting orders this window: list of {sym, ticker, side, order_id, limit_price}
_open_orders: list[dict] = []
# Pairs already traded this window (don't re-enter same pair)
_traded_pairs: set[str] = set()
# Prevents concurrent scan executions racing each other
_scan_lock = asyncio.Lock()
# Session-level fill tracking for P&L
_session_fills: list[dict] = []        # current window fills
_prev_arb_fills: list[dict] = []       # previous window fills (for share adjustment)
# Open legs for status display: sym → {side, entry_price}
_open_legs: dict[str, dict] = {}
# Adaptive share sizing
_current_shares: int = SHARES
MIN_SHARES: int      = 5
MAX_SHARES: int      = 200

# ── Order helpers ─────────────────────────────────────────────────────────────

async def _place_resting(
    client: httpx.AsyncClient,
    ticker: str,
    side: str,
    limit_price: float,
    shares: int,
    reason: str,
) -> str | None:
    """Post a resting limit order. Returns order_id or None."""
    price_field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
    limit_str   = f"{limit_price:.2f}"
    order = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            side,
        "count":           shares,
        "type":            "limit",
        price_field:       limit_str,
        "client_order_id": str(uuid.uuid4()),
    }
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_kalshi_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps(order),
            timeout=8,
        )
        if r.status_code not in (200, 201):
            console.print(f"  [yellow]Order failed {r.status_code}: {r.text[:120]}[/yellow]")
            return None
        o        = r.json().get("order", {})
        order_id = o.get("order_id", "")
        ts       = datetime.now().strftime("%H:%M:%S")
        console.print(
            f"  [cyan][{ts}] RESTING {side.upper():3} {shares}ct @ {limit_str}"
            f"  {ticker[-15:]}  [{reason}][/cyan]"
        )
        return order_id
    except Exception as e:
        console.print(f"  [yellow]Order error: {e}[/yellow]")
        return None


async def _sell_position(client: httpx.AsyncClient, ticker: str, side: str,
                          shares: int, reason: str) -> None:
    """Market-sell a filled position (limit at 0.01 = accept any bid)."""
    price_field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
    order = {
        "ticker":          ticker,
        "action":          "sell",
        "side":            side,
        "count":           shares,
        "type":            "limit",
        price_field:       "0.01",
        "client_order_id": str(uuid.uuid4()),
    }
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_kalshi_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps(order),
            timeout=8,
        )
        ts = datetime.now().strftime("%H:%M:%S")
        if r.status_code in (200, 201):
            console.print(f"  [magenta][{ts}] SELL {side.upper()} {shares}ct @ market  {ticker[-15:]}  [{reason}][/magenta]")
        else:
            console.print(f"  [yellow]Sell failed {r.status_code}: {r.text[:80]}[/yellow]")
    except Exception as e:
        console.print(f"  [yellow]Sell error: {e}[/yellow]")


async def _cancel_all(client: httpx.AsyncClient) -> None:
    """Cancel unfilled resting orders. Filled positions are left to expire at settlement."""
    for o in _open_orders:
        oid = o.get("order_id")
        if not oid:
            continue
        if o.get("logged_fill", 0) > 0:
            # Already filled — let it ride to settlement, just mark closed
            console.print(f"  [dim]Filled position {o['sym']} {o['side'].upper()} → expiring at settlement[/dim]")
            continue
        try:
            opath = f"/portfolio/orders/{oid}"
            await client.delete(
                f"{KALSHI_BASE}{opath}",
                headers=_kalshi_headers("DELETE", f"/trade-api/v2{opath}"),
                timeout=5,
            )
            console.print(f"  [dim]Cancelled unfilled {oid[:12]}…[/dim]")
        except Exception:
            pass
    _open_orders.clear()


def _pnl_summary() -> str:
    if not _session_fills:
        return ""
    cost      = sum(f["fill_price"] * f["shares"] for f in _session_fills)
    contracts = sum(f["shares"] for f in _session_fills)
    fee_if_win = sum(FEE_RATE * (1.0 - f["fill_price"]) * f["shares"] for f in _session_fills)
    net_if_win = contracts - cost - fee_if_win
    return (f"closed window: {len(_session_fills)} fills | "
            f"cost ${cost:.2f} | "
            f"projected if all win ${net_if_win:.2f} (fee est. ${fee_if_win:.2f})")


async def _check_fills(client: httpx.AsyncClient) -> None:
    """Log any newly filled orders."""
    new_fill = False
    for o in list(_open_orders):
        oid = o.get("order_id")
        if not oid:
            continue
        try:
            opath = f"/portfolio/orders/{oid}"
            r = await client.get(
                f"{KALSHI_BASE}{opath}",
                headers=_kalshi_headers("GET", f"/trade-api/v2{opath}"),
                timeout=5,
            )
            if r.status_code == 200:
                ord_data  = r.json().get("order", {})
                filled    = int(float(ord_data.get("fill_count_fp", 0) or 0))
                status    = ord_data.get("status", "")
                avg_price = float(ord_data.get("avg_fill_price_dollars", 0) or o["limit_price"])
                if filled > 0 and o.get("logged_fill") != filled:
                    new_fills = filled - (o.get("logged_fill") or 0)
                    o["logged_fill"] = filled
                    fee = FEE_RATE * (1.0 - avg_price) * new_fills
                    console.print(
                        f"  [green]FILLED {o['side'].upper()} {filled}ct "
                        f"@ {avg_price:.2f}  fee if win ${fee:.3f}  {o['ticker'][-15:]}[/green]"
                    )
                    _session_fills.append({
                        "side": o["side"], "fill_price": avg_price,
                        "shares": new_fills, "ticker": o["ticker"],
                        "strategy": o.get("strategy", "arb"),
                    })
                    new_fill = True
                if status in ("canceled", "expired"):
                    _open_orders.remove(o)
        except Exception:
            pass
    if new_fill:
        console.print(f"  [dim]{_pnl_summary()}[/dim]")

async def _get_fill_count(client: httpx.AsyncClient, order_id: str) -> int:
    try:
        opath = f"/portfolio/orders/{order_id}"
        r = await client.get(f"{KALSHI_BASE}{opath}",
                             headers=_kalshi_headers("GET", f"/trade-api/v2{opath}"), timeout=5)
        if r.status_code == 200:
            return int(float(r.json().get("order", {}).get("fill_count_fp", 0) or 0))
    except Exception:
        pass
    return 0


async def _cancel_order(client: httpx.AsyncClient, order_id: str) -> None:
    try:
        opath = f"/portfolio/orders/{order_id}"
        await client.delete(f"{KALSHI_BASE}{opath}",
                            headers=_kalshi_headers("DELETE", f"/trade-api/v2{opath}"), timeout=5)
    except Exception:
        pass


async def _verify_pair_fills(
    client: httpx.AsyncClient,
    oid_hi: str, ticker_x: str, hi_side: str,
    oid_lo: str, ticker_y: str, lo_side: str,
    pair_key: str,
) -> None:
    """5s after placing: if only one leg filled, market the other. If neither, cancel both."""
    await asyncio.sleep(5)
    hi_filled = await _get_fill_count(client, oid_hi) if oid_hi else 0
    lo_filled = await _get_fill_count(client, oid_lo) if oid_lo else 0

    if hi_filled > 0 and lo_filled > 0:
        return  # both filled — all good

    if hi_filled > 0 and lo_filled == 0:
        # Cancel unfilled lo, sell the filled hi — don't hold a naked position
        console.print(f"  [yellow]Pair {pair_key}: hi filled, lo not → cancelling lo + selling hi[/yellow]")
        await _cancel_order(client, oid_lo)
        _open_orders[:] = [o for o in _open_orders if o.get("order_id") != oid_lo]
        await _sell_position(client, ticker_x, hi_side, hi_filled, "hi close")

    elif lo_filled > 0 and hi_filled == 0:
        # Cancel unfilled hi, sell the filled lo — don't hold a naked position
        console.print(f"  [yellow]Pair {pair_key}: lo filled, hi not → cancelling hi + selling lo[/yellow]")
        await _cancel_order(client, oid_hi)
        _open_orders[:] = [o for o in _open_orders if o.get("order_id") != oid_hi]
        await _sell_position(client, ticker_y, lo_side, lo_filled, "lo close")

    else:
        # Neither filled — cancel both and allow re-entry
        console.print(f"  [yellow]Pair {pair_key}: neither leg filled after 5s → cancelling both[/yellow]")
        await _cancel_order(client, oid_hi)
        await _cancel_order(client, oid_lo)
        _open_orders[:] = [o for o in _open_orders if o.get("order_id") not in (oid_hi, oid_lo)]
        _traded_pairs.discard(pair_key)


async def _adjust_shares(client: httpx.AsyncClient) -> None:
    """After window close, fetch results for prev fills and adjust _current_shares ±5."""
    global _current_shares
    if not _prev_arb_fills:
        return
    # Give Kalshi ~15s to post settlement results before polling
    await asyncio.sleep(15)
    results: dict[str, str] = {}
    for ticker in {f["ticker"] for f in _prev_arb_fills}:
        try:
            r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                                 headers=_kalshi_headers("GET", f"/trade-api/v2/markets/{ticker}"), timeout=5)
            if r.status_code == 200:
                results[ticker] = r.json().get("market", {}).get("result", "")
        except Exception:
            pass
    net = 0.0
    for f in _prev_arb_fills:
        res = results.get(f["ticker"], "")
        if not res:
            continue
        if res == f["side"]:
            net += (1.0 - f["fill_price"]) * f["shares"] * (1 - FEE_RATE)
        else:
            net -= f["fill_price"] * f["shares"]
    if net > 0:
        _current_shares = min(_current_shares + 5, MAX_SHARES)
        console.print(f"  [green]Settled (2 windows ago) +${net:.2f} → shares +5 = {_current_shares}[/green]")
    elif net < 0:
        _current_shares = max(_current_shares - 5, MIN_SHARES)
        console.print(f"  [red]Settled (2 windows ago) -${abs(net):.2f} → shares -5 = {_current_shares}[/red]")
    else:
        console.print(f"  [dim]Settled (2 windows ago) breakeven → shares unchanged = {_current_shares}[/dim]")


# ── Arb logic ─────────────────────────────────────────────────────────────────

def _high_low(sym: str) -> tuple[float, float, str]:
    """Returns (high_ask, low_ask, high_side) for a coin."""
    ya = prices[sym]["yes_ask"]
    na = prices[sym]["no_ask"]
    if ya >= na:
        return ya, na, "yes"
    return na, ya, "no"



async def _scan_and_trade(client: httpx.AsyncClient) -> None:
    """Check alignment, find cheapest pair, place orders if conditions met."""
    if _scan_lock.locked():
        return   # another scan is already running — skip this tick
    async with _scan_lock:
        await _do_scan_and_trade(client)


async def _do_scan_and_trade(client: httpx.AsyncClient) -> None:
    # Wait 30s for window to settle and previous orders to clear
    if time.time() - _window_start_ts() < 30:
        return
    # Need all coins to have live prices (both sides)
    if any(prices[s]["yes_ask"] <= 0 or prices[s]["no_ask"] <= 0 for s in COINS):
        return

    highs = {s: _high_low(s) for s in COINS}

    # Alignment check: all coins must point same direction
    sides = {highs[s][2] for s in COINS}
    if len(sides) != 1:
        return   # mixed — skip
    dominant_side = sides.pop()   # "yes" or "no"


    # Find all pairs where hi+lo < THRESHOLD, not already traded this window
    candidates = []
    syms = list(COINS.keys())
    for i, a in enumerate(syms):
        for b in syms[i+1:]:
            for x, y in [(a, b), (b, a)]:
                hi_val = highs[x][0]
                lo_val = highs[y][1]
                cost   = hi_val + lo_val
                if cost < THRESHOLD and lo_val >= MIN_LO:
                    pair_key = f"{x}>{y}"
                    if pair_key not in _traded_pairs:
                        candidates.append((cost, x, y, hi_val, lo_val, pair_key))

    if not candidates:
        return

    # Pick cheapest valid pair
    candidates.sort(key=lambda c: c[0])
    cost, x, y, hi_val, lo_val, pair_key = candidates[0]

    # hi leg: 1¢ above ask to get filled
    # lo leg: whatever budget remains under MAX_COST, capped at lo_val + 1¢
    hi_limit = round(hi_val + 0.01, 2)
    lo_limit = min(round(MAX_COST - hi_limit - 0.01, 2), round(lo_val + 0.01, 2))

    if lo_limit <= 0:
        return
    if hi_limit + lo_limit > MAX_COST:
        return

    # hi side buys the expensive direction of coin X
    # lo side buys the CHEAP (opposite) direction of coin Y
    # e.g. all-YES: buy XRP YES (hi) + buy BTC NO (lo, since lo_val = no_ask)
    hi_side = highs[x][2]
    lo_side = "no" if highs[y][2] == "yes" else "yes"

    w = _window_start_et(0)
    ticker_x = _ticker_for(COINS[x], w)
    ticker_y = _ticker_for(COINS[y], w)

    now = datetime.now().strftime("%H:%M:%S")
    cost_max = hi_limit + lo_limit
    shares = _current_shares
    profit_if_hi_wins  = round(shares * (1.0 - hi_limit) * (1 - FEE_RATE) - shares * lo_limit, 2)
    profit_if_lo_wins  = round(shares * (1.0 - lo_limit) * (1 - FEE_RATE) - shares * hi_limit, 2)
    loss_if_both_lose  = round(-shares * cost_max, 2)

    console.print(
        f"\n[bold green]━━ ARB ENTRY ━━[/bold green]  {now}  "
        f"all-{dominant_side.upper()}  "
        f"[dim]hi+lo={cost:.2f} < {THRESHOLD}[/dim]"
    )
    console.print(
        f"  {x} {hi_side.upper()} hi={hi_val:.2f} → limit {hi_limit:.2f}  |  "
        f"  {y} {lo_side.upper()} lo={lo_val:.2f} → limit {lo_limit:.2f}  |  "
        f"combined max={cost_max:.2f}  shares={shares}"
    )
    # Scenario cheatsheet
    console.print(
        f"  [green]WANT[/green]  {x}→{hi_side.upper()} = +${profit_if_hi_wins:.2f}  "
        f"OR  {y}→{lo_side.upper()} = +${profit_if_lo_wins:.2f}  "
        f"OR both = jackpot"
    )
    console.print(
        f"  [red]FEAR[/red]  {x}→{'no' if hi_side=='yes' else 'yes'} AND "
        f"{y}→{'no' if lo_side=='yes' else 'yes'} = ${loss_if_both_lose:.2f}"
    )

    # Lock in the pair BEFORE any awaits — prevents race on concurrent scans
    _traded_pairs.add(pair_key)

    # Place both legs
    oid_hi = await _place_resting(client, ticker_x, hi_side, hi_limit, shares, f"{x} hi")
    oid_lo = await _place_resting(client, ticker_y, lo_side, lo_limit, shares, f"{y} lo")

    if oid_hi:
        _open_orders.append({"sym": x, "ticker": ticker_x, "side": hi_side,
                              "order_id": oid_hi, "limit_price": hi_limit,
                              "logged_fill": 0, "strategy": "arb"})
        _open_legs[x] = {"side": hi_side, "entry": hi_limit}
    if oid_lo:
        _open_orders.append({"sym": y, "ticker": ticker_y, "side": lo_side,
                              "order_id": oid_lo, "limit_price": lo_limit,
                              "logged_fill": 0, "strategy": "arb"})
        _open_legs[y] = {"side": lo_side, "entry": lo_limit}

    # Resting orders — let them sit, no fill verification needed

# ── Kalshi WS ─────────────────────────────────────────────────────────────────

async def _kalshi_ws_feed(client: httpx.AsyncClient) -> None:
    _msg_id   = 0
    subscribed: set[str] = set()

    def _next_id() -> int:
        nonlocal _msg_id
        _msg_id += 1
        return _msg_id

    while True:
        try:
            async with websockets.connect(
                KALSHI_WS, additional_headers=_ws_auth_headers(),
                ping_interval=20, open_timeout=10,
            ) as ws:
                console.print("[dim]Kalshi WS connected[/dim]")
                subscribed.clear()

                async def _subscribe() -> None:
                    tickers = []
                    for sym, series in COINS.items():
                        for offset in [0, 1]:
                            t = _ticker_for(series, _window_start_et(offset))
                            if t not in subscribed:
                                tickers.append(t)
                                subscribed.add(t)
                                _ticker_to_sym[t] = sym
                                prices[sym]["ticker"] = _ticker_for(series, _window_start_et(0))
                    if tickers:
                        await ws.send(json.dumps({
                            "id": _next_id(), "cmd": "subscribe",
                            "params": {"channels": ["ticker"], "market_tickers": tickers},
                        }))

                await _subscribe()
                last_resub = time.time()

                async for raw in ws:
                    msg   = json.loads(raw)
                    mtype = msg.get("type", "")
                    if mtype == "ticker":
                        data = msg.get("msg", msg)
                        t    = data.get("market_ticker", "")
                        ya   = float(data.get("yes_ask_dollars", 0) or 0)
                        yb   = float(data.get("yes_bid_dollars", 0) or 0)
                        na   = round(1.0 - yb, 4) if yb > 0 else 0.0
                        sym  = _ticker_to_sym.get(t)
                        if sym and ya > 0:
                            prices[sym]["yes_ask"] = ya
                            prices[sym]["no_ask"]  = na
                            # Scan on every price update — only after 3 PM ET
                            elapsed  = time.time() - _window_start_ts()
                            now_et   = datetime.now(timezone.utc) + ET_OFFSET
                            after3pm = now_et.hour >= 15
                            if elapsed <= 870 and after3pm:
                                asyncio.ensure_future(_scan_and_trade(client))

                    if time.time() - last_resub > 30:
                        await _subscribe()
                        last_resub = time.time()

        except Exception as e:
            console.print(f"[yellow]Kalshi WS: {e} — reconnecting[/yellow]")
            await asyncio.sleep(3)

# ── Management loop (window rolls, fill checks, status) ───────────────────────

async def _manage_loop(client: httpx.AsyncClient) -> None:
    global _current_window_ts

    _current_window_ts = _window_start_ts()
    console.print(f"[dim]Window: {_window_start_et().strftime('%H:%M')} ET[/dim]")
    last_status = -1

    while True:
        await asyncio.sleep(5)

        now       = time.time()
        window_ts = _window_start_ts()
        elapsed   = int(now - window_ts)

        # Window rolled
        if window_ts != _current_window_ts:
            console.print(
                f"\n[bold cyan]━━ NEW WINDOW ━━[/bold cyan]  "
                f"{_window_start_et().strftime('%H:%M')} ET"
            )
            await _cancel_all(client)
            _traded_pairs.clear()
            _open_legs.clear()
            _current_window_ts = window_ts
            # Show window summary, adjust shares, then reset
            if _session_fills:
                console.print(f"  [dim]{_pnl_summary()}[/dim]")
            _prev_arb_fills.clear()
            _prev_arb_fills.extend(_session_fills)
            _session_fills.clear()
            await _adjust_shares(client)
            # Portfolio balance (cash + open position value = total equity)
            try:
                r = await client.get(
                    f"{KALSHI_BASE}/portfolio/balance",
                    headers=_kalshi_headers("GET", "/trade-api/v2/portfolio/balance"),
                    timeout=5,
                )
                if r.status_code == 200:
                    d = r.json()
                    total = (d.get("balance", 0) + d.get("portfolio_value", 0)) / 100
                    console.print(f"  [dim]Portfolio: ${total:.2f}[/dim]")
            except Exception:
                pass

        await _check_fills(client)

        # Status every 30s
        bucket = elapsed // 30
        if bucket != last_status:
            last_status = bucket
            parts = []
            for sym in COINS:
                ya = prices[sym]["yes_ask"]
                na = prices[sym]["no_ask"]
                if ya > 0:
                    hi, lo, side = _high_low(sym)
                    parts.append(f"[dim]{sym}[/dim] Y={ya:.2f} N={na:.2f} hi={hi:.2f}({side[0].upper()})")
            sides = {_high_low(s)[2] for s in COINS if prices[s]["yes_ask"] > 0}
            aligned = "✓" if len(sides) == 1 else "✗"
            console.print(
                f"t={elapsed:>3}s  aligned={aligned}  [dim]shares={_current_shares}[/dim]  "
                + "   ".join(parts)
            )
            # Open leg tracker
            if _open_legs:
                leg_parts = []
                for sym, leg in _open_legs.items():
                    side = leg["side"]
                    entry = leg["entry"]
                    now_p = prices[sym]["yes_ask"] if side == "yes" else prices[sym]["no_ask"]
                    if now_p > 0:
                        arrow = "▲" if now_p > entry else "▼"
                        color = "green" if now_p > entry else "red"
                        leg_parts.append(
                            f"{sym} {side.upper()}  entry={entry:.2f} now=[{color}]{now_p:.2f}{arrow}[/{color}]"
                        )
                if leg_parts:
                    console.print("  legs: " + "   ".join(leg_parts))


# ── Idolized-Scallops shadow monitor ─────────────────────────────────────────
# Polls their Polymarket 15-min BTC/ETH/XRP positions every 60s.
# Maps net position → equivalent Kalshi YES/NO side and prints a dummy limit order.
# NO real orders are placed.

SCALLOPS_WALLET  = "0xe1d6b51521bd4365769199f392f9818661bd907c"
POLY_POSITIONS   = "https://data-api.polymarket.com/positions"
POLY_COIN_SLUGS  = {"BTC": "btc-updown-15m", "ETH": "eth-updown-15m", "XRP": "xrp-updown-15m", "SOL": "sol-updown-15m"}
# Kalshi series for shadow ticker mapping (SOL included for display even though we don't trade it)
POLY_KALSHI_SERIES = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "XRP": "KXXRP15M", "SOL": "KXSOL15M"}
SHADOW_FRACTION  = 0.10   # follow at 1/10th of their net position size

# Tracks how many shadow shares we've already "matched" per (coin, slug, side)
# key: (coin, slug)  value: {"side": "YES"|"NO", "matched": int, "ticker": str}
_scallops_matched: dict[tuple, dict] = {}

# Hypothetical fills: list of {ticker, side, shares, price, coin, settled}
_scallops_hypo_fills: list[dict] = []
# Running session P&L across all settled windows
_scallops_session_pnl: float = 0.0

def _poly_slug_to_kalshi_ticker(slug: str, coin: str) -> str | None:
    """btc-updown-15m-1775614500 → KXBTC15M-26APR071945-45"""
    try:
        ts     = int(slug.split("-")[-1])
        close  = datetime.fromtimestamp(ts + 900, tz=timezone.utc) + ET_OFFSET
        series = POLY_KALSHI_SERIES.get(coin, "")
        if not series:
            return None
        return series + "-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")
    except Exception:
        return None


async def _place_market(
    client: httpx.AsyncClient,
    ticker: str,
    side: str,
    shares: int,
    reason: str,
) -> str | None:
    """Place a market order (limit at 0.99 to guarantee fill). Returns order_id or None."""
    price_field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
    order = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            side,
        "count":           shares,
        "type":            "limit",
        price_field:       "0.99",
        "client_order_id": str(uuid.uuid4()),
    }
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_kalshi_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps(order),
            timeout=8,
        )
        if r.status_code not in (200, 201):
            console.print(f"  [yellow]Scallops order failed {r.status_code}: {r.text[:120]}[/yellow]")
            return None
        o        = r.json().get("order", {})
        order_id = o.get("order_id", "")
        ts       = datetime.now().strftime("%H:%M:%S")
        console.print(
            f"  [magenta][{ts}] SCALLOPS MARKET {side.upper()} {shares}ct  "
            f"{ticker[-15:]}  [{reason}][/magenta]"
        )
        return order_id
    except Exception as e:
        console.print(f"  [yellow]Scallops order error: {e}[/yellow]")
        return None


async def _scallops_place_and_record(
    client: httpx.AsyncClient,
    ticker: str, coin: str, side: str, shares: int, ask: float, reason: str,
) -> None:
    """Place a real market order and record it for P&L tracking."""
    if shares <= 0 or not ticker:
        return
    cost = round(ask * shares, 2) if ask > 0 else 0
    console.print(
        f"  [dim magenta]est. cost: {shares}ct × {ask:.2f} = ${cost:.2f}  "
        f"(win=${shares*(1-ask):.2f}  lose=-${cost:.2f})[/dim magenta]"
    )
    oid = await _place_market(client, ticker, side.lower(), shares, reason)
    if oid:
        _scallops_hypo_fills.append({
            "ticker": ticker, "coin": coin, "side": side,
            "shares": shares, "price": ask, "settled": False,
            "order_id": oid,
        })


async def _scallops_settle(client: httpx.AsyncClient) -> None:
    """Check Kalshi results for any unsettled hypo fills and update running P&L."""
    global _scallops_session_pnl
    unsettled = [f for f in _scallops_hypo_fills if not f["settled"]]
    if not unsettled:
        return
    tickers = {f["ticker"] for f in unsettled}
    results: dict[str, str] = {}
    for ticker in tickers:
        try:
            r = await client.get(
                f"{KALSHI_BASE}/markets/{ticker}",
                headers=_kalshi_headers("GET", f"/trade-api/v2/markets/{ticker}"),
                timeout=5,
            )
            if r.status_code == 200:
                results[ticker] = r.json().get("market", {}).get("result", "")
        except Exception:
            pass

    settled_lines = []
    window_pnl = 0.0
    for f in unsettled:
        result = results.get(f["ticker"], "")
        if not result:
            continue
        f["settled"] = True
        side_key = "yes" if f["side"] == "YES" else "no"
        won = (result == side_key)
        if won:
            net = round((1.0 - f["price"]) * f["shares"], 2)
            window_pnl += net
            settled_lines.append(
                f"  {f['coin']} {f['side']} {f['shares']}ct @ {f['price']:.2f}  "
                f"[green]WIN  +${net:.2f}[/green]"
            )
        else:
            net = round(-f["price"] * f["shares"], 2)
            window_pnl += net
            settled_lines.append(
                f"  {f['coin']} {f['side']} {f['shares']}ct @ {f['price']:.2f}  "
                f"[red]LOSS -${abs(net):.2f}[/red]"
            )

    if settled_lines:
        _scallops_session_pnl += window_pnl
        color = "green" if window_pnl >= 0 else "red"
        total_color = "green" if _scallops_session_pnl >= 0 else "red"
        console.print(f"\n[bold magenta]━━ SCALLOPS SETTLE ━━[/bold magenta]")
        for ln in settled_lines:
            console.print(ln)
        console.print(
            f"  window [{color}]{window_pnl:+.2f}[/{color}]  "
            f"session [{total_color}]{_scallops_session_pnl:+.2f}[/{total_color}]"
        )


async def _scallops_loop(client: httpx.AsyncClient) -> None:
    """
    Poll Idolized-Scallops Polymarket positions every 60s.
    Tracks cumulative net position per window, records hypothetical fills,
    and settles P&L when windows close. No real orders placed.
    """
    global _scallops_session_pnl
    await asyncio.sleep(10)   # let WS settle first
    while True:
        try:
            r = await client.get(
                POLY_POSITIONS,
                params={"user": SCALLOPS_WALLET, "limit": 50},
                timeout=10,
            )
            if r.status_code != 200:
                await asyncio.sleep(60)
                continue

            positions = r.json() if isinstance(r.json(), list) else []

            # Only care about current and next Kalshi window
            now_ts  = _window_start_ts()          # current window start (unix)
            next_ts = now_ts + 900                 # next window start

            # Group by (coin, slug) → net Up and Down size (only active positions)
            groups: dict[tuple, dict] = {}
            active_keys: set[tuple] = set()
            for p in positions:
                slug    = p.get("slug", "")
                outcome = p.get("outcome", "").lower()
                size    = float(p.get("size") or 0)
                cur     = float(p.get("curPrice") or 0)
                if cur <= 0:   # settled/worthless — skip
                    continue
                for coin, prefix in POLY_COIN_SLUGS.items():
                    if slug.startswith(prefix + "-"):
                        # Extract the window start timestamp from the slug
                        try:
                            slug_ts = int(slug.split("-")[-1])
                        except ValueError:
                            break
                        # Skip if this window has already closed on Kalshi
                        if slug_ts < now_ts:
                            break
                        # Skip anything beyond next window
                        if slug_ts > next_ts:
                            break
                        key = (coin, slug)
                        active_keys.add(key)
                        if key not in groups:
                            groups[key] = {"up": 0.0, "down": 0.0}
                        groups[key][outcome] += size
                        break

            # Settle any hypo fills for windows that just disappeared (curPrice=0 / gone)
            await _scallops_settle(client)

            # Drop matched state for windows no longer active
            for gone in set(_scallops_matched) - active_keys:
                del _scallops_matched[gone]

            if not groups:
                await asyncio.sleep(60)
                continue

            new_lines   = []
            delta_lines = []

            for (coin, slug), sides in sorted(groups.items()):
                net_up   = sides["up"] - sides["down"]
                net_side = "up" if net_up >= 0 else "down"
                net_size = abs(net_up)
                if net_size < 10:
                    continue

                kalshi_side   = "YES" if net_side == "up" else "NO"
                kalshi_ticker = _poly_slug_to_kalshi_ticker(slug, coin)
                target_shadow = max(1, round(net_size * SHADOW_FRACTION))

                sym_price = prices.get(coin, {})
                ask = sym_price.get("yes_ask" if kalshi_side == "YES" else "no_ask", 0)
                ask_str = f"@ {ask:.2f}" if ask > 0 else "@ n/a"

                key = (coin, slug)
                prev = _scallops_matched.get(key)

                if prev is None:
                    _scallops_matched[key] = {"side": kalshi_side, "matched": target_shadow, "ticker": kalshi_ticker}
                    await _scallops_place_and_record(client, kalshi_ticker or slug, coin, kalshi_side, target_shadow, ask, f"{coin} scallops open")
                    new_lines.append(
                        f"  [bold]{coin}[/bold] {kalshi_side}  "
                        f"poly_net={net_size:.0f}ct  "
                        f"[green]OPEN shadow={target_shadow}ct {ask_str}[/green]  "
                        f"[dim]{kalshi_ticker or slug}[/dim]"
                    )
                elif prev["side"] != kalshi_side:
                    _scallops_matched[key] = {"side": kalshi_side, "matched": target_shadow, "ticker": kalshi_ticker}
                    await _scallops_place_and_record(client, kalshi_ticker or slug, coin, kalshi_side, target_shadow, ask, f"{coin} scallops flip")
                    delta_lines.append(
                        f"  [bold]{coin}[/bold] {kalshi_side}  "
                        f"poly_net={net_size:.0f}ct  "
                        f"[yellow]FLIP → shadow={target_shadow}ct {ask_str}[/yellow]  "
                        f"[dim]{kalshi_ticker or slug}[/dim]"
                    )
                else:
                    already  = prev["matched"]
                    delta_sh = target_shadow - already
                    if delta_sh >= 1:
                        _scallops_matched[key]["matched"] = target_shadow
                        await _scallops_place_and_record(client, kalshi_ticker or slug, coin, kalshi_side, delta_sh, ask, f"{coin} scallops add")
                        delta_lines.append(
                            f"  [bold]{coin}[/bold] {kalshi_side}  "
                            f"poly_net={net_size:.0f}ct  "
                            f"[cyan]+{delta_sh}ct (total={target_shadow}, was {already}) {ask_str}[/cyan]  "
                            f"[dim]{kalshi_ticker or slug}[/dim]"
                        )

            if new_lines or delta_lines:
                now = datetime.now().strftime("%H:%M:%S")
                total_color = "green" if _scallops_session_pnl >= 0 else "red"
                console.print(
                    f"\n[bold magenta]━━ SCALLOPS {now}  "
                    f"session=[{total_color}]{_scallops_session_pnl:+.2f}[/{total_color}] ━━[/bold magenta]"
                )
                for ln in new_lines + delta_lines:
                    console.print(ln)

        except Exception as e:
            console.print(f"[dim]scallops poll: {e}[/dim]")

        await asyncio.sleep(60)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]Arb Trader — BTC / ETH / XRP correlated pairs[/bold cyan]")
    console.print(f"THRESHOLD={THRESHOLD}  MAX_COST={MAX_COST}  SHARES={SHARES}\n")

    async with httpx.AsyncClient() as client:
        try:
            await asyncio.gather(
                _kalshi_ws_feed(client),
                _manage_loop(client),
                _scallops_loop(client),
            )
        except asyncio.CancelledError:
            pass
        finally:
            console.print("\n[dim]Cancelling open orders…[/dim]")
            await _cancel_all(client)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
