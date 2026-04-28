#!/usr/bin/env python3
"""
mm_maker.py — Market maker limit order script for Kalshi 15-min windows.

Strategy:
  1. At the next 15-min window open, place a resting limit BUY at BUY_PRICE.
  2. When filled, immediately place a resting limit SELL at SELL_PRICE.
  3. Both orders are limit (maker) → 0% fee if they rest on the book.
  4. Profit per contract = SELL_PRICE - BUY_PRICE.

Config:  edit the block below before running.
Run:     python mm_maker.py
Stop:    Ctrl-C  (cancels any open orders first)
"""
import asyncio
import base64
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()
console = Console()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG — edit before running
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COIN        = "BTC"           # BTC | ETH | SOL | XRP
SIDE        = "no"           # "yes" or "no"
BUY_PRICE   = 0.42            # limit buy price (cents as decimal, e.g. 0.42 = 42¢)
SELL_PRICE  = 0.58            # limit sell price after fill
SHARES      = 100              # contracts per order
RUN_WINDOWS = 0              # how many windows to run (set 0 for infinite)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
ET_OFFSET   = timedelta(hours=-4)

COINS = {
    "BTC": "KXBTC15M"
}

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

KALSHI_WS = "wss://api.elections.kalshi.com/trade-api/ws/v2"

def _ws_headers() -> dict:
    ts  = str(round(time.time() * 1000))
    msg = ts + "GET" + "/trade-api/ws/v2"
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
        "Content-Type":            "application/json",
    }

# ── Ticker helpers ────────────────────────────────────────────────────────────

def _window_start_et(offset: int = 0) -> datetime:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    w      = now_et.replace(minute=mins, second=0, microsecond=0)
    return w + timedelta(minutes=15 * offset)

def _ticker_for(series: str, w: datetime) -> str:
    close = w + timedelta(minutes=15)
    return series + "-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")

async def _fetch_next_ticker(client: httpx.AsyncClient) -> tuple[str, datetime] | tuple[None, None]:
    """
    Ask Kalshi which markets are open/upcoming for this series and return
    the one whose open_time is closest to the next 15-min boundary.
    Falls back to the locally-computed ticker if the API call fails.
    """
    series = COINS[COIN]
    try:
        r = await client.get(
            f"{KALSHI_BASE}/markets",
            headers=_headers("GET", f"/trade-api/v2/markets"),
            params={"series_ticker": series, "status": "open", "limit": 10},
            timeout=8,
        )
        if r.status_code == 200:
            markets = r.json().get("markets", [])
            now_ts  = datetime.now(timezone.utc).timestamp()
            # pick the market whose open_time is soonest in the future
            future  = [m for m in markets if m.get("open_time")]
            future.sort(key=lambda m: abs(
                datetime.fromisoformat(m["open_time"].replace("Z", "+00:00")).timestamp() - now_ts
            ))
            if future:
                m      = future[0]
                ticker = m["ticker"]
                open_ts= datetime.fromisoformat(m["open_time"].replace("Z", "+00:00"))
                open_et= open_ts + ET_OFFSET
                console.print(f"  [dim]Kalshi market: {ticker}  opens {open_et.strftime('%H:%M ET')}[/dim]")
                return ticker, open_et
    except Exception as e:
        console.print(f"  [yellow]Market lookup failed ({e}), falling back to local ticker[/yellow]")

    # Fallback: compute locally
    w = _window_start_et(offset=1)
    return _ticker_for(series, w), w


def _next_window_ticker() -> tuple[str, datetime]:
    """Returns (ticker, window_open_dt) for the NEXT window (local calc only)."""
    series = COINS[COIN]
    w      = _window_start_et(offset=1)
    return _ticker_for(series, w), w

def _seconds_until_next_window() -> float:
    now_et  = datetime.now(timezone.utc) + ET_OFFSET
    mins    = (now_et.minute // 15) * 15
    current = now_et.replace(minute=mins, second=0, microsecond=0)
    next_w  = current + timedelta(minutes=15)
    return (next_w - now_et).total_seconds()

# ── Order helpers ─────────────────────────────────────────────────────────────

def _yes_price(side: str, price: float) -> str:
    """Kalshi always wants yes_price_dollars. For NO orders, send the complement."""
    return f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"

async def _place_limit_buy(client: httpx.AsyncClient, ticker: str) -> str | None:
    order = {
        "ticker":            ticker,
        "action":            "buy",
        "side":              SIDE,
        "count":             SHARES,
        "type":              "limit",
        "yes_price_dollars": _yes_price(SIDE, BUY_PRICE),
        "client_order_id":   str(uuid.uuid4()),
    }
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps(order),
            timeout=8,
        )
        if r.status_code in (200, 201):
            oid = r.json().get("order", {}).get("order_id", "")
            console.print(
                f"  [cyan]BUY  {SIDE.upper()} {SHARES}ct @ {BUY_PRICE:.2f}"
                f"  {ticker}  order_id={oid[:8]}[/cyan]"
            )
            return oid
        else:
            console.print(f"  [red]Buy failed {r.status_code}: {r.text[:120]}[/red]")
    except Exception as e:
        console.print(f"  [red]Buy error: {e}[/red]")
    return None


async def _place_limit_sell(client: httpx.AsyncClient, ticker: str, shares: int) -> str | None:
    order = {
        "ticker":            ticker,
        "action":            "sell",
        "side":              SIDE,
        "count":             shares,
        "type":              "limit",
        "yes_price_dollars": _yes_price(SIDE, SELL_PRICE),
        "client_order_id":   str(uuid.uuid4()),
    }
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps(order),
            timeout=8,
        )
        if r.status_code in (200, 201):
            oid = r.json().get("order", {}).get("order_id", "")
            console.print(
                f"  [green]SELL {SIDE.upper()} {shares}ct @ {SELL_PRICE:.2f}"
                f"  {ticker}  order_id={oid[:8]}[/green]"
            )
            return oid
        else:
            console.print(f"  [red]Sell failed {r.status_code}: {r.text[:120]}[/red]")
    except Exception as e:
        console.print(f"  [red]Sell error: {e}[/red]")
    return None


async def _get_filled_count(client: httpx.AsyncClient, order_id: str) -> int:
    """Returns number of contracts filled so far for this order."""
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    try:
        r = await client.get(
            f"{KALSHI_BASE}/portfolio/orders/{order_id}",
            headers=_headers("GET", path),
            timeout=8,
        )
        if r.status_code == 200:
            o = r.json().get("order", {})
            return int(float(o.get("fill_count_fp", 0) or 0))
    except Exception as e:
        console.print(f"  [yellow]Fill check error: {e}[/yellow]")
    return 0


async def _cancel_order(client: httpx.AsyncClient, order_id: str) -> None:
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    try:
        r = await client.delete(
            f"{KALSHI_BASE}/portfolio/orders/{order_id}",
            headers=_headers("DELETE", path),
            timeout=8,
        )
        if r.status_code in (200, 204):
            console.print(f"  [dim]Cancelled order {order_id[:8]}[/dim]")
        else:
            console.print(f"  [yellow]Cancel {r.status_code}: {r.text[:80]}[/yellow]")
    except Exception as e:
        console.print(f"  [yellow]Cancel error: {e}[/yellow]")


# ── Window runner ─────────────────────────────────────────────────────────────

async def run_window(client: httpx.AsyncClient, window_num: int) -> dict:
    """
    Waits for the next window open, places buy, waits for fill, places sell.
    Returns summary dict.
    """
    wait_secs = _seconds_until_next_window()

    # Fetch real ticker from Kalshi (some 15-min slots don't exist)
    ticker, w_dt = await _fetch_next_ticker(client)
    if ticker is None:
        console.print("  [red]Could not resolve next ticker — skipping window[/red]")
        await asyncio.sleep(max(wait_secs, 60))
        return {"status": "skip"}

    console.print(
        f"\n[bold cyan]Window {window_num}  —  {ticker}[/bold cyan]"
        f"\n  Opens at {w_dt.strftime('%H:%M ET')}  "
        f"(in {wait_secs:.0f}s)"
    )

    # ── Wait until window opens ───────────────────────────────────────────────
    if wait_secs > 0:
        console.print(f"  [dim]Waiting {wait_secs:.0f}s…[/dim]")
        await asyncio.sleep(wait_secs)

    ts = datetime.now().strftime("%H:%M:%S")
    console.print(f"\n[bold]  [{ts}] Window open — placing buy order[/bold]")

    # ── Place limit buy ───────────────────────────────────────────────────────
    buy_oid = await _place_limit_buy(client, ticker)
    if not buy_oid:
        return {"status": "buy_failed"}

    # ── Listen for fills via WebSocket ───────────────────────────────────────
    filled         = 0    # buy contracts filled so far
    sell_filled_ws = 0    # sell contracts confirmed filled via WS
    sell_oid       = None

    console.print(f"  [dim]Listening for fills via WebSocket…[/dim]")

    try:
        async with websockets.connect(
            KALSHI_WS, additional_headers=_ws_headers(),
            ping_interval=20, open_timeout=10,
        ) as ws:
            # Subscribe to fills channel
            await ws.send(json.dumps({
                "id": 1, "cmd": "subscribe",
                "params": {"channels": ["fill"]},
            }))
            console.print(f"  [dim]WS connected — subscribed to fill channel[/dim]")

            deadline = asyncio.get_event_loop().time() + 895
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    await ws.ping()
                    continue

                msg   = json.loads(raw)
                if msg.get("type") != "fill":
                    continue

                data   = msg.get("msg", msg)
                oid    = data.get("order_id", "")
                # Kalshi WS uses count_fp (float); fall back to count
                count  = int(float(data.get("count_fp") or data.get("count") or 0))
                price  = float(data.get("yes_price_dollars", 0) or
                               data.get("no_price_dollars", 0) or BUY_PRICE)
                ts     = datetime.now().strftime("%H:%M:%S")

                # ── Buy fill on our buy order ─────────────────────────────
                if oid == buy_oid:
                    if count <= 0:
                        continue
                    filled += count
                    console.print(
                        f"  [bold green][{ts}] BUY FILL {count}ct → total {filled}/{SHARES}ct"
                        f"  @ {price:.2f}[/bold green]"
                    )
                    # Cancel existing sell and replace with new total
                    if sell_oid:
                        await _cancel_order(client, sell_oid)
                    sell_oid = await _place_limit_sell(client, ticker, filled)
                    if filled >= SHARES:
                        console.print(f"  [bold green]Fully bought ({SHARES}ct). Sell resting.[/bold green]")

                # ── Sell fill on our sell order ───────────────────────────
                elif sell_oid and oid == sell_oid:
                    if count <= 0:
                        continue
                    sell_filled_ws += count
                    console.print(
                        f"  [bold cyan][{ts}] SELL FILL {count}ct → total sold {sell_filled_ws}ct"
                        f"  @ {price:.2f}[/bold cyan]"
                    )
                    if sell_filled_ws >= filled:
                        console.print(f"  [bold cyan]Fully sold ({filled}ct). Round-trip complete.[/bold cyan]")

        # ── Window close ──────────────────────────────────────────────────────
        if filled == 0:
            console.print(f"  [yellow]No fill — cancelling buy[/yellow]")
            await _cancel_order(client, buy_oid)
            return {"status": "no_fill", "ticker": ticker}

        # Use WS-tracked sell fills; fall back to REST if WS missed any
        sell_filled = sell_filled_ws
        if sell_oid and sell_filled < filled:
            rest_filled = await _get_filled_count(client, sell_oid)
            if rest_filled > sell_filled:
                console.print(f"  [dim]REST fill check: {rest_filled}ct sold (WS had {sell_filled}ct)[/dim]")
                sell_filled = rest_filled

        pnl_locked = round((SELL_PRICE - BUY_PRICE) * sell_filled, 4)

        console.print(
            f"\n  [bold]Summary:[/bold]"
            f"\n    Bought:      {filled}ct @ {BUY_PRICE:.2f}"
            f"\n    Sell filled: {sell_filled}ct @ {SELL_PRICE:.2f}"
            f"\n    P&L locked:  ${pnl_locked:+.4f}"
            f"\n    Remaining {filled - sell_filled}ct expire at settlement"
        )
        return {
            "status":       "done",
            "ticker":       ticker,
            "bought":       filled,
            "sell_filled":  sell_filled,
            "pnl_locked":   pnl_locked,
        }

    except asyncio.CancelledError:
        # Clean up on Ctrl-C
        console.print("\n  [yellow]Interrupted — cancelling open orders…[/yellow]")
        await _cancel_order(client, buy_oid)
        if sell_oid:
            await _cancel_order(client, sell_oid)
        raise


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    profit_per_ct = SELL_PRICE - BUY_PRICE
    console.print(
        f"[bold cyan]MM Maker — {COIN} {SIDE.upper()}[/bold cyan]\n"
        f"  Buy @ {BUY_PRICE:.2f}  →  Sell @ {SELL_PRICE:.2f}  "
        f"profit/ct=${profit_per_ct:.2f}  shares={SHARES}\n"
        f"  Profit if fully round-tripped: ${profit_per_ct * SHARES:.2f}"
        + (f"\n  Running {RUN_WINDOWS} window(s)" if RUN_WINDOWS else "\n  Running until Ctrl-C")
    )

    async with httpx.AsyncClient() as client:
        window_num  = 1
        session_pnl = 0.0
        while True:
            result       = await run_window(client, window_num)
            session_pnl += result.get("pnl_locked", 0.0)
            console.print(f"\n  [bold]Session P&L so far: ${session_pnl:+.4f}[/bold]")

            if RUN_WINDOWS and window_num >= RUN_WINDOWS:
                break
            window_num += 1


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
