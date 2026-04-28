#!/usr/bin/env python3
"""
expire_maker.py — Place resting limit buys on BOTH YES and NO at window open.
Whichever side fills first → cancel the other → hold to expiry.

No sell order. Profit = $1.00 - limit if winner, loss = -limit if loser.
0% maker fee.

Run:   python expire_maker.py
Stop:  Ctrl-C
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG — one block per coin, set enabled=False to skip
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COINS = {
    "BTC": dict(enabled=True,  series="KXBTC15M", low_limit=0.02, high_limit=0.40, shares=1),
    "ETH": dict(enabled=True,  series="KXETH15M", low_limit=0.02, high_limit=0.40, shares=1),
    "SOL": dict(enabled=True,  series="KXSOL15M", low_limit=0.02, high_limit=0.43, shares=1),
    "XRP": dict(enabled=True,  series="KXXRP15M", low_limit=0.02, high_limit=0.34, shares=1),
}

RUN_WINDOWS = 0   # 0 = run forever; set to N to stop after N windows

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
ET_OFFSET   = timedelta(hours=-4)

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)


# ── Auth helpers ──────────────────────────────────────────────────────────────

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

def _ws_headers() -> dict:
    return _sign("GET", "/trade-api/ws/v2")


# ── Ticker helpers ────────────────────────────────────────────────────────────

def _seconds_until_next_window() -> float:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    cur    = now_et.replace(minute=mins, second=0, microsecond=0)
    return (cur + timedelta(minutes=15) - now_et).total_seconds()

def _ticker_from_close(series: str, close_utc: datetime) -> str:
    """Build a Kalshi ticker from series + close time in UTC."""
    close_et = close_utc + ET_OFFSET
    return series + "-" + close_et.strftime("%y%b%d%H%M").upper() + "-" + close_et.strftime("%M")


async def _resolve_next_ticker(client: httpx.AsyncClient, series: str) -> tuple[str, datetime, datetime]:
    """
    Before sleeping: fetch the CURRENT open market, read its close_time.
    Next market opens at close_time, closes at close_time + 15min.
    Returns (next_ticker, next_open_utc, next_close_utc) — no sleep needed,
    no API call at window open.
    """
    try:
        r = await client.get(
            f"{KALSHI_BASE}/markets",
            headers=_headers("GET", "/trade-api/v2/markets"),
            params={"series_ticker": series, "status": "open", "limit": 10},
            timeout=8,
        )
        if r.status_code == 200:
            markets = r.json().get("markets", [])
            now_ts  = datetime.now(timezone.utc).timestamp()
            # Currently trading = open_time <= now
            trading = [
                m for m in markets
                if m.get("open_time") and
                datetime.fromisoformat(m["open_time"].replace("Z", "+00:00")).timestamp() <= now_ts
            ]
            if trading:
                trading.sort(
                    key=lambda m: datetime.fromisoformat(m["open_time"].replace("Z", "+00:00")).timestamp(),
                    reverse=True,
                )
                cur = trading[0]
                cur_ticker   = cur["ticker"]
                close_str    = cur.get("close_time") or cur.get("expiration_time", "")
                if close_str:
                    close_utc  = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    next_open  = close_utc                        # next opens when current closes
                    next_close = close_utc + timedelta(minutes=15)
                    next_ticker= _ticker_from_close(series, next_close)
                    cur_et     = datetime.fromisoformat(
                        cur["open_time"].replace("Z", "+00:00")) + ET_OFFSET
                    close_et   = close_utc + ET_OFFSET
                    console.print(
                        f"  [dim]  Current: {cur_ticker}  "
                        f"{cur_et.strftime('%H:%M')}–{close_et.strftime('%H:%M ET')}[/dim]"
                    )
                    console.print(
                        f"  [dim]  Next:    {next_ticker}  "
                        f"opens {(next_open+ET_OFFSET).strftime('%H:%M ET')}[/dim]"
                    )
                    return next_ticker, next_open, next_close
    except Exception as e:
        console.print(f"  [yellow]  Ticker resolve failed ({e}) — using local calc[/yellow]")

    # Fallback: local calculation
    now_et     = datetime.now(timezone.utc) + ET_OFFSET
    mins       = (now_et.minute // 15) * 15
    cur_open   = now_et.replace(minute=mins, second=0, microsecond=0)
    next_open  = cur_open + timedelta(minutes=15)
    next_close = next_open + timedelta(minutes=15)
    # Convert back to UTC for consistency
    next_open_utc  = next_open  - ET_OFFSET
    next_close_utc = next_close - ET_OFFSET
    next_ticker    = _ticker_from_close(series, next_close_utc)
    console.print(f"  [yellow]  Fallback next ticker: {next_ticker}[/yellow]")
    return next_ticker, next_open_utc, next_close_utc


# ── Order helpers ─────────────────────────────────────────────────────────────

async def _place_buy(client: httpx.AsyncClient, ticker: str, side: str,
                     price: float, shares: int, tag: str) -> str | None:
    yes_price = f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"
    order = {
        "ticker":            ticker,
        "action":            "buy",
        "side":              side,
        "count":             shares,
        "type":              "limit",
        "yes_price_dollars": yes_price,
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
                f"  [cyan]{tag}  BUY {side.upper()} {shares}ct @ {price:.2f}"
                f"  order={oid[:8]}[/cyan]"
            )
            return oid
        else:
            console.print(f"  [red]{tag}  Buy failed {r.status_code}: {r.text[:120]}[/red]")
    except Exception as e:
        console.print(f"  [red]{tag}  Buy error: {e}[/red]")
    return None


async def _cancel(client: httpx.AsyncClient, oid: str, tag: str) -> bool:
    """
    Cancel an order and verify it's actually gone.
    Returns True if confirmed cancelled, False if it may still be live.
    """
    try:
        r = await client.delete(
            f"{KALSHI_BASE}/portfolio/orders/{oid}",
            headers=_headers("DELETE", f"/trade-api/v2/portfolio/orders/{oid}"),
            timeout=8,
        )
        if r.status_code in (200, 204):
            # Verify it's actually gone
            r2 = await client.get(
                f"{KALSHI_BASE}/portfolio/orders/{oid}",
                headers=_headers("GET", f"/trade-api/v2/portfolio/orders/{oid}"),
                timeout=8,
            )
            if r2.status_code == 200:
                status = r2.json().get("order", {}).get("status", "?")
                remaining = r2.json().get("order", {}).get("remaining_count_fp", "?")
                if status in ("canceled", "cancelled"):
                    console.print(f"  [dim]{tag}  Cancelled ✓ {oid[:8]}[/dim]")
                    return True
                else:
                    console.print(
                        f"  [bold red]{tag}  Cancel FAILED — order still {status}"
                        f" remaining={remaining}  oid={oid[:8]}[/bold red]"
                    )
                    return False
            else:
                console.print(f"  [dim]{tag}  Cancelled (verify {r2.status_code}) {oid[:8]}[/dim]")
                return True
        elif r.status_code == 404:
            console.print(f"  [dim]{tag}  Already gone (settlement)[/dim]")
            return True
        else:
            console.print(f"  [yellow]{tag}  Cancel {r.status_code}: {r.text[:80]}[/yellow]")
            return False
    except Exception as e:
        console.print(f"  [yellow]{tag}  Cancel error: {e}[/yellow]")
        return False


# ── Per-coin state ────────────────────────────────────────────────────────────

class CoinState:
    def __init__(self, sym: str, cfg: dict, ticker: str):
        self.sym     = sym
        self.cfg     = cfg
        self.ticker  = ticker
        # price → {"yes": oid, "no": oid, "filled": bool}
        self.levels: dict[float, dict] = {}
        self.fills:  list[dict]        = []   # {price, side, count}


# ── Window runner ─────────────────────────────────────────────────────────────

async def run_window(client: httpx.AsyncClient, window_num: int,
                     prefetched: dict[str, tuple[str, datetime]] | None = None) -> dict[str, tuple[str, datetime]]:
    """
    Run one window. Returns prefetched tickers for the NEXT window
    (resolved during the final minute of this window).
    prefetched: {sym: (ticker, open_utc)} resolved by the previous window.
    """
    active = {sym: cfg for sym, cfg in COINS.items() if cfg["enabled"]}
    console.print(f"\n[bold cyan]━━ Window {window_num} ━━[/bold cyan]")

    states: dict[str, CoinState] = {}

    if prefetched:
        # Sleep until the window actually opens (guards against pre-fetch boundary drift)
        any_open = next(iter(prefetched.values()))[1]
        wait = (any_open - datetime.now(timezone.utc)).total_seconds()
        if wait > 2:
            open_wall = datetime.now() + timedelta(seconds=wait)
            console.print(
                f"  [dim]Window opens in {wait:.0f}s  ({open_wall.strftime('%H:%M:%S')})"
                f" — waiting…[/dim]"
            )
            await asyncio.sleep(wait)

        console.print("  [dim]Using pre-resolved tickers from previous window[/dim]")
        for sym, cfg in active.items():
            entry = prefetched.get(sym)
            if entry:
                ticker, _ = entry
                states[sym] = CoinState(sym, cfg, ticker)
                console.print(f"  [dim]  {sym}  {ticker}[/dim]")
    else:
        # First run — resolve next tickers and wait
        wait_secs = _seconds_until_next_window()
        if wait_secs > 2:
            open_wall = datetime.now() + timedelta(seconds=wait_secs)
            console.print(
                f"\n  [dim]Opens in {wait_secs:.0f}s  ({open_wall.strftime('%H:%M:%S')})"
                f" — waiting…[/dim]"
            )
            await asyncio.sleep(wait_secs)
        for sym, cfg in active.items():
            next_ticker, _, _ = await _resolve_next_ticker(client, cfg["series"])
            states[sym] = CoinState(sym, cfg, next_ticker)

    ts = datetime.now().strftime("%H:%M:%S")
    console.print(f"\n[bold]  [{ts}] Window open — placing orders[/bold]")
    for sym, st in states.items():
        console.print(
            f"  {sym}  {st.ticker}"
            f"  limit={st.cfg['low_limit']:.2f}  shares={st.cfg['shares']}"
        )

    # Place YES + NO limit buys — highest price first across all symbols,
    # so near-mid orders (most likely to fill immediately) are resting ASAP.
    oid_map: dict[str, tuple[str, float, str]] = {}   # oid → (sym, price, side)

    # Build unified price list high→low (union of all enabled coins' ranges)
    all_prices: set[float] = set()
    for sym, st in states.items():
        cfg = st.cfg
        for cp in range(int(cfg["high_limit"] * 100), int(cfg["low_limit"] * 100) - 1, -1):
            all_prices.add(round(cp / 100, 2))
            st.levels[round(cp / 100, 2)] = {"yes": None, "no": None, "filled": False}

    sorted_prices = sorted(all_prices, reverse=True)   # 0.45 → 0.10
    total_orders  = sum(len(st.levels) * 2 for st in states.values())
    console.print(f"  Placing {total_orders} orders across {len(states)} coins "
                  f"({sorted_prices[-1]:.2f}–{sorted_prices[0]:.2f} high→low)…")

    # ── Connect WS first, then place orders concurrently ─────────────────────
    try:
        async with websockets.connect(
            KALSHI_WS, additional_headers=_ws_headers(),
            ping_interval=20, open_timeout=10,
        ) as ws:
            await ws.send(json.dumps({
                "id": 1, "cmd": "subscribe",
                "params": {"channels": ["fill"]},
            }))
            console.print(f"  [dim]WS connected — placing orders while listening…[/dim]")

            # Place orders as a background task so WS listens simultaneously
            async def place_all():
                for p in sorted_prices:
                    for sym, st in states.items():
                        if p not in st.levels:
                            continue
                        cfg = st.cfg
                        yes_oid = await _place_buy(client, st.ticker, "yes", p, cfg["shares"], f"{sym}@{p:.2f}")
                        await asyncio.sleep(0.15)
                        no_oid  = await _place_buy(client, st.ticker, "no",  p, cfg["shares"], f"{sym}@{p:.2f}")
                        await asyncio.sleep(0.15)
                        st.levels[p]["yes"] = yes_oid
                        st.levels[p]["no"]  = no_oid
                        if yes_oid: oid_map[yes_oid] = (sym, p, "yes")
                        if no_oid:  oid_map[no_oid]  = (sym, p, "no")
                console.print(f"  [dim]All {len(oid_map)} orders placed.[/dim]")

            placement_task = asyncio.create_task(place_all())

            deadline      = asyncio.get_event_loop().time() + 895
            next_tickers: dict[str, tuple[str, datetime]] = {}
            prefetch_done = False

            while asyncio.get_event_loop().time() < deadline:
                # Pre-fetch next window tickers in the final 90s of this window.
                # Use local wall-clock calc — avoids race where Kalshi already
                # lists the NEXT market as "open", causing _resolve_next_ticker
                # to return a ticker one window too far ahead.
                if not prefetch_done and (deadline - asyncio.get_event_loop().time()) < 90:
                    console.print("  [dim]Pre-fetching next window tickers…[/dim]")
                    now_et        = datetime.now(timezone.utc) + ET_OFFSET
                    mins          = (now_et.minute // 15) * 15
                    cur_open_et   = now_et.replace(minute=mins, second=0, microsecond=0)
                    nxt_open_et   = cur_open_et   + timedelta(minutes=15)
                    nxt_close_et  = nxt_open_et   + timedelta(minutes=15)
                    nxt_close_utc = nxt_close_et  - ET_OFFSET
                    nxt_open_utc = nxt_open_et - ET_OFFSET
                    for sym, cfg in active.items():
                        nt = _ticker_from_close(cfg["series"], nxt_close_utc)
                        next_tickers[sym] = (nt, nxt_open_utc)
                        console.print(f"  [dim]  {sym}: {nt}  opens {nxt_open_et.strftime('%H:%M ET')}[/dim]")
                    prefetch_done = True

                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    await ws.ping()
                    continue

                msg = json.loads(raw)
                if msg.get("type") != "fill":
                    continue

                data  = msg.get("msg", msg)
                oid   = data.get("order_id", "")
                count = int(float(data.get("count_fp") or data.get("count") or 0))
                if count <= 0 or oid not in oid_map:
                    continue

                sym, price, side = oid_map[oid]
                st = states[sym]
                lv = st.levels[price]
                if lv["filled"]:
                    # Second side filled — locked profit confirmed
                    ts = datetime.now().strftime("%H:%M:%S")
                    console.print(
                        f"  [bold yellow][{ts}] {sym}@{price:.2f}  LOCKED"
                        f" +${1.0 - price*2:.2f} guaranteed[/bold yellow]"
                    )
                    st.fills.append({"price": price, "side": side, "count": count, "locked": True})
                    continue

                lv["filled"] = True
                ts = datetime.now().strftime("%H:%M:%S")
                console.print(
                    f"  [bold green][{ts}] {sym}@{price:.2f}  FILLED {side.upper()}"
                    f"  {count}ct[/bold green]  — leaving other side open (lock opportunity)"
                )
                st.fills.append({"price": price, "side": side, "count": count})

            placement_task.cancel()

    except asyncio.CancelledError:
        console.print("\n  [yellow]Interrupted — cancelling all open orders…[/yellow]")
        for sym, st in states.items():
            for p, lv in st.levels.items():
                if not lv["filled"]:
                    if lv["yes"]: await _cancel(client, lv["yes"], f"{sym}@{p:.2f}")
                    if lv["no"]:  await _cancel(client, lv["no"],  f"{sym}@{p:.2f}")
        raise

    # Cancel any unfilled orders at window end
    for sym, st in states.items():
        for p, lv in st.levels.items():
            if not lv["filled"]:
                if lv["yes"]: await _cancel(client, lv["yes"], f"{sym}@{p:.2f}")
                if lv["no"]:  await _cancel(client, lv["no"],  f"{sym}@{p:.2f}")

    # ── Window summary ────────────────────────────────────────────────────────
    console.print(f"\n  [bold]Window {window_num} summary:[/bold]")
    for sym, st in states.items():
        n = len(st.fills)
        console.print(f"    {sym}  {n} fills  →  hold to expiry")
        for f in st.fills:
            if f.get("locked"):
                console.print(
                    f"      {f['side'].upper()} {f['count']}ct @ {f['price']:.2f}"
                    f"  [yellow]BOTH FILLED → locked +${1.0 - f['price']*2:.2f}[/yellow]"
                )
            else:
                console.print(
                    f"      {f['side'].upper()} {f['count']}ct @ {f['price']:.2f}"
                    f"  (win=+${1-f['price']:.2f}  lose=-${f['price']:.2f})"
                )

    return next_tickers


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    active = {sym: cfg for sym, cfg in COINS.items() if cfg["enabled"]}
    console.print("[bold cyan]Expire Maker[/bold cyan]")
    console.print("Strategy: YES+NO limit buys at every cent in range → each fills independently → hold to expiry\n")
    for sym, cfg in active.items():
        n_levels = int(cfg["high_limit"] * 100) - int(cfg["low_limit"] * 100) + 1
        console.print(
            f"  {sym}  {cfg['low_limit']:.2f}–{cfg['high_limit']:.2f}"
            f"  {n_levels} price levels × 2 sides = {n_levels*2} orders/window"
            f"  shares={cfg['shares']}"
        )
    console.print()

    async with httpx.AsyncClient() as client:
        window_num   = 1
        next_tickers: dict[str, tuple[str, datetime]] | None = None
        while True:
            next_tickers = await run_window(client, window_num, prefetched=next_tickers)
            if RUN_WINDOWS and window_num >= RUN_WINDOWS:
                break
            window_num += 1


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
