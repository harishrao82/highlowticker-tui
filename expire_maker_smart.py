#!/usr/bin/env python3
"""
expire_maker_smart.py — Hybrid cancel + replenish strategy.

Per price level:
  1. Place YES + NO limit buys at window open.
  2. First fill → try to CANCEL the other side (verified).
       • Cancel succeeds → single-side hold to expiry (normal).
       • Cancel FAILS   → order still live, replenish one level below
                          (the surviving order is a potential second fill → LOCK).
  3. Both sides fill (LOCK) → guaranteed profit = $1.00 - 2×price →
                               replenish one level below.
  4. End of window → cancel any remaining unfilled orders.

Rich Live table shows status at every price level in real-time.

Run:   python expire_maker_smart.py
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
from rich.live import Live
from rich.table import Table
from rich.text import Text

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
    close_et = close_utc + ET_OFFSET
    return series + "-" + close_et.strftime("%y%b%d%H%M").upper() + "-" + close_et.strftime("%M")


async def _resolve_next_ticker(
    client: httpx.AsyncClient, series: str
) -> tuple[str, datetime, datetime]:
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
                close_str = cur.get("close_time") or cur.get("expiration_time", "")
                if close_str:
                    close_utc  = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    next_open  = close_utc
                    next_close = close_utc + timedelta(minutes=15)
                    next_ticker = _ticker_from_close(series, next_close)
                    cur_et  = datetime.fromisoformat(cur["open_time"].replace("Z", "+00:00")) + ET_OFFSET
                    close_et = close_utc + ET_OFFSET
                    console.print(
                        f"  [dim]  Current: {cur['ticker']}  "
                        f"{cur_et.strftime('%H:%M')}–{close_et.strftime('%H:%M ET')}[/dim]"
                    )
                    console.print(
                        f"  [dim]  Next:    {next_ticker}  "
                        f"opens {(next_open+ET_OFFSET).strftime('%H:%M ET')}[/dim]"
                    )
                    return next_ticker, next_open, next_close
    except Exception as e:
        console.print(f"  [yellow]  Ticker resolve failed ({e}) — using local calc[/yellow]")

    # Fallback
    now_et         = datetime.now(timezone.utc) + ET_OFFSET
    mins           = (now_et.minute // 15) * 15
    cur_open_et    = now_et.replace(minute=mins, second=0, microsecond=0)
    next_open_et   = cur_open_et   + timedelta(minutes=15)
    next_close_et  = next_open_et  + timedelta(minutes=15)
    next_open_utc  = next_open_et  - ET_OFFSET
    next_close_utc = next_close_et - ET_OFFSET
    next_ticker    = _ticker_from_close(series, next_close_utc)
    console.print(f"  [yellow]  Fallback next ticker: {next_ticker}[/yellow]")
    return next_ticker, next_open_utc, next_close_utc


# ── Order helpers ─────────────────────────────────────────────────────────────

async def _place_buy(
    client: httpx.AsyncClient, ticker: str, side: str,
    price: float, shares: int, tag: str
) -> str | None:
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
    Cancel an order and verify it was actually cancelled.
    Returns True if confirmed gone, False if still live.
    """
    try:
        r = await client.delete(
            f"{KALSHI_BASE}/portfolio/orders/{oid}",
            headers=_headers("DELETE", f"/trade-api/v2/portfolio/orders/{oid}"),
            timeout=8,
        )
        if r.status_code in (200, 204):
            r2 = await client.get(
                f"{KALSHI_BASE}/portfolio/orders/{oid}",
                headers=_headers("GET", f"/trade-api/v2/portfolio/orders/{oid}"),
                timeout=8,
            )
            if r2.status_code == 200:
                status    = r2.json().get("order", {}).get("status", "?")
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
                # Verify endpoint failed — assume success (404 = settled/gone)
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
        self.sym    = sym
        self.cfg    = cfg
        self.ticker = ticker
        # price → {
        #   "yes": oid, "no": oid,
        #   "yes_filled": bool, "no_filled": bool,
        #   "yes_filled_count": int, "no_filled_count": int,
        #   "cancel_failed": str | None,  # "yes" or "no" — which side's cancel failed
        # }
        self.levels: dict[float, dict] = {}
        self.fills:  list[dict]        = []

    def lowest_active(self) -> float | None:
        """Lowest price where NOT both sides are filled."""
        active = [p for p, lv in self.levels.items()
                  if not (lv["yes_filled"] and lv["no_filled"])]
        return min(active) if active else None

    def next_replenish_price(self) -> float | None:
        low = self.lowest_active()
        if low is None:
            return None
        new_p = round(low - 0.01, 2)
        if new_p < self.cfg["low_limit"]:
            return None
        if new_p in self.levels:
            return None
        return new_p


# ── Live status table ─────────────────────────────────────────────────────────

def _build_status_table(states: dict, window_num: int, elapsed: int) -> Table:
    syms   = list(states.keys())
    shares = next(iter(states.values())).cfg["shares"] if states else 1

    all_prices = sorted(
        {p for st in states.values() for p in st.levels},
        reverse=True
    )

    tbl = Table(box=None, padding=(0, 1), show_header=True,
                title=f"Window {window_num}  t+{elapsed}s  shares={shares}")
    tbl.add_column("Price", width=6, justify="right", style="bold cyan")
    for sym in syms:
        tbl.add_column(f"{sym} Y", width=6, justify="center")
        tbl.add_column(f"{sym} N", width=6, justify="center")

    for p in all_prices:
        row: list = [f"${p:.2f}"]
        for sym in syms:
            st = states[sym]
            lv = st.levels.get(p)
            if lv is None:
                row += [Text("—", style="dim"), Text("—", style="dim")]
                continue

            both = lv["yes_filled"] and lv["no_filled"]
            cf   = lv.get("cancel_failed")   # "yes" or "no" — which side's cancel failed

            for side in ("yes", "no"):
                filled_ct = lv.get(f"{side}_filled_count", 0)
                fully     = lv.get(f"{side}_filled", False)
                other     = "no" if side == "yes" else "yes"

                if both:
                    cell = Text("LOCK", style="bold yellow")
                elif fully:
                    # This side filled
                    if cf == other:
                        # Other side's cancel failed — show as warning
                        cell = Text(f"FILL!", style="bold magenta")
                    else:
                        cell = Text(f"FILL", style="bold green")
                elif cf == side:
                    # This side's cancel failed — still live, dangerous
                    cell = Text("C!live", style="bold red")
                elif filled_ct > 0:
                    cell = Text(f"{filled_ct}/{shares}", style="yellow")
                else:
                    cell = Text("·", style="dim")
                row.append(cell)
        tbl.add_row(*row)

    locked_ct  = sum(
        1 for st in states.values()
        for lv in st.levels.values()
        if lv.get("yes_filled") and lv.get("no_filled")
    )
    locked_pnl = sum(
        1.0 - p * 2
        for st in states.values()
        for p, lv in st.levels.items()
        if lv.get("yes_filled") and lv.get("no_filled") and (1.0 - p * 2) > 0
    )
    cancel_fails = sum(
        1 for st in states.values()
        for lv in st.levels.values()
        if lv.get("cancel_failed")
    )
    cf_note = f"  [bold red]Cancel fails: {cancel_fails}[/bold red]" if cancel_fails else ""
    tbl.caption = (
        f"[yellow]Locks: {locked_ct}  +${locked_pnl:.2f} guaranteed[/yellow]{cf_note}  "
        f"[dim]· open  FILL green  C!live = cancel failed  LOCK = both filled[/dim]"
    )
    return tbl


# ── Window runner ─────────────────────────────────────────────────────────────

async def run_window(
    client: httpx.AsyncClient, window_num: int,
    prefetched: dict[str, tuple[str, datetime]] | None = None,
) -> dict[str, tuple[str, datetime]]:
    active = {sym: cfg for sym, cfg in COINS.items() if cfg["enabled"]}
    console.print(f"\n[bold cyan]━━ Window {window_num} ━━[/bold cyan]")

    states: dict[str, CoinState] = {}

    if prefetched:
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

    oid_map: dict[str, tuple[str, float, str]] = {}   # oid → (sym, price, side)

    all_prices: set[float] = set()
    for sym, st in states.items():
        cfg = st.cfg
        for cp in range(int(cfg["high_limit"] * 100), int(cfg["low_limit"] * 100) - 1, -1):
            p = round(cp / 100, 2)
            all_prices.add(p)
            st.levels[p] = {
                "yes": None, "no": None,
                "yes_filled": False, "no_filled": False,
                "cancel_failed": None,
            }

    sorted_prices = sorted(all_prices, reverse=True)
    total_orders  = sum(len(st.levels) * 2 for st in states.values())
    console.print(
        f"  Placing {total_orders} orders across {len(states)} coins "
        f"({sorted_prices[-1]:.2f}–{sorted_prices[0]:.2f} high→low)…"
    )

    try:
        async with websockets.connect(
            KALSHI_WS, additional_headers=_ws_headers(),
            ping_interval=20, open_timeout=10,
        ) as ws:
            await ws.send(json.dumps({
                "id": 1, "cmd": "subscribe",
                "params": {"channels": ["fill"]},
            }))
            console.print("  [dim]WS connected — placing orders while listening…[/dim]")

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

            deadline       = asyncio.get_event_loop().time() + 895
            window_start_t = asyncio.get_event_loop().time()
            next_tickers: dict[str, tuple[str, datetime]] = {}
            prefetch_done = False

            with Live(
                _build_status_table(states, window_num, 0),
                console=console, refresh_per_second=2,
                vertical_overflow="visible",
            ) as live:
                while asyncio.get_event_loop().time() < deadline:
                    # Pre-fetch next window tickers in final 90s
                    if not prefetch_done and (deadline - asyncio.get_event_loop().time()) < 90:
                        console.print("  [dim]Pre-fetching next window tickers…[/dim]")
                        now_et        = datetime.now(timezone.utc) + ET_OFFSET
                        mins          = (now_et.minute // 15) * 15
                        cur_open_et   = now_et.replace(minute=mins, second=0, microsecond=0)
                        nxt_open_et   = cur_open_et  + timedelta(minutes=15)
                        nxt_close_et  = nxt_open_et  + timedelta(minutes=15)
                        nxt_close_utc = nxt_close_et - ET_OFFSET
                        nxt_open_utc  = nxt_open_et  - ET_OFFSET
                        for sym, cfg in active.items():
                            nt = _ticker_from_close(cfg["series"], nxt_close_utc)
                            next_tickers[sym] = (nt, nxt_open_utc)
                            console.print(f"  [dim]  {sym}: {nt}  opens {nxt_open_et.strftime('%H:%M ET')}[/dim]")
                        prefetch_done = True

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        await ws.ping()
                        elapsed = int(asyncio.get_event_loop().time() - window_start_t)
                        live.update(_build_status_table(states, window_num, elapsed))
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
                    st  = states[sym]
                    lv  = st.levels[price]
                    ts  = datetime.now().strftime("%H:%M:%S")
                    tag = f"{sym}@{price:.2f}"

                    # Accumulate partial fills
                    fill_key = f"{side}_filled_count"
                    lv[fill_key] = lv.get(fill_key, 0) + count
                    fully_filled = lv[fill_key] >= st.cfg["shares"]
                    lv[f"{side}_filled"] = fully_filled

                    if not fully_filled:
                        console.print(
                            f"  [dim]{tag}  partial {side.upper()}"
                            f"  {lv[fill_key]}/{st.cfg['shares']}ct[/dim]"
                        )
                        elapsed = int(asyncio.get_event_loop().time() - window_start_t)
                        live.update(_build_status_table(states, window_num, elapsed))
                        continue

                    both_filled = lv["yes_filled"] and lv["no_filled"]

                    if both_filled:
                        # ── LOCK ──────────────────────────────────────────────
                        locked_pnl = round(1.0 - price * 2, 2)
                        console.print(
                            f"  [bold yellow][{ts}] {tag}  LOCKED"
                            f"  +${locked_pnl:.2f} guaranteed[/bold yellow]"
                        )
                        st.fills.append({"price": price, "side": side, "count": count, "locked": True})

                        new_p = st.next_replenish_price()
                        if new_p is not None:
                            console.print(
                                f"  [dim cyan]{sym}  replenish @ ${new_p:.2f}"
                                f"  (lowest active ${st.lowest_active():.2f})[/dim cyan]"
                            )
                            st.levels[new_p] = {
                                "yes": None, "no": None,
                                "yes_filled": False, "no_filled": False,
                                "cancel_failed": None,
                            }
                            yes_oid = await _place_buy(client, st.ticker, "yes", new_p,
                                                       st.cfg["shares"], f"{sym}@{new_p:.2f}[R]")
                            await asyncio.sleep(0.15)
                            no_oid  = await _place_buy(client, st.ticker, "no",  new_p,
                                                       st.cfg["shares"], f"{sym}@{new_p:.2f}[R]")
                            st.levels[new_p]["yes"] = yes_oid
                            st.levels[new_p]["no"]  = no_oid
                            if yes_oid: oid_map[yes_oid] = (sym, new_p, "yes")
                            if no_oid:  oid_map[no_oid]  = (sym, new_p, "no")
                        else:
                            console.print(f"  [dim]{sym}  no replenish — at low_limit[/dim]")

                    else:
                        # ── First fill — try to cancel the other side ─────────
                        other_side = "no" if side == "yes" else "yes"
                        other_oid  = lv.get(other_side)

                        console.print(
                            f"  [bold green][{ts}] {tag}  FILLED {side.upper()}"
                            f"  {count}ct[/bold green]"
                            f"  — cancelling {other_side.upper()}…"
                        )
                        st.fills.append({"price": price, "side": side, "count": count})

                        if other_oid:
                            cancelled = await _cancel(client, other_oid, tag)
                            if not cancelled:
                                # ── Cancel failed — order still live ──────────
                                lv["cancel_failed"] = other_side
                                console.print(
                                    f"  [bold red]{tag}  {other_side.upper()} cancel failed"
                                    f" — order still live, replenishing below[/bold red]"
                                )
                                # Replenish: new level below so we extend coverage
                                new_p = st.next_replenish_price()
                                if new_p is not None:
                                    console.print(
                                        f"  [dim cyan]{sym}  replenish @ ${new_p:.2f}"
                                        f"  (cancel-fail backup)[/dim cyan]"
                                    )
                                    st.levels[new_p] = {
                                        "yes": None, "no": None,
                                        "yes_filled": False, "no_filled": False,
                                        "cancel_failed": None,
                                    }
                                    yes_oid = await _place_buy(
                                        client, st.ticker, "yes", new_p,
                                        st.cfg["shares"], f"{sym}@{new_p:.2f}[CF]"
                                    )
                                    await asyncio.sleep(0.15)
                                    no_oid  = await _place_buy(
                                        client, st.ticker, "no", new_p,
                                        st.cfg["shares"], f"{sym}@{new_p:.2f}[CF]"
                                    )
                                    st.levels[new_p]["yes"] = yes_oid
                                    st.levels[new_p]["no"]  = no_oid
                                    if yes_oid: oid_map[yes_oid] = (sym, new_p, "yes")
                                    if no_oid:  oid_map[no_oid]  = (sym, new_p, "no")

                    elapsed = int(asyncio.get_event_loop().time() - window_start_t)
                    live.update(_build_status_table(states, window_num, elapsed))

            placement_task.cancel()

    except asyncio.CancelledError:
        console.print("\n  [yellow]Interrupted — cancelling open orders…[/yellow]")
        for sym, st in states.items():
            for p, lv in st.levels.items():
                if not lv["yes_filled"] and lv["yes"]:
                    await _cancel(client, lv["yes"], f"{sym}@{p:.2f}")
                if not lv["no_filled"] and lv["no"]:
                    await _cancel(client, lv["no"],  f"{sym}@{p:.2f}")
        raise

    # ── End-of-window cleanup ─────────────────────────────────────────────────
    console.print("\n  [dim]Window closing — cancelling unfilled orders…[/dim]")
    for sym, st in states.items():
        for p, lv in st.levels.items():
            if not lv["yes_filled"] and lv["yes"]:
                await _cancel(client, lv["yes"], f"{sym}@{p:.2f}")
            if not lv["no_filled"] and lv["no"]:
                await _cancel(client, lv["no"],  f"{sym}@{p:.2f}")

    # ── Window summary ────────────────────────────────────────────────────────
    console.print(f"\n  [bold]Window {window_num} summary:[/bold]")
    for sym, st in states.items():
        locks    = [f for f in st.fills if f.get("locked")]
        singles  = [f for f in st.fills if not f.get("locked")]
        cf_levels = [(p, lv) for p, lv in st.levels.items() if lv.get("cancel_failed")]
        console.print(
            f"    {sym}  {len(locks)} locks  {len(singles)} singles"
            f"  {len(cf_levels)} cancel-fails"
        )
        for f in locks:
            console.print(
                f"      LOCK @ ${f['price']:.2f}"
                f"  [yellow]+${1.0 - f['price']*2:.2f} guaranteed[/yellow]"
            )
        for f in singles:
            console.print(
                f"      {f['side'].upper()} @ ${f['price']:.2f}"
                f"  (win=+${1-f['price']:.2f}  lose=-${f['price']:.2f})"
            )
        for p, lv in cf_levels:
            console.print(
                f"      [bold red]CANCEL FAIL @ ${p:.2f}"
                f"  {lv['cancel_failed'].upper()} still live[/bold red]"
            )

    return next_tickers


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    active = {sym: cfg for sym, cfg in COINS.items() if cfg["enabled"]}
    console.print("[bold cyan]Expire Maker Smart[/bold cyan]")
    console.print(
        "Strategy: YES+NO limit buys → first fill cancels other side (verified)\n"
        "          cancel fail → replenish below | LOCK → replenish below\n"
    )
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
