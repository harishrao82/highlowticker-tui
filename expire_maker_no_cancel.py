#!/usr/bin/env python3
"""
expire_maker_no_cancel.py

Strategy:
  - Batch place YES + NO limit buys at every cent from low_limit → high_limit
  - Listen for fills via WebSocket
  - When one side fills → queue the other side for batch cancel (10s debounce)
  - End of window → batch cancel all remaining unfilled orders

0% maker fee.

Run:   python expire_maker_no_cancel.py
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
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COINS = {
    "BTC": dict(enabled=True,  series="KXBTC15M", low_limit=0.02, high_limit=0.40, shares=1),
    "ETH": dict(enabled=True,  series="KXETH15M", low_limit=0.02, high_limit=0.40, shares=1),
    "SOL": dict(enabled=True,  series="KXSOL15M", low_limit=0.02, high_limit=0.43, shares=1),
    "XRP": dict(enabled=True,  series="KXXRP15M", low_limit=0.02, high_limit=0.34, shares=1),
}

RUN_WINDOWS   = 0     # 0 = run forever
CANCEL_DELAY  = 10    # seconds to buffer before batch-cancelling other side

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
ET_OFFSET   = timedelta(hours=-4)

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)


# ── Auth ──────────────────────────────────────────────────────────────────────

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
            markets  = r.json().get("markets", [])
            now_ts   = datetime.now(timezone.utc).timestamp()
            trading  = [
                m for m in markets
                if m.get("open_time") and
                datetime.fromisoformat(m["open_time"].replace("Z", "+00:00")).timestamp() <= now_ts
            ]
            if trading:
                trading.sort(
                    key=lambda m: datetime.fromisoformat(m["open_time"].replace("Z", "+00:00")).timestamp(),
                    reverse=True,
                )
                cur       = trading[0]
                close_str = cur.get("close_time") or cur.get("expiration_time", "")
                if close_str:
                    close_utc   = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    next_open   = close_utc
                    next_close  = close_utc + timedelta(minutes=15)
                    next_ticker = _ticker_from_close(series, next_close)
                    cur_et      = datetime.fromisoformat(cur["open_time"].replace("Z", "+00:00")) + ET_OFFSET
                    console.print(
                        f"  [dim]  Current: {cur['ticker']}  "
                        f"{cur_et.strftime('%H:%M')}–{(close_utc+ET_OFFSET).strftime('%H:%M ET')}[/dim]"
                    )
                    console.print(f"  [dim]  Next:    {next_ticker}[/dim]")
                    return next_ticker, next_open, next_close
    except Exception as e:
        console.print(f"  [yellow]  Ticker resolve failed ({e}) — using local calc[/yellow]")

    now_et         = datetime.now(timezone.utc) + ET_OFFSET
    mins           = (now_et.minute // 15) * 15
    cur_open_et    = now_et.replace(minute=mins, second=0, microsecond=0)
    next_open_et   = cur_open_et  + timedelta(minutes=15)
    next_close_et  = next_open_et + timedelta(minutes=15)
    next_open_utc  = next_open_et  - ET_OFFSET
    next_close_utc = next_close_et - ET_OFFSET
    next_ticker    = _ticker_from_close(series, next_close_utc)
    console.print(f"  [yellow]  Fallback ticker: {next_ticker}[/yellow]")
    return next_ticker, next_open_utc, next_close_utc


# ── Batch order helpers ───────────────────────────────────────────────────────

async def _place_buy(
    client: httpx.AsyncClient, ticker: str, side: str,
    price: float, shares: int, tag: str, coid: str | None = None
) -> str | None:
    yes_price = f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"
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
                "client_order_id":   coid or str(uuid.uuid4()),
            }),
            timeout=8,
        )
        if r.status_code in (200, 201):
            oid = r.json().get("order", {}).get("order_id", "")
            console.print(f"  [cyan]{tag}  BUY {side.upper()} {shares}ct @ {price:.2f}  {oid[:8]}[/cyan]")
            return oid
        else:
            console.print(f"  [red]{tag}  Buy failed {r.status_code}: {r.text[:120]}[/red]")
    except Exception as e:
        console.print(f"  [red]{tag}  Buy error: {e}[/red]")
    return None


async def _place_sell(
    client: httpx.AsyncClient, ticker: str, side: str,
    price: float, shares: int, tag: str
) -> str | None:
    yes_price = f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps({
                "ticker":            ticker,
                "action":            "sell",
                "side":              side,
                "count":             shares,
                "type":              "limit",
                "yes_price_dollars": yes_price,
                "client_order_id":   str(uuid.uuid4()),
            }),
            timeout=8,
        )
        if r.status_code in (200, 201):
            oid = r.json().get("order", {}).get("order_id", "")
            console.print(f"  [magenta]{tag}  SELL {side.upper()} {shares}ct @ {price:.2f}  {oid[:8]}[/magenta]")
            return oid
        else:
            console.print(f"  [red]{tag}  Sell failed {r.status_code}: {r.text[:120]}[/red]")
    except Exception as e:
        console.print(f"  [red]{tag}  Sell error: {e}[/red]")
    return None


async def _cancel_batch(client: httpx.AsyncClient, oids: list[str], tag: str) -> None:
    """Cancel up to 20 orders per call."""
    for i in range(0, len(oids), 20):
        chunk = oids[i : i + 20]
        try:
            r = await client.request(
                "DELETE",
                f"{KALSHI_BASE}/portfolio/orders/batched",
                headers=_headers("DELETE", "/trade-api/v2/portfolio/orders/batched"),
                content=json.dumps({"orders": [{"order_id": oid} for oid in chunk]}),
                timeout=15,
            )
            if r.status_code == 200:
                for item in r.json().get("orders", []):
                    err   = item.get("error")
                    order = item.get("order") or {}
                    if err and err.get("code") != "not_found":
                        console.print(f"  [yellow]{tag}  err: {err.get('message', err)}[/yellow]")
                    else:
                        remaining = float(order.get("remaining_count_fp", 0))
                        if remaining != 0:
                            console.print(
                                f"  [yellow]{tag}  remaining={remaining}"
                                f"  {order.get('order_id','?')[:8]}[/yellow]"
                            )
            else:
                console.print(f"  [yellow]{tag}  {r.status_code}: {r.text[:120]}[/yellow]")
        except Exception as e:
            console.print(f"  [yellow]{tag}  error: {e}[/yellow]")
        await asyncio.sleep(1)
    console.print(f"  [dim]{tag}  cancelled {len(oids)} orders[/dim]")


# ── Per-coin state ────────────────────────────────────────────────────────────

class CoinState:
    def __init__(self, sym: str, cfg: dict, ticker: str):
        self.sym    = sym
        self.cfg    = cfg
        self.ticker = ticker
        # price → {"yes": oid|None, "no": oid|None,
        #           "yes_filled": bool, "no_filled": bool,
        #           "yes_filled_count": int, "no_filled_count": int}
        self.levels: dict[float, dict] = {}
        self.fills:  list[dict]        = []
        # Rebalance: oids of extra orders placed (buy + sell), for end-of-window cleanup
        self.rebal_buy_oids: list[str]        = []
        self.sell_oids:      list[str]        = []
        # Lowest fill price seen per side — rebalance triggers only on new lows
        self.rebal_lowest: dict[str, float]   = {"yes": 99.0, "no": 99.0}


# ── Window runner ─────────────────────────────────────────────────────────────

async def run_window(
    client: httpx.AsyncClient,
    window_num: int,
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
                f"  [dim]Opens in {wait:.0f}s  ({open_wall.strftime('%H:%M:%S')}) — waiting…[/dim]"
            )
            await asyncio.sleep(wait)
        console.print("  [dim]Using pre-resolved tickers[/dim]")
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
                f"\n  [dim]Opens in {wait_secs:.0f}s  ({open_wall.strftime('%H:%M:%S')}) — waiting…[/dim]"
            )
            await asyncio.sleep(wait_secs)
        for sym, cfg in active.items():
            next_ticker, _, _ = await _resolve_next_ticker(client, cfg["series"])
            states[sym] = CoinState(sym, cfg, next_ticker)

    ts = datetime.now().strftime("%H:%M:%S")
    console.print(f"\n[bold]  [{ts}] Window open — placing orders[/bold]")

    # ── Build order list: high→low across all coins ───────────────────────────
    # Pre-generate client_order_ids so fill events are never missed during placement
    coid_map: dict[str, tuple[str, float, str]] = {}  # coid → (sym, price, side)

    all_prices: set[float] = set()
    for sym, st in states.items():
        for cp in range(int(st.cfg["high_limit"] * 100), int(st.cfg["low_limit"] * 100) - 1, -1):
            p = round(cp / 100, 2)
            all_prices.add(p)
            yes_coid = str(uuid.uuid4())
            no_coid  = str(uuid.uuid4())
            st.levels[p] = {"yes": None, "no": None,
                             "yes_filled": False, "no_filled": False,
                             "yes_coid": yes_coid, "no_coid": no_coid}
            coid_map[yes_coid] = (sym, p, "yes")
            coid_map[no_coid]  = (sym, p, "no")

    sorted_prices = sorted(all_prices, reverse=True)
    total_orders  = sum(len(st.levels) * 2 for st in states.values())
    console.print(
        f"  Placing {total_orders} orders across {len(states)} coins "
        f"({sorted_prices[-1]:.2f}–{sorted_prices[0]:.2f})…"
    )

    # ── Connect WS ───────────────────────────────────────────────────────────
    try:
        async with websockets.connect(
            KALSHI_WS, additional_headers=_ws_headers(),
            ping_interval=20, open_timeout=10,
        ) as ws:
            await ws.send(json.dumps({
                "id": 1, "cmd": "subscribe",
                "params": {"channels": ["fill"]},
            }))
            console.print("  [dim]WS connected — fill channel[/dim]")

            # ── Place orders in batches of 10, 3s between chunks ─────────
            async def place_all():
                # Build ordered spec list: high→low, interleaved across coins
                order_specs = []
                for p in sorted_prices:
                    for sym, st in states.items():
                        if p not in st.levels:
                            continue
                        lv = st.levels[p]
                        for side in ("yes", "no"):
                            order_specs.append({
                                "sym": sym, "price": p, "side": side,
                                "coid": lv[f"{side}_coid"],
                                "ticker": st.ticker,
                                "shares": st.cfg["shares"],
                            })

                placed = 0
                BATCH = 10
                for bi, i in enumerate(range(0, len(order_specs), BATCH)):
                    chunk = order_specs[i : i + BATCH]
                    batch_body = []
                    for spec in chunk:
                        p, side = spec["price"], spec["side"]
                        yp = f"{p:.2f}" if side == "yes" else f"{1.0 - p:.2f}"
                        batch_body.append({
                            "ticker":            spec["ticker"],
                            "action":            "buy",
                            "side":              side,
                            "count":             spec["shares"],
                            "type":              "limit",
                            "yes_price_dollars": yp,
                            "client_order_id":   spec["coid"],
                        })
                    try:
                        r = await client.post(
                            f"{KALSHI_BASE}/portfolio/orders/batched",
                            headers=_headers("POST", "/trade-api/v2/portfolio/orders/batched"),
                            content=json.dumps({"orders": batch_body}),
                            timeout=15,
                        )
                    except Exception as e:
                        console.print(f"  [red]Batch {bi+1} error: {e} — falling back[/red]")
                        r = None

                    if r is not None and r.status_code in (200, 201):
                        results = r.json().get("orders", [])
                        ok = 0
                        for j, item in enumerate(results):
                            if j >= len(chunk):
                                break
                            err = item.get("error")
                            if err:
                                console.print(
                                    f"  [yellow]Batch {bi+1} item err:"
                                    f" {err.get('message', err)}[/yellow]"
                                )
                                continue
                            order = item.get("order") or {}
                            oid   = order.get("order_id", "")
                            if oid:
                                spec = chunk[j]
                                states[spec["sym"]].levels[spec["price"]][spec["side"]] = oid
                                placed += 1
                                ok     += 1
                        console.print(
                            f"  [dim]Batch {bi+1}/{(len(order_specs)+BATCH-1)//BATCH}:"
                            f" {ok}/{len(chunk)} placed[/dim]"
                        )
                    else:
                        # Fallback: individual placement for this chunk
                        status = r.status_code if r is not None else "err"
                        console.print(
                            f"  [yellow]Batch {bi+1} {status} — individual fallback[/yellow]"
                        )
                        for spec in chunk:
                            oid = await _place_buy(
                                client, spec["ticker"], spec["side"], spec["price"],
                                spec["shares"], f"{spec['sym']}@{spec['price']:.2f}",
                                spec["coid"],
                            )
                            if oid:
                                states[spec["sym"]].levels[spec["price"]][spec["side"]] = oid
                                placed += 1
                            await asyncio.sleep(0.15)

                    if i + BATCH < len(order_specs):
                        await asyncio.sleep(3)

                console.print(f"  [dim]{placed}/{len(order_specs)} orders placed.[/dim]")

            placement_task = asyncio.create_task(place_all())

            # ── Every-minute status + rebalance at minute 12 ─────────────
            async def status_and_rebal() -> None:
                rebal_fired = False
                while True:
                    await asyncio.sleep(60)
                    ts_r      = datetime.now().strftime("%H:%M:%S")
                    remaining = deadline - asyncio.get_event_loop().time()

                    # ── Status ────────────────────────────────────────────
                    for sym_r, st_r in states.items():
                        yes_ps = [p for p, lv_ in st_r.levels.items() if lv_.get("yes_filled")]
                        no_ps  = [p for p, lv_ in st_r.levels.items() if lv_.get("no_filled")]
                        low_y  = min(yes_ps) if yes_ps else None
                        low_n  = min(no_ps)  if no_ps  else None

                        if low_y is not None and (low_n is None or low_y < low_n):
                            dominant = f"YES@{low_y:.2f}"
                            expect   = "YES"
                        elif low_n is not None:
                            dominant = f"NO@{low_n:.2f}"
                            expect   = "NO"
                        else:
                            dominant = "–"
                            expect   = "–"

                        console.print(
                            f"  [dim][{ts_r}] {sym_r}"
                            f"  low={dominant}  expect={expect}"
                            f"  yes={len(yes_ps)}ct  no={len(no_ps)}ct"
                            f"  {remaining:.0f}s left[/dim]"
                        )

                    # ── Rebalance at minute 12 (≤180s remaining) ─────────
                    if not rebal_fired and remaining <= 180:
                        rebal_fired = True
                        console.print(f"  [bold magenta][{ts_r}] Minute-12 rebalance[/bold magenta]")
                        for sym_r, st_r in states.items():
                            for side_r in ("yes", "no"):
                                other_r   = "no" if side_r == "yes" else "yes"
                                filled_ps = [p for p, lv_ in st_r.levels.items()
                                             if lv_.get(f"{side_r}_filled")]
                                if not filled_ps:
                                    continue
                                lowest_p = min(filled_ps)
                                n = int(round((st_r.cfg["high_limit"] - lowest_p) * 100))
                                if n <= 0:
                                    continue
                                other_count = sum(
                                    1 for lv_ in st_r.levels.values()
                                    if lv_.get(f"{other_r}_filled")
                                )
                                console.print(
                                    f"  [magenta]{sym_r}"
                                    f"  BUY {side_r.upper()} {n}ct @ {lowest_p:.2f}"
                                    f"  SELL {other_r.upper()} {other_count}ct"
                                    f" @ {1.0-lowest_p:.2f}[/magenta]"
                                )
                                oid = await _place_buy(
                                    client, st_r.ticker, side_r, lowest_p, n,
                                    f"{sym_r}_RB"
                                )
                                if oid:
                                    st_r.rebal_buy_oids.append(oid)
                                if other_count > 0:
                                    oid = await _place_sell(
                                        client, st_r.ticker, other_r,
                                        round(1.0 - lowest_p, 2), other_count,
                                        f"{sym_r}_SQ"
                                    )
                                    if oid:
                                        st_r.sell_oids.append(oid)

            rebal_task = asyncio.create_task(status_and_rebal())

            # ── 10s cancel buffer ─────────────────────────────────────────
            cancel_buffer: set[str] = set()
            flush_task: asyncio.Task | None = None

            async def flush_cancels():
                await asyncio.sleep(CANCEL_DELAY)
                if cancel_buffer:
                    # Skip any oids that already filled (both sides hit = they settled)
                    filled_oids = {
                        lv[side]
                        for st in states.values()
                        for lv in st.levels.values()
                        for side in ("yes", "no")
                        if lv.get(f"{side}_filled") and lv.get(side)
                    }
                    oids = [o for o in cancel_buffer if o not in filled_oids]
                    cancel_buffer.clear()
                    if oids:
                        console.print(f"  [dim]10s flush: cancelling {len(oids)} other-side orders…[/dim]")
                        await _cancel_batch(client, oids, "10s-flush")

            # ── Main event loop ───────────────────────────────────────────
            deadline      = asyncio.get_event_loop().time() + 895
            next_tickers: dict[str, tuple[str, datetime]] = {}
            prefetch_done = False

            while asyncio.get_event_loop().time() < deadline:
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
                        console.print(f"  [dim]  {sym}: {nt}[/dim]")
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
                coid  = data.get("client_order_id", "")
                count = int(float(data.get("count_fp") or 0))
                if count <= 0 or coid not in coid_map:
                    continue

                sym, price, side = coid_map[coid]
                st  = states[sym]
                lv  = st.levels[price]
                ts  = datetime.now().strftime("%H:%M:%S")

                # Accumulate partial fills
                fill_key = f"{side}_filled_count"
                lv[fill_key] = lv.get(fill_key, 0) + count
                fully_filled = lv[fill_key] >= st.cfg["shares"]
                lv[f"{side}_filled"] = fully_filled

                if not fully_filled:
                    console.print(
                        f"  [dim][{ts}] {sym}@{price:.2f}  partial {side.upper()}"
                        f"  {lv[fill_key]}/{st.cfg['shares']}ct[/dim]"
                    )
                    continue

                # Fully filled — queue other side for cancel
                other_side = "no" if side == "yes" else "yes"
                other_oid  = lv.get(other_side)
                already_filled = lv.get(f"{other_side}_filled", False)

                if already_filled:
                    console.print(
                        f"  [bold yellow][{ts}] {sym}@{price:.2f}  BOTH FILLED"
                        f"  +${1.0 - price*2:.2f} locked[/bold yellow]"
                    )
                else:
                    console.print(
                        f"  [bold green][{ts}] {sym}@{price:.2f}  FILLED {side.upper()}"
                        f"  {count}ct[/bold green]  → queuing {other_side.upper()} cancel"
                    )

                st.fills.append({"price": price, "side": side, "count": count,
                                  "locked": already_filled})

                if other_oid and not already_filled:
                    cancel_buffer.add(other_oid)
                    if flush_task is None or flush_task.done():
                        flush_task = asyncio.create_task(flush_cancels())

            placement_task.cancel()
            if flush_task and not flush_task.done():
                flush_task.cancel()
            rebal_task.cancel()

    except asyncio.CancelledError:
        console.print("\n  [yellow]Interrupted — cancelling open orders…[/yellow]")
        unfilled = [
            lv[side]
            for st in states.values()
            for lv in st.levels.values()
            for side in ("yes", "no")
            if not lv.get(f"{side}_filled") and lv.get(side)
        ] + [
            oid
            for st in states.values()
            for oid in st.rebal_buy_oids + st.sell_oids
        ]
        if unfilled:
            await _cancel_batch(client, unfilled, "interrupt")
        raise

    # ── End-of-window: cancel all remaining unfilled orders ───────────────────
    unfilled = [
        lv[side]
        for st in states.values()
        for lv in st.levels.values()
        for side in ("yes", "no")
        if not lv.get(f"{side}_filled") and lv.get(side)
    ] + [
        oid
        for st in states.values()
        for oid in st.rebal_buy_oids + st.sell_oids
    ]
    if unfilled:
        console.print(f"  [dim]Cancelling {len(unfilled)} unfilled orders…[/dim]")
        await _cancel_batch(client, unfilled, "cleanup")

    # ── Window summary ────────────────────────────────────────────────────────
    console.print(f"\n  [bold]Window {window_num} summary:[/bold]")
    for sym, st in states.items():
        locks   = [f for f in st.fills if f.get("locked")]
        singles = [f for f in st.fills if not f.get("locked")]
        console.print(f"    {sym}  {len(locks)} locks  {len(singles)} singles")
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

    return next_tickers


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    active = {sym: cfg for sym, cfg in COINS.items() if cfg["enabled"]}
    console.print("[bold cyan]Expire Maker[/bold cyan]")
    console.print(
        f"Strategy: batch place YES+NO → fill detected → queue other side for"
        f" {CANCEL_DELAY}s batch cancel\n"
    )
    for sym, cfg in active.items():
        n = int(cfg["high_limit"] * 100) - int(cfg["low_limit"] * 100) + 1
        console.print(
            f"  {sym}  {cfg['low_limit']:.2f}–{cfg['high_limit']:.2f}"
            f"  {n} levels × 2 sides = {n*2} orders  shares={cfg['shares']}"
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
