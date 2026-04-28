#!/usr/bin/env python3
"""
kalshi_predrift_strategy.py — pre-window drift directional strategy.

Hypothesis (validated on 87-window backtest): when |drift| from T-120s to
window-open spot exceeds 0.02%, the drift-aligned side wins ~81% of the time
and the first observable Kalshi ask is often mispriced (Kalshi's MMs haven't
fully caught up). Taking that opening ask and holding to settlement produced
+22% return per stake in backtest.

This runs alongside kalshi_momentum_live.py without touching it. Separate
process, separate log, separate config.

Strategy per window:
  1. 30 seconds BEFORE window opens: pre-fetch Coinbase historical 1-min
     candle at window_start_ts - 120s (cached in memory for fast access).
  2. At window_start, compute:
       drift = (live_spot_now - prefetched_hist) / prefetched_hist
  3. If |drift| >= MIN_DRIFT_PCT:
       side = yes if drift > 0 else no
  4. Poll Kalshi asks every 200ms until book is ready (typically <500ms).
  5. Place aggressive limit buy on the drift-aligned side.
  6. Hold to settlement, log P&L to ~/.predrift_trades.jsonl

Run:   python3 kalshi_predrift_strategy.py
       STATUS_INTERVAL=1 MIN_DRIFT=0.02 python3 kalshi_predrift_strategy.py
"""
import asyncio
import base64
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import ccxt
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
    "BTC": dict(enabled=True,  series="KXBTC15M", cb_product="BTC-USD"),
    "ETH": dict(enabled=True,  series="KXETH15M", cb_product="ETH-USD"),
    "SOL": dict(enabled=True,  series="KXSOL15M", cb_product="SOL-USD"),
    # XRP disabled — small strikes make pre-drift noisy, -12% in backtest
    "XRP": dict(enabled=False, series="KXXRP15M", cb_product="XRP-USD"),
}

SHARES           = 1          # fixed small size — experiment
MIN_DRIFT_PCT    = float(os.environ.get("MIN_DRIFT", "0.02"))  # |drift| threshold
MAX_ENTRY_PRICE  = 0.85       # skip if ask is already this rich
MIN_ENTRY_PRICE  = 0.05       # skip if ask is this cheap (book not ready)
LIMIT_BUFFER     = 0.02       # place limit at ask + buffer to guarantee fill
PREFETCH_LEAD_S  = 30         # start pre-fetching the next window's historical
                              # price this many seconds BEFORE it opens
ASK_POLL_INTERVAL = 0.2       # how frequently to poll Kalshi asks at window open
ASK_MAX_WAIT_S   = 5.0        # give up after this long if book never forms
DRIFT_LOOKBACK_S = 120        # 2 min — matches Kalshi CFB internal reference

TRADE_LOG = Path.home() / ".predrift_trades.jsonl"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
ET_OFFSET   = timedelta(hours=-4)

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

_cb_historical = ccxt.coinbase()


# ── Auth ──────────────────────────────────────────────────────────────────

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


# ── Live spot prices from Coinbase WS ─────────────────────────────────────

live_prices: dict[str, float] = {}

async def _coinbase_ws_feed() -> None:
    products        = [cfg["cb_product"] for cfg in COINS.values() if cfg["enabled"]]
    product_to_coin = {cfg["cb_product"]: coin
                       for coin, cfg in COINS.items() if cfg["enabled"]}
    while True:
        try:
            async with websockets.connect(
                COINBASE_WS, ping_interval=20, open_timeout=10,
            ) as ws:
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
                                live_prices[sym] = px
        except Exception as e:
            console.print(f"[yellow]Coinbase WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)


# ── Historical price fetch (for T-120 reference) ──────────────────────────

async def _fetch_historical(cb_product: str, ts: int) -> float | None:
    """Fetch Coinbase 1-min candle open at unix second ts."""
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


# ── Kalshi REST helpers ───────────────────────────────────────────────────

async def _poll_asks_once(client: httpx.AsyncClient, ticker: str) -> tuple[float, float]:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            m   = r.json().get("market", {})
            ya  = float(m.get("yes_ask_dollars") or 0)
            yb  = float(m.get("yes_bid_dollars") or 0)
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


async def _place_buy(client: httpx.AsyncClient, ticker: str, side: str,
                     price: float, shares: int, tag: str) -> dict | None:
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
            return {"order_id": oid, "client_order_id": coid}
        console.print(f"  [red]{tag}  order failed {r.status_code}: {r.text[:150]}[/red]")
    except Exception as e:
        console.print(f"  [red]{tag}  order error: {e}[/red]")
    return None


# ── Ticker + timing helpers ───────────────────────────────────────────────

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


def _log_trade(entry: dict) -> None:
    try:
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        console.print(f"[yellow]trade log write: {e}[/yellow]")


# ── Session stats ─────────────────────────────────────────────────────────

session = {"fires": 0, "skips": 0, "wins": 0, "losses": 0,
           "pending": 0, "gross_pnl": 0.0}


# ── Per-window runner ─────────────────────────────────────────────────────

async def run_window(client: httpx.AsyncClient, window_num: int,
                     window_start_utc: datetime,
                     prefetched_hist: dict[str, float | None] | None = None) -> None:
    close_utc       = window_start_utc + timedelta(minutes=15)
    window_start_ts = int(window_start_utc.timestamp())
    label           = (window_start_utc + ET_OFFSET).strftime("%H:%M ET")

    console.rule(f"[bold cyan]━━ Window {window_num}  {label} ━━[/bold cyan]")

    # Resolve tickers per enabled coin
    tickers: dict[str, str] = {}
    for sym, cfg in COINS.items():
        if not cfg["enabled"]:
            continue
        tickers[sym] = _ticker_for(cfg["series"], close_utc)
        console.print(f"  [dim]{sym}[/dim]  {tickers[sym]}")

    # Historical prices: either pre-fetched (fast path) or fetch now (fallback)
    hist_prices: dict[str, float | None] = dict(prefetched_hist or {})
    need_fetch = [s for s in tickers if hist_prices.get(s) is None]
    if need_fetch:
        console.print(f"  [dim]Fetching historical for {need_fetch} (not pre-fetched)[/dim]")
        async def _fetch_one(sym: str) -> None:
            cb = COINS[sym]["cb_product"]
            hist_prices[sym] = await _fetch_historical(
                cb, window_start_ts - DRIFT_LOOKBACK_S
            )
        await asyncio.gather(*[_fetch_one(s) for s in need_fetch])

    for sym, px in hist_prices.items():
        if px:
            console.print(f"  [dim]{sym} hist T-{DRIFT_LOOKBACK_S}s = ${px:,.2f}[/dim]")

    # Wait until window actually opens (might already be past if we entered late)
    while time.time() < window_start_ts:
        await asyncio.sleep(0.05)

    now_ts_print = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    console.print(f"\n[bold]  [{now_ts_print}] window open — computing drift per coin[/bold]")

    trades_placed: list[dict] = []

    async def _handle(sym: str) -> None:
        ticker  = tickers[sym]
        hist_px = hist_prices.get(sym)
        now_px  = live_prices.get(sym)
        if not (hist_px and now_px):
            console.print(f"  [yellow]{sym}  no price data (hist={hist_px}, "
                          f"now={now_px}) — skip[/yellow]")
            session["skips"] += 1
            return

        drift = (now_px - hist_px) / hist_px * 100.0

        if abs(drift) < MIN_DRIFT_PCT:
            console.print(f"  [dim]{sym}  hist=${hist_px:,.2f} now=${now_px:,.2f} "
                          f"drift={drift:+.3f}% < {MIN_DRIFT_PCT}% — skip[/dim]")
            session["skips"] += 1
            return

        side = "yes" if drift > 0 else "no"

        # Poll Kalshi asks until the book forms (first valid tick), up to ASK_MAX_WAIT_S
        ya, na = 0.0, 0.0
        deadline = time.time() + ASK_MAX_WAIT_S
        poll_count = 0
        while time.time() < deadline:
            ya, na = await _poll_asks_once(client, ticker)
            poll_count += 1
            if (0 < ya < 1) and (0 < na < 1):
                break
            await asyncio.sleep(ASK_POLL_INTERVAL)

        cur_ask = ya if side == "yes" else na
        if cur_ask <= 0 or cur_ask >= 1:
            console.print(f"  [yellow]{sym}  drift={drift:+.3f}% but no valid ask "
                          f"after {poll_count} polls — skip[/yellow]")
            session["skips"] += 1
            return
        if cur_ask < MIN_ENTRY_PRICE or cur_ask > MAX_ENTRY_PRICE:
            console.print(f"  [yellow]{sym}  drift={drift:+.3f}% but "
                          f"{side} ask={cur_ask:.2f} out of range "
                          f"[{MIN_ENTRY_PRICE},{MAX_ENTRY_PRICE}] — skip[/yellow]")
            session["skips"] += 1
            return

        limit_px = round(min(cur_ask + LIMIT_BUFFER, MAX_ENTRY_PRICE), 2)
        elapsed = time.time() - window_start_ts
        console.print(
            f"  [bold cyan]{sym}  T+{elapsed:.2f}s  "
            f"hist=${hist_px:,.2f} now=${now_px:,.2f}  "
            f"drift={drift:+.3f}% → buy {side.upper()} @ {limit_px:.2f} "
            f"(ask={cur_ask:.2f}, {poll_count} polls)[/bold cyan]"
        )

        order = await _place_buy(client, ticker, side, limit_px, SHARES,
                                 f"{sym}@pre-drift")
        if order:
            trades_placed.append({
                "sym":            sym,
                "ticker":         ticker,
                "side":           side,
                "drift_pct":      drift,
                "hist_px":        hist_px,
                "now_px":         now_px,
                "ask_at_trigger": cur_ask,
                "limit_px":       limit_px,
                "order_id":       order["order_id"],
                "placed_at":      datetime.now().isoformat(),
                "elapsed_sec":    elapsed,
            })
            session["fires"]   += 1
            session["pending"] += 1

    await asyncio.gather(*[_handle(sym) for sym in tickers])

    # Wait until window close + small buffer for final result
    close_ts = window_start_ts + 895
    while time.time() < close_ts:
        await asyncio.sleep(5)
    await asyncio.sleep(8)

    # Resolve + log
    if not trades_placed:
        console.print(f"\n  [dim]No trades placed this window.[/dim]")
        return

    console.print(f"\n[bold]  Window {window_num} results[/bold]")
    for t in trades_placed:
        result = await _fetch_result(client, t["ticker"])
        entry  = t["ask_at_trigger"]
        side   = t["side"]
        sym    = t["sym"]
        pnl    = None
        won    = None

        if result in ("yes", "no"):
            won = (side == result)
            pnl = (1.0 - entry) * SHARES if won else -entry * SHARES
            mark = "[bold green]WIN[/bold green]" if won else "[bold red]LOSS[/bold red]"
            console.print(
                f"    {sym}  {side.upper()}@{entry:.2f}  result={result.upper()}  "
                f"{mark}  P&L ${pnl:+.2f}"
            )
            session["pending"]   -= 1
            session["wins"]      += int(won)
            session["losses"]    += int(not won)
            session["gross_pnl"] = round(session["gross_pnl"] + pnl, 4)
        else:
            console.print(f"    {sym}  {side.upper()}@{entry:.2f}  [yellow]result=?[/yellow]")

        _log_trade({
            "ts_utc":          datetime.now(timezone.utc).isoformat(),
            "window":          t["ticker"],
            "coin":            sym,
            "window_start_ts": window_start_ts,
            "side":            side,
            "drift_pct":       round(t["drift_pct"], 4),
            "hist_px":         t["hist_px"],
            "now_px":          t["now_px"],
            "ask_at_trigger":  entry,
            "limit_px_sent":   t["limit_px"],
            "shares":          SHARES,
            "order_id":        t["order_id"],
            "winner":          result,
            "won":             won,
            "pnl":             round(pnl, 4) if pnl is not None else None,
        })

    s = session
    if s["wins"] + s["losses"] > 0:
        wr = s["wins"] / (s["wins"] + s["losses"]) * 100
        col = "green" if s["gross_pnl"] >= 0 else "red"
        console.print(
            f"\n  [bold]Session[/bold]  fires {s['fires']}  skips {s['skips']}  "
            f"wins {s['wins']}  losses {s['losses']}  "
            f"win% {wr:.0f}%  "
            f"gross P&L [{col}]${s['gross_pnl']:+.2f}[/{col}]"
        )


# ── Main ──────────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]Kalshi Pre-Drift Strategy[/bold cyan]")
    console.print(
        f"  at window open: compute drift over past {DRIFT_LOOKBACK_S}s,"
        f" fire {SHARES}-share if |drift| ≥ {MIN_DRIFT_PCT}%"
    )
    console.print(
        f"  pre-fetch T-{DRIFT_LOOKBACK_S}s historical {PREFETCH_LEAD_S}s "
        f"before window open; poll Kalshi asks every {ASK_POLL_INTERVAL*1000:.0f}ms "
        f"(max {ASK_MAX_WAIT_S:.0f}s) until book is ready"
    )
    console.print(f"  entry range: [{MIN_ENTRY_PRICE:.2f}, {MAX_ENTRY_PRICE:.2f}]  "
                  f"limit at ask + {LIMIT_BUFFER:.2f}")
    console.print(f"  enabled: "
                  f"{', '.join(s for s,c in COINS.items() if c['enabled'])}")
    console.print(f"  trade log: {TRADE_LOG}\n")

    async with httpx.AsyncClient() as client:
        asyncio.create_task(_coinbase_ws_feed())

        # Warm up WS
        console.print("[dim]Warming up Coinbase WS…[/dim]")
        for _ in range(15):
            if all(live_prices.get(s) for s in COINS if COINS[s]["enabled"]):
                break
            await asyncio.sleep(0.5)
        snap = "  ".join(f"{s}=${p:,.2f}" for s, p in live_prices.items())
        console.print(f"[dim]Initial prices: {snap}[/dim]\n")

        # Wait for next clean window boundary — don't fire mid-window
        wait_secs = _seconds_until_next_window()
        if wait_secs > 2:
            open_wall = datetime.now() + timedelta(seconds=wait_secs)
            console.print(
                f"[dim]Waiting {wait_secs:.0f}s for next window boundary "
                f"({open_wall.strftime('%H:%M:%S')}) …[/dim]"
            )
            await asyncio.sleep(wait_secs + 0.2)

        # Pre-fetch helper — returns historical T-120 spot for each enabled coin.
        async def _prefetch(target_window_start_ts: int) -> dict[str, float | None]:
            tasks = {}
            for sym, cfg in COINS.items():
                if not cfg["enabled"]:
                    continue
                tasks[sym] = asyncio.create_task(
                    _fetch_historical(cfg["cb_product"],
                                      target_window_start_ts - DRIFT_LOOKBACK_S)
                )
            return {sym: await t for sym, t in tasks.items()}

        # Pre-fetch for the FIRST window (we're already at its boundary after
        # the sleep above). Subsequent windows get their pre-fetch via the
        # scheduler task below, which runs PREFETCH_LEAD_S before window open.
        prefetched: dict[str, float | None] = await _prefetch(
            int(_current_window_start_utc().timestamp())
        )

        window_num = 1
        while True:
            window_start_utc = _current_window_start_utc()
            window_start_ts  = int(window_start_utc.timestamp())

            try:
                await run_window(client, window_num, window_start_utc, prefetched)
            except Exception as e:
                console.print(f"[red]window {window_num} crashed: {e}[/red]")
                await asyncio.sleep(2)

            window_num += 1

            # Pre-fetch for the NEXT window. Schedule the fetch to run at
            # PREFETCH_LEAD_S before the next window opens, so the historical
            # candle is ready the instant the window flips.
            next_window_ts = window_start_ts + 900
            trigger_time   = next_window_ts - PREFETCH_LEAD_S
            wait_sec       = trigger_time - time.time()
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)

            try:
                prefetched = await asyncio.wait_for(
                    _prefetch(next_window_ts), timeout=10
                )
                ready_at = time.time() - next_window_ts
                console.print(f"  [dim]Pre-fetched next window "
                              f"(ready at T{ready_at:+.1f}s relative to window open)[/dim]")
            except (asyncio.TimeoutError, Exception) as e:
                console.print(f"  [yellow]Pre-fetch for next window failed: {e}[/yellow]")
                prefetched = {}


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
