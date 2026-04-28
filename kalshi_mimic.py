#!/usr/bin/env python3
"""
Kalshi Mimic — shadow-follows Idolized-Scallops bot trades on Polymarket
and shows the equivalent Kalshi market odds in real time.

No real orders placed — pure observation / paper tracking.

Detection path:
  Polygon WS  →  instant trigger when bot wallet has on-chain activity
  Polymarket data API  →  fetch trade details (with retry for indexing lag)
  Baseline poll        →  every 30s in case WS misses anything

For each bot trade batch detected:
  - Side, total $, shares, avg price, detection lag
  - Current Kalshi YES ask / NO ask for the equivalent market

Run: python3 kalshi_mimic.py
Stop: Ctrl-C
"""
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

import httpx
import websockets
from rich.console import Console
from rich.table import Table

console = Console()

# ── Constants ──────────────────────────────────────────────────────────────────
SHADOW_WALLET = "0xe1d6b51521bd4365769199f392f9818661bd907c"   # Idolized-Scallops
_SHADOW_TOPIC = "0x000000000000000000000000" + SHADOW_WALLET[2:]

TRADES_API  = "https://data-api.polymarket.com/trades"
KALSHI_API  = "https://api.elections.kalshi.com/trade-api/v2"
POLYGON_WS  = "wss://rpc-mainnet.matic.quiknode.pro"

POLL_INTERVAL   = 30    # seconds between baseline polls
MAX_RETRY_SECS  = 120   # how long to retry after Polygon trigger
RETRY_INTERVAL  = 10    # seconds between retries

# ── Polymarket slug → Kalshi series mapping ────────────────────────────────────
# Polymarket slugs include a window timestamp suffix, e.g. "btc-updown-15m-1775529000".
# We match by prefix so any window's trades are caught.
# Series ticker is the Kalshi series prefix — active market discovered via REST.
#
# Kalshi 4-hour BTC: no dedicated series found as of Apr 2026 — mapped to KXBTCD
# (daily Bitcoin above/below) as the closest equivalent; confirmed at runtime.
POLY_PREFIXES: list[tuple[str, dict]] = [
    ("btc-updown-15m",        {"series": "KXBTC15M", "coin": "BTC", "window": "15m"}),
    ("eth-updown-15m",        {"series": "KXETH15M", "coin": "ETH", "window": "15m"}),
    ("sol-updown-15m",        {"series": "KXSOL15M", "coin": "SOL", "window": "15m"}),
    ("xrp-updown-15m",        {"series": "KXXRP15M", "coin": "XRP", "window": "15m"}),
    # Daily / evening BTC windows — "bitcoin-up-or-down-<date>-<time>-et"
    ("bitcoin-up-or-down-",   {"series": "KXBTCD",   "coin": "BTC", "window": "daily"}),
]

def _slug_meta(slug: str) -> dict | None:
    """Return metadata for a Polymarket slug, matching by prefix."""
    for prefix, meta in POLY_PREFIXES:
        if slug.startswith(prefix):
            return meta
    return None

# Convenience set of all series we care about
ALL_SERIES = {meta["series"] for _, meta in POLY_PREFIXES}

# ── State ──────────────────────────────────────────────────────────────────────
seen_trade_ids: set[str]        = set()
kalshi_tickers: dict[str, str]  = {}   # series → active market ticker
_trigger = asyncio.Event()

# Cumulative Polymarket position per slug (across all batches in a window)
# slug → {up_sh, dn_sh, up_cost, dn_cost, count}
poly_pos: dict[str, dict] = {}

# Kalshi mimic position per slug — what we've already "bought" to match previous batches
# slug → {side: "YES"|"NO"|None, sh: float}
mimic_pos: dict[str, dict] = {}

# Running totals per slug (for summary table)
totals: dict[str, dict] = {}   # slug → {up, dn, count}


# ── Kalshi helpers ─────────────────────────────────────────────────────────────

async def _find_kalshi_ticker(client: httpx.AsyncClient, series: str) -> str | None:
    """Return the soonest-closing open Kalshi market ticker for a series."""
    try:
        r = await client.get(
            f"{KALSHI_API}/markets",
            params={"series_ticker": series, "status": "open", "limit": 10},
            timeout=10,
        )
        markets = r.json().get("markets", [])
        if markets:
            by_close = sorted(markets, key=lambda m: m.get("close_time", ""))
            return by_close[0]["ticker"]
    except Exception as e:
        console.print(f"[dim yellow]Kalshi series lookup {series}: {e}[/dim yellow]")
    return None


async def _get_kalshi_odds(client: httpx.AsyncClient, ticker: str) -> dict | None:
    """Fetch current YES/NO odds for a Kalshi market ticker."""
    try:
        r = await client.get(f"{KALSHI_API}/markets/{ticker}", timeout=10)
        m = r.json().get("market", {})
        # Kalshi REST returns dollar values in yes_ask_dollars / no_ask_dollars
        ya = float(m.get("yes_ask_dollars") or 0)
        na = float(m.get("no_ask_dollars")  or 0)
        yb = float(m.get("yes_bid_dollars") or 0)
        nb = float(m.get("no_bid_dollars")  or 0)
        if ya <= 0 and na <= 0:
            return None
        return {
            "ticker":   ticker,
            "yes_ask":  ya,
            "no_ask":   na,
            "yes_bid":  yb,
            "no_bid":   nb,
        }
    except Exception as e:
        console.print(f"[dim yellow]Kalshi odds {ticker}: {e}[/dim yellow]")
    return None


async def _refresh_tickers(client: httpx.AsyncClient) -> None:
    """Refresh the series → ticker cache for all tracked series."""
    for series in ALL_SERIES:
        ticker = await _find_kalshi_ticker(client, series)
        if ticker:
            kalshi_tickers[series] = ticker


# ── Polymarket helpers ─────────────────────────────────────────────────────────

async def _fetch_trades(client: httpx.AsyncClient) -> list[dict]:
    try:
        r = await client.get(
            TRADES_API,
            params={"user": SHADOW_WALLET, "limit": 200},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        console.print(f"[dim yellow]Trades API: {e}[/dim yellow]")
        return []


async def _process_trades(client: httpx.AsyncClient, raw: list[dict]) -> int:
    """Check raw trades for new ones; print and track any found. Returns count added."""
    new_by_slug: dict[str, list[dict]] = {}

    for t in raw:
        slug = t.get("slug", "")
        if not slug or t.get("side") != "BUY":
            continue
        if _slug_meta(slug) is None:
            continue

        tx_id = t.get("transactionHash") or t.get("id") or ""
        if not tx_id or tx_id in seen_trade_ids:
            continue
        seen_trade_ids.add(tx_id)

        price  = float(t.get("price")  or 0)
        size   = float(t.get("size")   or 0)
        side   = t.get("outcome", "").strip()
        ts_raw = float(t.get("timestamp") or 0)

        trade = {
            "side":   side,
            "price":  price,
            "shares": size,
            "cost":   price * size,
            "ts":     ts_raw,
            "ts_str": datetime.fromtimestamp(ts_raw).strftime("%H:%M:%S") if ts_raw else "?",
        }
        new_by_slug.setdefault(slug, []).append(trade)

    added = 0
    for slug, new_trades in new_by_slug.items():
        meta   = _slug_meta(slug) or {}
        series = meta.get("series", "")

        # Ensure we have a Kalshi ticker
        if series not in kalshi_tickers:
            ticker = await _find_kalshi_ticker(client, series)
            if ticker:
                kalshi_tickers[series] = ticker

        ticker = kalshi_tickers.get(series)
        odds   = await _get_kalshi_odds(client, ticker) if ticker else None

        _update_poly_pos(slug, new_trades)   # update cumulative BEFORE printing
        _print_bot_trade(slug, meta, new_trades, odds)
        added += len(new_trades)

    return added


def _net_from_poly(up_sh: float, dn_sh: float, up_avg: float, dn_avg: float
                   ) -> tuple[str | None, float, float]:
    """Return (side, net_shares, net_avg_price) for a Poly position.
    Matched pairs cancel; remainder is the directional exposure."""
    matched   = min(up_sh, dn_sh)
    net_up_sh = up_sh - matched
    net_dn_sh = dn_sh - matched
    if net_up_sh > 0:
        return "Up",   net_up_sh, up_avg
    if net_dn_sh > 0:
        return "Down", net_dn_sh, dn_avg
    return None, 0.0, 0.0


def _print_bot_trade(slug: str, meta: dict, trades: list[dict], odds: dict | None) -> None:
    """Render a detected bot trade batch to the console, including the Kalshi delta action."""
    coin   = meta["coin"]
    window = meta["window"]
    now    = datetime.now().strftime("%H:%M:%S")

    # ── This batch ────────────────────────────────────────────────────────────
    agg: dict[str, dict] = {}
    for t in trades:
        s = t["side"]
        if s not in agg:
            agg[s] = {"dollars": 0.0, "shares": 0.0, "count": 0}
        agg[s]["dollars"] += t["cost"]
        agg[s]["shares"]  += t["shares"]
        agg[s]["count"]   += 1

    earliest = min(t["ts"] for t in trades if t["ts"] > 0) if trades else 0
    lag = int(time.time() - earliest) if earliest > 0 else 0
    batch_num = poly_pos.get(slug, {}).get("batch", 0)

    console.print(f"\n[bold cyan]━━ BOT TRADE ━━[/bold cyan]  "
                  f"[bold]{coin} {window}[/bold]  {now}  "
                  f"[dim]lag={lag}s  batch #{batch_num}  slug={slug[-30:]}[/dim]")

    for side, a in agg.items():
        color  = "green" if side == "Up" else "red"
        avg_px = a["dollars"] / a["shares"] if a["shares"] > 0 else 0
        console.print(
            f"  [{color}]{side:<4}[/{color}]  "
            f"[bold]${a['dollars']:.1f}[/bold]  "
            f"{a['shares']:.0f} sh  "
            f"avg={avg_px:.3f}  "
            f"({a['count']} fill{'s' if a['count']>1 else ''})"
        )

    # ── Cumulative Poly position (already updated before this call) ───────────
    pos = poly_pos.get(slug, {})
    cum_up_sh  = pos.get("up_sh",   0.0)
    cum_dn_sh  = pos.get("dn_sh",   0.0)
    cum_up_avg = pos.get("up_cost", 0.0) / cum_up_sh if cum_up_sh > 0 else 0.0
    cum_dn_avg = pos.get("dn_cost", 0.0) / cum_dn_sh if cum_dn_sh > 0 else 0.0

    cum_side, cum_net_sh, _ = _net_from_poly(
        cum_up_sh, cum_dn_sh, cum_up_avg, cum_dn_avg
    )

    if batch_num > 1:
        cum_color = "green" if cum_side == "Up" else "red" if cum_side else "dim"
        cum_label = f"[{cum_color}]{cum_side or 'flat'}[/{cum_color}]"
        console.print(
            f"  Cumulative Poly → {cum_label}  "
            f"[bold]{cum_net_sh:.0f} sh[/bold]  "
            f"(Up {cum_up_sh:.0f}sh  Down {cum_dn_sh:.0f}sh)"
        )

    # ── Kalshi delta: what to add to match the new cumulative net ─────────────
    # mimic_pos tracks what we've already bought on Kalshi from prior batches.
    mp = mimic_pos.get(slug, {"side": None, "sh": 0.0})
    prev_mimic_side = mp["side"]
    prev_mimic_sh   = mp["sh"]

    if odds:
        ya = odds["yes_ask"]
        na = odds["no_ask"]
        console.print(
            f"  Kalshi [{odds['ticker']}]  "
            f"YES ask=[green]{ya:.2f}[/green]  "
            f"NO  ask=[red]{na:.2f}[/red]"
        )

        # Map Poly side → Kalshi side
        ks_side = "YES" if cum_side == "Up" else "NO" if cum_side == "Down" else None
        ks_ask  = ya if ks_side == "YES" else na if ks_side == "NO" else 0.0

        if cum_side is None:
            # Flat — cancel any mimic position
            if prev_mimic_sh > 0:
                console.print(
                    f"  → [yellow]Position now flat — sell {prev_mimic_sh:.0f} "
                    f"{prev_mimic_side} on Kalshi[/yellow]"
                )
            else:
                console.print("  → [dim]Position flat — nothing to do[/dim]")
        elif ks_side == prev_mimic_side or prev_mimic_side is None:
            # Same direction — just top up
            delta_sh   = cum_net_sh - prev_mimic_sh
            delta_cost = delta_sh * ks_ask
            color      = "green" if ks_side == "YES" else "red"
            if delta_sh > 0.5:
                console.print(
                    f"  → Add [{color}]{delta_sh:.0f} {ks_side}[/{color}] "
                    f"@ {ks_ask:.2f}  =  [bold]${delta_cost:.1f}[/bold]  "
                    f"[dim](already have {prev_mimic_sh:.0f}, target {cum_net_sh:.0f})[/dim]"
                )
            else:
                console.print(
                    f"  → [dim]Already matched  ({prev_mimic_sh:.0f} {ks_side} held)[/dim]"
                )
        else:
            # Direction flipped — unwind old side, open new
            new_cost = cum_net_sh * ks_ask
            color    = "green" if ks_side == "YES" else "red"
            console.print(
                f"  → [yellow]FLIP[/yellow]  sell {prev_mimic_sh:.0f} {prev_mimic_side}  "
                f"then buy [{color}]{cum_net_sh:.0f} {ks_side}[/{color}] "
                f"@ {ks_ask:.2f}  =  [bold]${new_cost:.1f}[/bold]"
            )

        # Update mimic_pos to reflect what we would hold after acting
        mimic_pos[slug] = {"side": ks_side, "sh": cum_net_sh}

    else:
        series = meta.get("series", "?")
        console.print(f"  [yellow]No open Kalshi market found for series {series}[/yellow]")


def _update_poly_pos(slug: str, trades: list[dict]) -> None:
    """Update cumulative Polymarket position and totals for a slug."""
    if slug not in poly_pos:
        poly_pos[slug] = {"up_sh": 0.0, "dn_sh": 0.0,
                          "up_cost": 0.0, "dn_cost": 0.0,
                          "count": 0, "batch": 0}
    if slug not in totals:
        totals[slug] = {"up": 0.0, "dn": 0.0, "count": 0}

    poly_pos[slug]["batch"] += 1
    for t in trades:
        if t["side"] == "Up":
            poly_pos[slug]["up_sh"]   += t["shares"]
            poly_pos[slug]["up_cost"] += t["cost"]
            totals[slug]["up"]        += t["cost"]
        else:
            poly_pos[slug]["dn_sh"]   += t["shares"]
            poly_pos[slug]["dn_cost"] += t["cost"]
            totals[slug]["dn"]        += t["cost"]
        poly_pos[slug]["count"] += 1
        totals[slug]["count"]   += 1


# ── Polygon WebSocket ──────────────────────────────────────────────────────────

async def _polygon_ws() -> None:
    """Subscribe to Polygon logs for the bot wallet; set _trigger on any activity."""
    _local_seen: set[str] = set()
    while True:
        try:
            async with websockets.connect(
                POLYGON_WS, ping_interval=20, open_timeout=10
            ) as ws:
                sub = json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                    "params": ["logs", {"topics": [None, _SHADOW_TOPIC]}],
                })
                await ws.send(sub)
                resp   = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                sub_id = resp.get("result")
                console.print(
                    f"[dim]Polygon WS connected — watching {SHADOW_WALLET[:12]}…  sub={sub_id}[/dim]"
                )

                async for raw in ws:
                    msg = json.loads(raw)
                    log = msg.get("params", {}).get("result")
                    if not log:
                        continue
                    tx = log.get("transactionHash", "")
                    if tx and tx not in _local_seen:
                        _local_seen.add(tx)
                        if len(_local_seen) > 2000:
                            _local_seen.clear()
                        console.print(f"[cyan dim]  ⬡ Bot tx on-chain: {tx[:22]}…[/cyan dim]")
                        _trigger.set()

        except Exception as e:
            console.print(f"[yellow]Polygon WS: {e} — reconnecting in 5s[/yellow]")
            await asyncio.sleep(5)


# ── Watch loop — triggered by Polygon WS ──────────────────────────────────────

async def _watch_loop(client: httpx.AsyncClient) -> None:
    """Wait for Polygon trigger, then retry polling until trade is indexed."""
    while True:
        await _trigger.wait()
        _trigger.clear()
        console.print("  [cyan dim]◆ Trigger — polling data API (retry up to 2 min)…[/cyan dim]")

        before = len(seen_trade_ids)
        for attempt in range(MAX_RETRY_SECS // RETRY_INTERVAL):
            await asyncio.sleep(RETRY_INTERVAL)
            raw   = await _fetch_trades(client)
            added = await _process_trades(client, raw)
            if added > 0:
                console.print(f"  [cyan]◆ {added} new trade(s) indexed after {(attempt+1)*RETRY_INTERVAL}s[/cyan]")
                break
        else:
            console.print("  [dim yellow]No new trades found after 2 min (may be non-tracked market)[/dim yellow]")


# ── Baseline poll (catches anything Polygon WS misses) ────────────────────────

async def _baseline_poll(client: httpx.AsyncClient) -> None:
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        raw = await _fetch_trades(client)
        await _process_trades(client, raw)


# ── Ticker refresh loop ────────────────────────────────────────────────────────

async def _ticker_refresh_loop(client: httpx.AsyncClient) -> None:
    while True:
        await _refresh_tickers(client)
        await asyncio.sleep(300)   # refresh every 5 min


# ── Session summary ────────────────────────────────────────────────────────────

async def _summary_loop() -> None:
    """Print running session totals every 5 minutes."""
    while True:
        await asyncio.sleep(300)
        if not totals:
            continue
        t = Table(
            title=f"Session Totals  {datetime.now().strftime('%H:%M:%S')}",
            show_header=True,
            header_style="bold cyan",
        )
        t.add_column("Market",    width=22)
        t.add_column("Up $",      justify="right", width=10)
        t.add_column("Down $",    justify="right", width=10)
        t.add_column("Trades",    justify="right", width=8)
        for slug, s in totals.items():
            meta  = _slug_meta(slug) or {}
            label = f"{meta.get('coin','?')} {meta.get('window','?')}"
            t.add_row(label, f"${s['up']:.0f}", f"${s['dn']:.0f}", str(s["count"]))
        console.print(t)


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]Kalshi Mimic — Idolized-Scallops Shadow Tracker[/bold cyan]")
    console.print(f"Wallet : [dim]{SHADOW_WALLET}[/dim]")
    console.print(f"Markets: {', '.join(p for p, _ in POLY_PREFIXES)}\n")

    async with httpx.AsyncClient() as client:
        # Discover Kalshi tickers
        console.print("[dim]Discovering Kalshi markets…[/dim]")
        await _refresh_tickers(client)
        for series, ticker in sorted(kalshi_tickers.items()):
            console.print(f"  [dim]{series:12}[/dim] → [green]{ticker}[/green]")
        for series in ALL_SERIES:
            if series not in kalshi_tickers:
                console.print(f"  [dim]{series:12}[/dim] → [yellow]no open market[/yellow]")
        console.print()

        # Pre-load historical trades so we don't re-print them
        console.print("[dim]Loading historical trades (will not be re-printed)…[/dim]")
        raw = await _fetch_trades(client)
        for t in raw:
            tx = t.get("transactionHash") or t.get("id") or ""
            if tx:
                seen_trade_ids.add(tx)
        console.print(f"[dim]{len(seen_trade_ids)} historical trade IDs cached.  "
                      f"Watching for new activity…[/dim]\n")

        await asyncio.gather(
            _polygon_ws(),
            _watch_loop(client),
            _baseline_poll(client),
            _ticker_refresh_loop(client),
            _summary_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
