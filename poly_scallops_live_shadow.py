#!/usr/bin/env python3
"""
poly_scallops_live_shadow.py — live-poll Idolized-Scallops' trades and log
each one alongside the coin's price + momentum delta at trade time.

For every new trade we observe:
  - trade timestamp (Polymarket report time)
  - fetch timestamp (when our poll saw it)
  - lag (fetch - trade)
  - slug, outcome (Up/Down), side (BUY/SELL), price, size, notional
  - window_start_ts for the 15-min window containing the trade
  - elapsed_in_window seconds
  - coin spot price at trade time (from Coinbase WS cache)
  - coin spot price at window open (first observation after window start)
  - delta_pct = (price_at_trade - price_at_window_open) / price_at_window_open

Outputs:
  ~/.scallops_live_trades.jsonl  — one JSON object per observed trade

Run:   python poly_scallops_live_shadow.py
Stop:  Ctrl-C
"""
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import websockets
from rich.console import Console

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SHADOW_WALLETS = {
    "Idolized-Scallops": "0xe1d6b51521bd4365769199f392f9818661bd907c",
    "Twin-Driving-Shower": "0x3a847382ad6fff9be1db4e073fd9b869f6884d44",
}
SHADOW_WALLET = SHADOW_WALLETS["Idolized-Scallops"]  # primary (for backward compat)
ACTIVITY_API  = "https://data-api.polymarket.com/activity"   # ~30s indexing lag
TRADES_API    = "https://data-api.polymarket.com/trades"     # ~180s lag (fallback)
COINBASE_WS   = "wss://advanced-trade-ws.coinbase.com"

COINS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
}

TRADE_LOG     = Path.home() / ".scallops_live_trades.jsonl"
TWIN_LOG      = Path.home() / ".twin_live_trades.jsonl"
SEEN_STATE    = Path.home() / ".scallops_live_seen.json"
TWIN_SEEN     = Path.home() / ".twin_live_seen.json"
POLL_INTERVAL = 3       # seconds between trade polls
PRICE_CACHE_MIN = 30    # keep this many minutes of spot history per coin

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

con = Console()

# price_cache[coin][unix_second] = price
price_cache: dict[str, dict[int, float]] = {c: {} for c in COINS}


# ── Coinbase live price feed ────────────────────────────────────────────────

async def coinbase_price_feed() -> None:
    product_to_coin = {v: k for k, v in COINS.items()}
    products = list(COINS.values())
    while True:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=20,
                                          open_timeout=10) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": products,
                    "channel": "ticker",
                }))
                con.print(f"[dim]Coinbase WS connected — {', '.join(products)}[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for ev in msg.get("events", []):
                        for t in ev.get("tickers", []):
                            p   = t.get("product_id", "")
                            px  = float(t.get("price", 0) or 0)
                            sym = product_to_coin.get(p)
                            if not (sym and px > 0):
                                continue
                            now_s = int(time.time())
                            price_cache[sym][now_s] = px
                            # Prune old
                            cutoff = now_s - PRICE_CACHE_MIN * 60
                            for k in list(price_cache[sym].keys()):
                                if k < cutoff:
                                    del price_cache[sym][k]
        except Exception as e:
            con.print(f"[yellow]Coinbase WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)


def price_near(sym: str, ts: int, window: int = 5) -> float | None:
    """Nearest cached spot for `sym` within ±window seconds of ts."""
    cache = price_cache.get(sym, {})
    if ts in cache:
        return cache[ts]
    for d in range(1, window + 1):
        if (ts - d) in cache:
            return cache[ts - d]
        if (ts + d) in cache:
            return cache[ts + d]
    return None


def price_at_or_after(sym: str, ts: int, window: int = 30) -> float | None:
    """Earliest cached spot for `sym` at ts or up to `window` seconds after.

    Used for 'price at window open' — take the first spot observation we
    got after the 15-min boundary.
    """
    cache = price_cache.get(sym, {})
    for d in range(window + 1):
        if (ts + d) in cache:
            return cache[ts + d]
    return None


# ── Slug parsing ────────────────────────────────────────────────────────────

# Map short-prefix & long-word names to our coin codes.
_COIN_PREFIXES = [
    ("btc", "BTC"), ("eth", "ETH"), ("sol", "SOL"), ("xrp", "XRP"),
]
_COIN_LONGNAMES = [
    ("bitcoin", "BTC"), ("ethereum", "ETH"),
    ("solana",  "SOL"), ("xrp",      "XRP"),
]

def parse_slug(slug: str) -> tuple[str | None, str | None, int | None, int | None]:
    """Parse slug → (coin, market_type, window_start_ts, window_duration_sec).

    Handles:
      btc-updown-15m-<ts>            → (BTC, "15m",   ts, 900)
      eth-updown-4h-<ts>             → (ETH, "4h",    ts, 14400)
      bitcoin-up-or-down-apr-11...   → (BTC, "named", None, None)
      bitcoin-above-72600-on-...     → (BTC, "named", None, None)
    """
    if not slug:
        return (None, None, None, None)
    s = slug.lower()

    # Structured: <coin>-updown-<period>-<ts>
    for prefix, coin in _COIN_PREFIXES:
        for period, dur in (("15m", 900), ("4h", 14400), ("1h", 3600),
                            ("5m", 300), ("1d", 86400)):
            if s.startswith(f"{prefix}-updown-{period}-"):
                try:
                    ts = int(s.rsplit("-", 1)[1])
                    return (coin, period, ts, dur)
                except Exception:
                    return (coin, period, None, dur)

    # Named long-form: bitcoin-up-or-down-..., ethereum-above-...
    for longname, coin in _COIN_LONGNAMES:
        if s.startswith(longname):
            return (coin, "named", None, None)

    return (None, None, None, None)


# ── Scallops trade poller ───────────────────────────────────────────────────

session_counts = {"new": 0, "polls": 0, "per_coin": {c: 0 for c in COINS}}


def load_seen() -> set[str]:
    if SEEN_STATE.exists():
        try:
            return set(json.loads(SEEN_STATE.read_text()))
        except Exception:
            return set()
    return set()


def save_seen(seen: set[str]) -> None:
    try:
        SEEN_STATE.write_text(json.dumps(list(seen)[-5000:]))
    except Exception as e:
        con.print(f"[yellow]seen persist: {e}[/yellow]")


async def scallops_poll() -> None:
    seen = load_seen()
    first_run = len(seen) == 0
    con.print(f"[dim]Resuming with {len(seen)} known tx hashes[/dim]")

    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            session_counts["polls"] += 1
            try:
                # Primary: /activity endpoint (~30s indexing lag vs ~180s for /trades)
                r = await client.get(ACTIVITY_API,
                                     params={"user": SHADOW_WALLET, "limit": 100})
                if r.status_code == 200:
                    trades = r.json() or []
                    new_this_poll = 0
                    # Process in chronological order (oldest first)
                    for t in sorted(trades, key=lambda x: int(x.get("timestamp", 0) or 0)):
                        tx = t.get("transactionHash") or ""
                        if not tx or tx in seen:
                            continue
                        # /activity only has BUY-side TRADE events for our use case
                        if t.get("type") != "TRADE" or t.get("side") != "BUY":
                            continue
                        seen.add(tx)
                        if first_run:
                            # Suppress initial backfill spam — just mark as seen
                            continue
                        new_this_poll += 1
                        session_counts["new"] += 1
                        log_trade(t)
                    if first_run:
                        con.print(f"[dim]  First run — silently marked "
                                  f"{len(trades)} historical trades as seen. "
                                  f"Future polls will log live trades only.[/dim]")
                        first_run = False
                        save_seen(seen)
                    elif new_this_poll:
                        save_seen(seen)
                else:
                    con.print(f"[yellow]activity poll {r.status_code}: {r.text[:120]}[/yellow]")
            except Exception as e:
                con.print(f"[yellow]poll error: {e}[/yellow]")
            await asyncio.sleep(POLL_INTERVAL)


def log_trade(t: dict) -> None:
    trade_ts = int(t.get("timestamp", 0) or 0)
    fetch_ts = int(time.time())
    lag      = fetch_ts - trade_ts

    slug      = t.get("slug", "")
    side      = t.get("side", "")
    outcome   = t.get("outcome", "")
    price     = float(t.get("price", 0) or 0)
    size      = float(t.get("size", 0) or 0)
    notional  = round(price * size, 2)
    tx        = t.get("transactionHash", "")

    coin, market_type, window_start_ts, window_duration = parse_slug(slug)
    if coin:
        session_counts["per_coin"][coin] = session_counts["per_coin"].get(coin, 0) + 1

    elapsed = None
    if window_start_ts is not None:
        elapsed = trade_ts - window_start_ts

    # Spot prices & delta (only meaningful when we know the window start)
    price_at_trade = price_near(coin, trade_ts) if coin else None
    price_at_open  = (price_at_or_after(coin, window_start_ts)
                      if (coin and window_start_ts is not None) else None)
    delta_pct = None
    if price_at_trade and price_at_open:
        delta_pct = (price_at_trade - price_at_open) / price_at_open * 100.0

    entry = {
        "fetch_ts":         fetch_ts,
        "fetch_iso":        datetime.fromtimestamp(fetch_ts, tz=timezone.utc).isoformat(),
        "trade_ts":         trade_ts,
        "trade_iso":        datetime.fromtimestamp(trade_ts, tz=timezone.utc).isoformat(),
        "lag_sec":          lag,
        "slug":             slug,
        "coin":             coin,
        "market_type":      market_type,
        "window_duration":  window_duration,
        "side":             side,
        "outcome":          outcome,
        "price":            price,
        "size":             size,
        "notional":         notional,
        "tx":               tx,
        "window_start_ts":  window_start_ts,
        "elapsed_in_window":elapsed,
        "coin_at_trade":    price_at_trade,
        "coin_at_open":     price_at_open,
        "delta_pct":        round(delta_pct, 4) if delta_pct is not None else None,
    }

    # Append JSONL
    try:
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        con.print(f"[yellow]log write: {e}[/yellow]")

    # Pretty-print — HIS trade time is the primary timestamp
    if outcome.lower() == "up":
        o_col = "green"
    elif outcome.lower() == "down":
        o_col = "red"
    else:
        o_col = "white"

    side_col  = "cyan" if side == "BUY" else "magenta"
    delta_str = f"Δ={delta_pct:+.3f}%" if delta_pct is not None else "Δ=      ?"
    elapsed_str = f"+{elapsed:>5}s" if elapsed is not None else "       "
    coin_str  = coin or "???"
    mt_str    = (market_type or "?  ").ljust(5)
    short_slug = slug[-22:] if len(slug) > 22 else slug.ljust(22)

    his_time  = datetime.fromtimestamp(trade_ts).strftime("%H:%M:%S")
    our_time  = datetime.fromtimestamp(fetch_ts).strftime("%H:%M:%S")

    # Saturday data-API indexing is naturally 2-5 min slow; treat anything
    # older than 10 min as true backfill.
    is_stale = lag > 600
    tag = "[dim yellow]BACKFILL[/dim yellow]" if is_stale else "[bold white]LIVE[/bold white]"

    con.print(
        f"  [bold]{his_time}[/bold]  "
        f"{tag}  "
        f"{coin_str} {mt_str}  {elapsed_str}  "
        f"[{side_col}]{side:<4}[/{side_col}] "
        f"[{o_col}]{outcome:<4}[/{o_col}]  "
        f"@{price:.3f}  sz={size:>7.0f}  ${notional:>7.2f}  "
        f"{delta_str}  "
        f"[dim]seen={our_time} lag={lag}s  {short_slug}[/dim]"
    )


# ── Periodic session summary ───────────────────────────────────────────────

async def status_loop() -> None:
    while True:
        await asyncio.sleep(60)
        s = session_counts
        parts = [f"{c}={n}" for c, n in s["per_coin"].items() if n]
        coin_str = " ".join(parts) if parts else "—"
        cached = {c: len(price_cache[c]) for c in COINS}
        con.print(
            f"[dim][{datetime.now().strftime('%H:%M:%S')}] "
            f"polls={s['polls']}  new_trades={s['new']}  "
            f"{coin_str}  "
            f"cache BTC={cached['BTC']} ETH={cached['ETH']} "
            f"SOL={cached['SOL']} XRP={cached['XRP']}[/dim]"
        )


# ── Generic wallet poll (for Twin and future bots) ─────────────────────────

async def wallet_poll(name: str, wallet: str, log_path: Path, seen_path: Path) -> None:
    """Poll a wallet's trades via /activity and log to a separate JSONL."""
    # Load seen
    seen: set[str] = set()
    if seen_path.exists():
        try:
            seen = set(json.loads(seen_path.read_text()))
        except Exception:
            pass

    first_run = len(seen) == 0
    con.print(f"[dim]{name} shadow: wallet={wallet[:12]}… seen={len(seen)}[/dim]")

    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            try:
                r = await client.get(ACTIVITY_API,
                                     params={"user": wallet, "limit": 100})
                if r.status_code == 200:
                    trades = r.json() or []
                    new_count = 0
                    for t in sorted(trades, key=lambda x: int(x.get("timestamp", 0) or 0)):
                        if not isinstance(t, dict):
                            continue
                        tx = t.get("transactionHash") or ""
                        if not tx or tx in seen:
                            continue
                        if t.get("type") != "TRADE" or t.get("side") != "BUY":
                            continue
                        seen.add(tx)
                        if first_run:
                            continue
                        new_count += 1
                        # Log using same format as Scallops
                        log_trade_generic(t, name, log_path)

                    if first_run:
                        con.print(f"[dim]  {name}: marked {len(trades)} historical as seen[/dim]")
                        first_run = False

                    if new_count or first_run:
                        try:
                            seen_path.write_text(json.dumps(list(seen)[-5000:]))
                        except Exception:
                            pass
            except Exception as e:
                con.print(f"[yellow]{name} poll: {e}[/yellow]")
            await asyncio.sleep(POLL_INTERVAL)


def log_trade_generic(t: dict, bot_name: str, log_path: Path) -> None:
    """Log a trade from any bot wallet, same format as Scallops."""
    trade_ts = int(t.get("timestamp", 0) or 0)
    fetch_ts = int(time.time())
    lag = fetch_ts - trade_ts

    slug = t.get("slug", "")
    side = t.get("side", "")
    outcome = t.get("outcome", "")
    price = float(t.get("price", 0) or 0)
    size = float(t.get("size", 0) or 0)
    notional = round(price * size, 2)
    tx = t.get("transactionHash", "")

    coin, market_type, window_start_ts, window_duration = parse_slug(slug)

    elapsed = None
    if window_start_ts is not None:
        elapsed = trade_ts - window_start_ts

    price_at_trade = price_near(coin, trade_ts) if coin else None
    price_at_open = (price_at_or_after(coin, window_start_ts)
                     if (coin and window_start_ts is not None) else None)
    delta_pct = None
    if price_at_trade and price_at_open:
        delta_pct = (price_at_trade - price_at_open) / price_at_open * 100.0

    entry = {
        "fetch_ts": fetch_ts, "trade_ts": trade_ts, "lag_sec": lag,
        "bot": bot_name, "slug": slug, "coin": coin,
        "market_type": market_type, "side": side, "outcome": outcome,
        "price": price, "size": size, "notional": notional, "tx": tx,
        "window_start_ts": window_start_ts, "elapsed_in_window": elapsed,
        "coin_at_trade": price_at_trade, "coin_at_open": price_at_open,
        "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
    }

    try:
        with open(log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

    # Brief console output
    if outcome.lower() == "up":
        o_col = "green"
    elif outcome.lower() == "down":
        o_col = "red"
    else:
        o_col = "white"

    his_time = datetime.fromtimestamp(trade_ts).strftime("%H:%M:%S")
    coin_str = coin or "???"
    mt_str = (market_type or "?").ljust(5)
    elapsed_str = f"+{elapsed:>5}s" if elapsed is not None else "       "
    delta_str = f"Δ={delta_pct:+.3f}%" if delta_pct is not None else "Δ=      ?"

    con.print(
        f"  [bold]{his_time}[/bold]  [{bot_name}]  "
        f"{coin_str} {mt_str}  {elapsed_str}  "
        f"[{o_col}]{outcome:<4}[/{o_col}]  "
        f"@{price:.3f}  ${notional:>7.2f}  {delta_str}  "
        f"[dim]lag={lag}s[/dim]"
    )


# ── Main ───────────────────────────────────────────────────────────────────

async def main() -> None:
    con.print("[bold cyan]Scallops Live Shadow[/bold cyan]")
    con.print(f"  wallet:  {SHADOW_WALLET}")
    con.print(f"  log:     {TRADE_LOG}")
    con.print(f"  poll:    every {POLL_INTERVAL}s")
    con.print(f"  coins:   {', '.join(COINS.keys())}\n")

    # Warm up price cache before polling trades
    asyncio.create_task(coinbase_price_feed())
    con.print("[dim]Warming up Coinbase price cache…[/dim]")
    for _ in range(15):
        if all(price_cache[c] for c in COINS):
            break
        await asyncio.sleep(0.5)
    snap = "  ".join(
        f"{c}=${list(price_cache[c].values())[-1]:,.2f}"
        if price_cache[c] else f"{c}=?"
        for c in COINS
    )
    con.print(f"[dim]Initial prices: {snap}[/dim]\n")

    # Start Twin shadow poll
    asyncio.create_task(wallet_poll(
        name="Twin", wallet=SHADOW_WALLETS["Twin-Driving-Shower"],
        log_path=TWIN_LOG, seen_path=TWIN_SEEN,
    ))

    await asyncio.gather(
        scallops_poll(),
        status_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        con.print("\n[dim]Stopped.[/dim]")
