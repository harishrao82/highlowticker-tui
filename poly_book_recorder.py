#!/usr/bin/env python3
"""
poly_book_recorder.py — live-record Polymarket CLOB book updates for the
4 crypto 15-min Up/Down markets (BTC, ETH, SOL, XRP).

For every window:
  1. At window start, resolve 4 slugs to 8 condition tokens (Up + Down per coin)
  2. Subscribe to Polymarket CLOB WebSocket for those tokens
  3. Log every book update (best bid/ask + sizes) to JSONL
  4. On window close, disconnect and prepare for next

Log: ~/.poly_book_snapshots.jsonl
  Each line: {ts, window_start_ts, coin, outcome, best_bid, best_ask, bid_size, ask_size}

Run:   python poly_book_recorder.py
Stop:  Ctrl-C
"""
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from rich.console import Console

console = Console()

GAMMA_API = "https://gamma-api.polymarket.com/markets"
CLOB_API  = "https://clob.polymarket.com"
POLL_INTERVAL = 2.0  # seconds between book snapshots per token

COINS = ["BTC", "ETH", "SOL", "XRP"]
BOOK_LOG = Path.home() / ".poly_book_snapshots.jsonl"

# ── Helpers ──────────────────────────────────────────────────────────────────

def window_start_ts(offset_windows: int = 0) -> int:
    """Current or next Nth 15-min window start (UTC)."""
    now = int(time.time())
    base = (now // 900) * 900
    return base + offset_windows * 900


def seconds_until_next_window() -> float:
    return (window_start_ts(1) - time.time())


def slug_for(coin: str, wts: int) -> str:
    return f"{coin.lower()}-updown-15m-{wts}"


async def resolve_market(client: httpx.AsyncClient, slug: str) -> dict | None:
    """Query Gamma API for a slug → market metadata (includes clobTokenIds)."""
    try:
        r = await client.get(GAMMA_API, params={"slug": slug}, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        return data[0] if data else None
    except Exception as e:
        console.print(f"[yellow]resolve_market {slug}: {e}[/yellow]")
        return None


async def resolve_window_tokens(wts: int) -> dict | None:
    """For a window start ts, return {coin: {'Up': token_id, 'Down': token_id}}."""
    async with httpx.AsyncClient() as client:
        result = {}
        for coin in COINS:
            slug = slug_for(coin, wts)
            market = await resolve_market(client, slug)
            if not market:
                console.print(f"[yellow]  {slug} not found[/yellow]")
                continue
            try:
                tokens = json.loads(market.get("clobTokenIds", "[]"))
                outcomes = json.loads(market.get("outcomes", "[]"))
                if len(tokens) < 2 or len(outcomes) < 2:
                    continue
                up_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
                dn_idx = 1 - up_idx
                result[coin] = {"Up": tokens[up_idx], "Down": tokens[dn_idx]}
            except Exception as e:
                console.print(f"[yellow]  {slug} parse: {e}[/yellow]")
        return result if result else None


def log_snapshot(entry: dict) -> None:
    try:
        with open(BOOK_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        console.print(f"[yellow]log write: {e}[/yellow]")


# ── REST polling loop ────────────────────────────────────────────────────────

async def fetch_book(client: httpx.AsyncClient, token_id: str) -> dict | None:
    """GET /book for a token — returns {'bids': [...], 'asks': [...]}"""
    try:
        r = await client.get(
            f"{CLOB_API}/book",
            params={"token_id": token_id},
            timeout=8,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


async def stream_window(wts: int, coin_tokens: dict) -> None:
    """Poll CLOB /book for all 8 tokens every POLL_INTERVAL seconds,
    log snapshots until window closes."""
    tokens = []  # list of (coin, outcome, token_id)
    for coin, sides in coin_tokens.items():
        for outcome, tid in sides.items():
            tokens.append((coin, outcome, tid))

    window_end = wts + 900
    update_count = 0
    last_snapshot = {}  # (coin, outcome) → last logged (bid, ask, bid_sz, ask_sz)

    async with httpx.AsyncClient() as client:
        while time.time() < window_end:
            iter_start = time.time()
            # Fetch all 8 token books in parallel
            results = await asyncio.gather(
                *[fetch_book(client, tid) for (_, _, tid) in tokens],
                return_exceptions=True,
            )

            now_ts = time.time()
            for (coin, outcome, tid), book in zip(tokens, results):
                if not book or isinstance(book, Exception):
                    continue
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                best_bid = None; bid_size = None
                best_ask = None; ask_size = None
                if bids:
                    sb = sorted(bids, key=lambda b: -float(b.get("price", 0)))
                    best_bid = float(sb[0].get("price", 0))
                    bid_size = float(sb[0].get("size", 0))
                if asks:
                    sa = sorted(asks, key=lambda a: float(a.get("price", 0)))
                    best_ask = float(sa[0].get("price", 0))
                    ask_size = float(sa[0].get("size", 0))

                # Only log if something changed (avoid spam)
                key = (coin, outcome)
                snap = (best_bid, best_ask, bid_size, ask_size)
                if last_snapshot.get(key) == snap:
                    continue
                last_snapshot[key] = snap

                log_snapshot({
                    "ts": round(now_ts, 3),
                    "window_start_ts": wts,
                    "elapsed": round(now_ts - wts, 2),
                    "coin": coin,
                    "outcome": outcome,
                    "best_bid": best_bid,
                    "bid_size": bid_size,
                    "best_ask": best_ask,
                    "ask_size": ask_size,
                    "asset_id": tid,
                })
                update_count += 1

            if update_count > 0 and update_count % 100 == 0:
                console.print(f"[dim]  logged {update_count} book snapshots[/dim]")

            # Sleep to hit POLL_INTERVAL
            elapsed = time.time() - iter_start
            sleep_for = max(0.1, POLL_INTERVAL - elapsed)
            if time.time() + sleep_for >= window_end:
                break
            await asyncio.sleep(sleep_for)

    console.print(f"[bold]  Window closed. Logged {update_count} book snapshots.[/bold]")


# ── Main loop ────────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]Polymarket Book Recorder[/bold cyan]")
    console.print(f"  coins: {', '.join(COINS)}")
    console.print(f"  log: {BOOK_LOG}")
    console.print()

    # Start with current window immediately (don't wait for next)
    # Pre-resolve the FIRST target window's tokens immediately (current or next)
    first_iteration = True
    while True:
        if first_iteration:
            first_iteration = False
            wts = window_start_ts(0)
            elapsed = time.time() - wts
            console.print(f"[dim]Starting mid-window (T={elapsed:.0f}s into current window)[/dim]")
            tokens = await resolve_window_tokens(wts)
        else:
            # Pre-resolve the NEXT window ~30s before it starts so polling can
            # begin at T=0 without API call latency blocking the first ticks.
            wait = seconds_until_next_window()
            if wait > 35:
                console.print(f"[dim]Next window in {wait:.0f}s (will pre-resolve 30s early)…[/dim]")
                await asyncio.sleep(wait - 30)
            next_wts = window_start_ts(1)
            console.print(f"[dim]Pre-resolving tokens for wts={next_wts}…[/dim]")
            tokens = await resolve_window_tokens(next_wts)
            # Now wait for actual window start
            while time.time() < next_wts:
                await asyncio.sleep(0.1)
            wts = next_wts

        dt = datetime.fromtimestamp(wts, tz=timezone.utc).astimezone()
        console.rule(f"[bold]Window {dt.strftime('%H:%M %Z')} (wts={wts})[/bold]")

        if not tokens:
            console.print(f"[yellow]  No markets resolved — waiting for next window[/yellow]")
            await asyncio.sleep(60)
            continue

        console.print(f"  Resolved: {', '.join(tokens.keys())}")

        try:
            await stream_window(wts, tokens)
        except Exception as e:
            console.print(f"[red]  window stream error: {e}[/red]")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold]stopped[/bold]")
