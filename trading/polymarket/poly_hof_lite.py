#!/usr/bin/env python3
"""
Polymarket Hall of Fame Tracker — lightweight terminal version.

Polls every tracked wallet directly. No spend/odds filter — shows ALL BUY trades.
Run: python3 poly_hof_lite.py
"""
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console
from rich.text import Text

HOF_FILE   = Path.home() / ".polymarket_hof.json"
TRADES_API = "https://data-api.polymarket.com/trades"
POLL_SECS  = 15     # per-wallet poll interval
HOF_RELOAD = 30     # seconds between HoF file reloads

console = Console()
_seen: set[str] = set()

TRADER_COLORS = [
    "rgb(234,179,8)",    # gold
    "rgb(96,165,250)",   # blue
    "rgb(167,139,250)",  # purple
    "rgb(74,222,128)",   # green
    "rgb(251,146,60)",   # orange
    "rgb(244,114,182)",  # pink
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_hof() -> dict:
    try:
        return json.loads(HOF_FILE.read_text()) if HOF_FILE.exists() else {}
    except Exception:
        return {}


async def _fetch(client: httpx.AsyncClient, wallet: str, limit: int = 100) -> list[dict]:
    try:
        r = await client.get(TRADES_API, params={"user": wallet, "limit": limit}, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


def _color_for(wallet: str, hof: dict) -> str:
    wallets = list(hof.keys())
    try:
        idx = wallets.index(wallet) % len(TRADER_COLORS)
    except ValueError:
        idx = 0
    return TRADER_COLORS[idx]


def _print_trade(t: dict, trader_name: str, color: str) -> None:
    price   = float(t.get("price") or 0)
    size    = float(t.get("size")  or 0)
    spent   = price * size
    payout  = size
    ts      = float(t.get("timestamp") or 0)
    trade_t = datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "??:??:??"
    outcome = t.get("outcome", "?")
    title   = t.get("title",   "?")
    slug    = t.get("slug",    "")
    wallet  = t.get("proxyWallet", "")

    implied = (1 / price - 1) * 100 if price > 0 else 0

    t_obj = Text()
    t_obj.append("★ ", f"bold {color}")
    t_obj.append(f"{trade_t}  ", "dim")
    t_obj.append(f"{trader_name:<20}", f"bold {color}")
    t_obj.append(f"  {outcome[:12]:<12}", "bold white")
    t_obj.append(f"  odds ")
    t_obj.append(f"{price:.3f}", "cyan")
    if spent >= 1:
        t_obj.append(f"  spent ")
        t_obj.append(f"${spent:>8,.0f}", "bold")
        t_obj.append(f"  wins ")
        t_obj.append(f"${payout:>9,.0f}", "bold green")
        t_obj.append(f"  ({implied:.0f}% if hits)")
    console.print(t_obj)

    console.print(f"    [dim]Market :[/dim] {title[:80]}")
    if slug:
        console.print(f"    [dim]Link   :[/dim] https://polymarket.com/event/{slug}")
    if wallet:
        console.print(f"    [dim]Profile:[/dim] https://polymarket.com/profile/{wallet}")
    console.print()


async def _seed_wallet(client: httpx.AsyncClient, wallet: str) -> None:
    """Seed _seen with existing trades for a wallet so we don't replay history."""
    trades = await _fetch(client, wallet, limit=200)
    for t in trades:
        _seen.add(t.get("transactionHash", ""))


async def _poll_wallet(client: httpx.AsyncClient, wallet: str, entry: dict, color: str) -> None:
    trades = await _fetch(client, wallet, limit=100)
    new_trades = []
    for t in trades:
        tx = t.get("transactionHash", "")
        if tx and tx not in _seen:
            _seen.add(tx)
            new_trades.append(t)

    now_str = datetime.now().strftime("%H:%M:%S")
    for t in new_trades:
        if t.get("side") != "BUY":
            continue
        price = float(t.get("price") or 0)
        size  = float(t.get("size")  or 0)
        if price <= 0 or size <= 0:
            continue
        trader_name = entry.get("name", "anon")
        console.rule(f"[{color}]★ HoF ALERT  {now_str}  {trader_name}[/{color}]")
        _print_trade(t, trader_name, color)


async def _load_history(client: httpx.AsyncClient, hof: dict) -> None:
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    console.print("[dim]Loading today's trades for HoF wallets...[/dim]")

    all_hits: list[tuple[dict, str, str]] = []  # (trade, trader_name, color)
    for wallet, entry in hof.items():
        color = _color_for(wallet, hof)
        trades = await _fetch(client, wallet, limit=500)
        for t in trades:
            tx = t.get("transactionHash", "")
            ts = float(t.get("timestamp") or 0)
            if ts < today_start:
                continue
            if t.get("side") != "BUY":
                continue
            price = float(t.get("price") or 0)
            size  = float(t.get("size")  or 0)
            if price <= 0 or size <= 0:
                continue
            _seen.add(tx)
            all_hits.append((t, entry.get("name", "anon"), color))

    all_hits.sort(key=lambda x: float(x[0].get("timestamp") or 0))
    console.print(f"[green]{len(all_hits)} trade(s) today[/green]\n")

    if all_hits:
        console.rule("[bold]TODAY'S HoF TRADES[/bold]")
        for t, trader_name, color in all_hits:
            _print_trade(t, trader_name, color)


async def _poll_loop(client: httpx.AsyncClient, hof: dict) -> None:
    hof_last_reload = time.time()
    known_wallets   = set(hof.keys())

    # Seed _seen for any wallets already known (so we don't re-emit history on first poll)
    console.print("[dim]Seeding seen-set for tracked wallets...[/dim]")
    await asyncio.gather(*[_seed_wallet(client, w) for w in known_wallets])

    # Load today's history
    await _load_history(client, hof)

    console.rule("[cyan]LIVE HoF FEED[/cyan]")

    while True:
        # Reload HoF file and pick up newly added wallets
        if time.time() - hof_last_reload > HOF_RELOAD:
            hof = _load_hof()
            hof_last_reload = time.time()
            new_wallets = set(hof.keys()) - known_wallets
            if new_wallets:
                console.print(f"[dim]New wallets added: {len(new_wallets)}. Seeding...[/dim]")
                await asyncio.gather(*[_seed_wallet(client, w) for w in new_wallets])
                known_wallets = set(hof.keys())

        if not hof:
            now_str = datetime.now().strftime("%H:%M:%S")
            sys.stdout.write(f"\r[{now_str}] No wallets in HoF — add one with poly_scan_lite.py    ")
            sys.stdout.flush()
            await asyncio.sleep(POLL_SECS)
            continue

        # Poll all wallets concurrently
        tasks = [
            _poll_wallet(client, wallet, entry, _color_for(wallet, hof))
            for wallet, entry in hof.items()
        ]
        await asyncio.gather(*tasks)

        now_str = datetime.now().strftime("%H:%M:%S")
        sys.stdout.write(f"\r[{now_str}] watching {len(hof)} wallet(s)... ({len(_seen)} seen)    ")
        sys.stdout.flush()

        await asyncio.sleep(POLL_SECS)


async def main() -> None:
    console.print("[bold cyan]Polymarket HoF Tracker (lite)[/bold cyan]")
    console.print(f"  HoF file  : [dim]{HOF_FILE}[/dim]")
    console.print(f"  Poll every: [bold]{POLL_SECS}s[/bold] per wallet")
    console.print(f"  No filter — shows ALL BUY trades from tracked wallets")
    console.print()

    hof = _load_hof()
    if not hof:
        console.print(f"[yellow]HoF is empty. Add wallets via poly_scan_lite.py or edit {HOF_FILE}[/yellow]\n")

    async with httpx.AsyncClient() as client:
        await _poll_loop(client, hof)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
