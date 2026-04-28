#!/usr/bin/env python3
"""
BTC price comparison across exchanges — shows live divergence between
Coinbase, Bitstamp, Kraken, Gemini and how the composite (≈BRTI) is derived.

BRTI (CF Benchmarks Real Time Index) methodology:
  - Collects trade prices every second from constituent exchanges
  - Applies volume-weighting and outlier filtering
  - We approximate it as a simple equal-weight average of 4 exchanges

Run: python3 btc_price_sources.py
Stop: Ctrl-C
"""
import asyncio
import json
import time
from datetime import datetime

import websockets
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

console = Console()

prices:      dict[str, float] = {}   # exchange → latest trade price
last_update: dict[str, float] = {}   # exchange → unix timestamp of last update
tick_count:  dict[str, int]   = {}   # exchange → number of ticks received

SOURCES = ["coinbase", "bitstamp", "kraken", "gemini"]
STALE_AFTER = 10  # seconds before a price is considered stale


def _fresh_prices() -> dict[str, float]:
    """Only prices updated within STALE_AFTER seconds."""
    now = time.time()
    return {k: v for k, v in prices.items()
            if v > 0 and (now - last_update.get(k, 0)) < STALE_AFTER}


def _composite() -> float | None:
    """Equal-weight average of fresh exchange prices — our BRTI approximation."""
    fp = _fresh_prices()
    return sum(fp.values()) / len(fp) if fp else None


def _make_table() -> Table:
    now       = datetime.now().strftime("%H:%M:%S")
    comp      = _composite()
    fp        = _fresh_prices()
    vals      = list(fp.values())
    spread    = max(vals) - min(vals) if len(vals) >= 2 else 0
    n_sources = len(fp)

    t = Table(
        title=f"BTC Price Sources  {now}  ({n_sources}/{len(SOURCES)} live)",
        show_header=True,
        header_style="bold cyan",
    )
    t.add_column("Exchange",      width=12)
    t.add_column("Last Trade",    justify="right", width=14)
    t.add_column("vs Composite",  justify="right", width=14)
    t.add_column("Weight",        justify="right", width=10)
    t.add_column("Ticks",         justify="right", width=8)
    t.add_column("Age",           justify="right", width=8)

    for src in SOURCES:
        price = prices.get(src, 0)
        age   = time.time() - last_update.get(src, 0)
        ticks = tick_count.get(src, 0)
        stale = age > STALE_AFTER or price <= 0

        if price <= 0:
            t.add_row(src.capitalize(), "[dim]connecting...[/dim]", "", "", "", "")
            continue

        age_str = f"[red]{age:.0f}s stale[/red]" if stale else f"[dim]{age:.0f}s[/dim]"

        if comp and not stale:
            diff     = price - comp
            weight   = f"1/{n_sources} = {1/n_sources:.2f}"
            if abs(diff) < 5:
                diff_str = f"[green]{diff:+.2f}[/green]"
            elif abs(diff) < 25:
                diff_str = f"[yellow]{diff:+.2f}[/yellow]"
            else:
                diff_str = f"[red]{diff:+.2f}[/red]"
        else:
            diff_str = "[dim]stale[/dim]"
            weight   = "[dim]excluded[/dim]"

        price_str = f"[dim]${price:,.2f}[/dim]" if stale else f"${price:,.2f}"
        t.add_row(src.capitalize(), price_str, diff_str, weight, str(ticks), age_str)

    t.add_section()

    # Composite row
    if comp:
        t.add_row(
            "[bold]Composite≈BRTI[/bold]",
            f"[bold green]${comp:,.2f}[/bold green]",
            f"[dim]avg({n_sources} exchanges)[/dim]",
            "", "", "",
        )
    else:
        t.add_row("[bold]Composite≈BRTI[/bold]", "[yellow]waiting...[/yellow]", "", "", "", "")

    # Spread row
    if len(vals) >= 2:
        spread_col = "green" if spread < 10 else "yellow" if spread < 50 else "red"
        t.add_row(
            "[dim]Spread (max-min)[/dim]",
            f"[{spread_col}]${spread:.2f}[/{spread_col}]",
            f"[dim]hi=${max(vals):,.2f}  lo=${min(vals):,.2f}[/dim]",
            "", "", "",
        )

    return t


async def _coinbase() -> None:
    sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"}
    while True:
        try:
            async with websockets.connect(
                "wss://advanced-trade-ws.coinbase.com", ping_interval=20
            ) as ws:
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for evt in msg.get("events", []):
                        for tk in evt.get("tickers", []):
                            p = float(tk.get("price", 0) or 0)
                            if p > 0:
                                prices["coinbase"]      = p
                                last_update["coinbase"] = time.time()
                                tick_count["coinbase"]  = tick_count.get("coinbase", 0) + 1
        except Exception:
            await asyncio.sleep(3)


async def _bitstamp() -> None:
    sub = {"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}
    while True:
        try:
            async with websockets.connect("wss://ws.bitstamp.net", ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("event") == "trade":
                        p = float(msg.get("data", {}).get("price", 0) or 0)
                        if p > 0:
                            prices["bitstamp"]      = p
                            last_update["bitstamp"] = time.time()
                            tick_count["bitstamp"]  = tick_count.get("bitstamp", 0) + 1
        except Exception:
            await asyncio.sleep(3)


async def _kraken() -> None:
    sub = {"method": "subscribe", "params": {"channel": "ticker", "symbol": ["BTC/USD"]}}
    while True:
        try:
            async with websockets.connect("wss://ws.kraken.com/v2", ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") == "ticker":
                        for d in msg.get("data", []):
                            p = float(d.get("last", 0) or 0)
                            if p > 0:
                                prices["kraken"]      = p
                                last_update["kraken"] = time.time()
                                tick_count["kraken"]  = tick_count.get("kraken", 0) + 1
        except Exception:
            await asyncio.sleep(3)


async def _gemini() -> None:
    while True:
        try:
            async with websockets.connect(
                "wss://api.gemini.com/v1/marketdata/BTCUSD?trades=true",
                ping_interval=20,
            ) as ws:
                async for raw in ws:
                    msg = json.loads(raw)
                    for evt in msg.get("events", []):
                        if evt.get("type") == "trade":
                            p = float(evt.get("price", 0) or 0)
                            if p > 0:
                                prices["gemini"]      = p
                                last_update["gemini"] = time.time()
                                tick_count["gemini"]  = tick_count.get("gemini", 0) + 1
        except Exception:
            await asyncio.sleep(3)


async def _display() -> None:
    with Live(console=console, refresh_per_second=2) as live:
        while True:
            live.update(_make_table())
            await asyncio.sleep(0.5)


async def main() -> None:
    console.print("[bold cyan]BTC Multi-Source Price Feed[/bold cyan]")
    console.print(
        "Composite = equal-weight average of fresh exchange prices (≈BRTI)\n"
        "Stale threshold = 10s — excluded from composite if no trade in 10s\n"
    )
    await asyncio.gather(
        _coinbase(),
        _bitstamp(),
        _kraken(),
        _gemini(),
        _display(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
