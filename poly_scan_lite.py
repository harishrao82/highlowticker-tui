#!/usr/bin/env python3
"""
Polymarket Whale Scanner — lightweight terminal version.

Prints trades matching: spent >= MIN_SPEND and odds <= MAX_PRICE.
Press h + Enter on a printed wallet address to add to Hall of Fame.
Run: python3 poly_scan_lite.py
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
GAMMA_API  = "https://gamma-api.polymarket.com/markets"
MIN_SPEND  = 1000
MAX_PRICE  = 0.9
POLL_SECS  = 4      # fast async polling — persistent HTTP connection
PAGE_SIZE  = 500

console = Console()
_seen: set[str] = set()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clean_name(name: str, wallet: str) -> str:
    if not name or name.lower().startswith(wallet.lower()[:6].lower()):
        return "anon"
    if len(name) > 30 and "-" in name and name.replace("-", "").replace("0x", "").isalnum():
        return "anon"
    return name


def _load_hof() -> dict:
    try:
        return json.loads(HOF_FILE.read_text()) if HOF_FILE.exists() else {}
    except Exception:
        return {}


def _save_hof(hof: dict) -> None:
    try:
        HOF_FILE.write_text(json.dumps(hof, indent=2))
    except Exception:
        pass


def _outcome(slug: str, outcome: str, client: httpx.Client) -> str:
    if not slug:
        return ""
    try:
        r = client.get(GAMMA_API, params={"slug": slug}, timeout=8)
        data = r.json()
        if isinstance(data, list):
            data = data[0] if data else {}
        if not data.get("closed"):
            return ""
        prices   = json.loads(data["outcomePrices"]) if isinstance(data.get("outcomePrices"), str) else []
        outcomes = json.loads(data["outcomes"])       if isinstance(data.get("outcomes"), str)      else []
        idx = next((i for i, o in enumerate(outcomes)
                    if str(o).strip().lower() == outcome.strip().lower()), None)
        if idx is None or idx >= len(prices):
            return ""
        p = float(prices[idx])
        return "WIN" if p >= 0.99 else ("LOSS" if p <= 0.01 else "VOID")
    except Exception:
        return ""


def _print_hit(h: dict, hof: dict, client: httpx.Client) -> None:
    implied = (1 / h["price"] - 1) * 100
    trade_t = datetime.fromtimestamp(h["ts"]).strftime("%H:%M:%S")
    in_hof  = h["wallet"] in hof

    t = Text()
    # Header line
    if in_hof:
        t.append("★ ", "bold rgb(234,179,8)")
    t.append(f"{trade_t}  ", "dim")
    t.append(f"{h['outcome'][:12]:<12}", "bold white")
    t.append(f"  odds ")
    t.append(f"{h['price']:.3f}", "cyan")
    t.append(f"  spent ")
    t.append(f"${h['spent']:>8,.0f}", "bold")
    t.append(f"  wins ")
    t.append(f"${h['payout']:>9,.0f}", "bold green")
    t.append(f"  ({implied:.0f}% if hits)")
    console.print(t)

    # Market
    console.print(f"    [dim]Market :[/dim] {h['title'][:80]}")
    if h.get("slug"):
        console.print(f"    [dim]Link   :[/dim] https://polymarket.com/event/{h['slug']}")

    # Trader
    name = h["name"]
    wallet = h["wallet"]
    hof_badge = " [bold rgb(234,179,8)]★ HoF[/bold rgb(234,179,8)]" if in_hof else ""
    console.print(f"    [dim]Trader :[/dim] {name}  ({wallet[:20]}...){hof_badge}")
    if wallet:
        console.print(f"    [dim]Profile:[/dim] https://polymarket.com/profile/{wallet}")

    # Add to HoF prompt (non-blocking — just print the command)
    if not in_hof:
        console.print(f"    [dim]→ run: [/dim][cyan]echo h {wallet}[/cyan][dim] | python3 poly_hof_add.py[/dim]")

    console.print()


async def _fetch(client: httpx.AsyncClient, params: dict) -> list[dict]:
    try:
        r = await client.get(TRADES_API, params=params, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


async def _load_history(client: httpx.AsyncClient) -> list[dict]:
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    hits, offset, seen_tx = [], 0, set()
    console.print("[dim]Scanning today's history...[/dim]", end=" ")
    while True:
        trades = await _fetch(client, {"limit": PAGE_SIZE, "offset": offset})
        if not trades:
            break
        oldest = None
        for t in trades:
            ts = float(t.get("timestamp") or 0)
            if oldest is None or ts < oldest:
                oldest = ts
            if ts < today_start or t.get("side") != "BUY":
                continue
            tx = t.get("transactionHash", "")
            if tx in seen_tx:
                continue
            seen_tx.add(tx)
            price = float(t.get("price") or 0)
            size  = float(t.get("size")  or 0)
            if price <= 0 or size <= 0 or price > MAX_PRICE or price * size < MIN_SPEND:
                continue
            wallet = t.get("proxyWallet", "")
            hits.append({
                "title":   t.get("title", "?"),
                "outcome": t.get("outcome", "?"),
                "price":   price,
                "size":    size,
                "spent":   price * size,
                "payout":  size,
                "ts":      ts,
                "tx":      tx,
                "slug":    t.get("slug", ""),
                "wallet":  wallet,
                "name":    _clean_name(t.get("name") or t.get("pseudonym") or "", wallet),
            })
        console.print(".", end="", highlight=False)
        if oldest is not None and oldest < today_start:
            break
        offset += PAGE_SIZE
    console.print(f" [green]{len(hits)} match(es)[/green]")
    return sorted(hits, key=lambda h: h["ts"])


async def _poll_loop(client: httpx.AsyncClient) -> None:
    hof = _load_hof()
    hof_last_check = time.time()

    while True:
        await asyncio.sleep(POLL_SECS)

        # Reload HoF every 60s
        if time.time() - hof_last_check > 60:
            hof = _load_hof()
            hof_last_check = time.time()

        # Global feed
        trades = await _fetch(client, {"limit": PAGE_SIZE})
        candidates = [t for t in trades if t.get("transactionHash", "") not in _seen]
        for t in candidates:
            _seen.add(t.get("transactionHash", ""))

        now_str = datetime.now().strftime("%H:%M:%S")
        matched = 0
        for t in candidates:
            if t.get("side") != "BUY":
                continue
            price = float(t.get("price") or 0)
            size  = float(t.get("size")  or 0)
            if price <= 0 or size <= 0 or price > MAX_PRICE or price * size < MIN_SPEND:
                continue
            wallet = t.get("proxyWallet", "")
            h = {
                "title":   t.get("title", "?"),
                "outcome": t.get("outcome", "?"),
                "price":   price,
                "size":    size,
                "spent":   price * size,
                "payout":  size,
                "ts":      float(t.get("timestamp") or 0),
                "tx":      t.get("transactionHash", ""),
                "slug":    t.get("slug", ""),
                "wallet":  wallet,
                "name":    _clean_name(t.get("name") or t.get("pseudonym") or "", wallet),
            }
            if matched == 0:
                console.rule(f"[cyan]WHALE ALERT  {now_str}[/cyan]")
            with httpx.Client() as sync_client:
                _print_hit(h, hof, sync_client)
            matched += 1

        # HoF wallets — check directly in case they trade in low-volume markets
        for wallet, entry in hof.items():
            wallet_trades = await _fetch(client, {"user": wallet, "limit": 50})
            for t in wallet_trades:
                tx = t.get("transactionHash", "")
                if tx in _seen or t.get("side") != "BUY":
                    continue
                price = float(t.get("price") or 0)
                size  = float(t.get("size")  or 0)
                if price <= 0 or size <= 0 or price > MAX_PRICE or price * size < MIN_SPEND:
                    continue
                _seen.add(tx)
                h = {
                    "title":   t.get("title", "?"),
                    "outcome": t.get("outcome", "?"),
                    "price":   price,
                    "size":    size,
                    "spent":   price * size,
                    "payout":  size,
                    "ts":      float(t.get("timestamp") or 0),
                    "tx":      tx,
                    "slug":    t.get("slug", ""),
                    "wallet":  wallet,
                    "name":    entry.get("name", "anon"),
                }
                console.rule(f"[rgb(234,179,8)]★ HoF ALERT  {now_str}[/rgb(234,179,8)]")
                with httpx.Client() as sync_client:
                    _print_hit(h, hof, sync_client)

        if not matched:
            sys.stdout.write(f"\r[{now_str}] watching... ({len(_seen)} seen)    ")
            sys.stdout.flush()


async def main() -> None:
    console.print(f"[bold cyan]Polymarket Whale Scanner (lite)[/bold cyan]")
    console.print(f"  Min spend : [bold]${MIN_SPEND:,}[/bold]")
    console.print(f"  Max odds  : [bold]{MAX_PRICE:.0%}[/bold]")
    console.print(f"  Poll every: [bold]{POLL_SECS}s[/bold]")
    console.print(f"  HoF file  : [dim]{HOF_FILE}[/dim]")
    console.print()

    async with httpx.AsyncClient() as client:
        # Load today's history first
        history = await _load_history(client)
        hof = _load_hof()

        if history:
            console.rule("[bold]TODAY'S MATCHING BETS[/bold]")
            with httpx.Client() as sync_client:
                for h in history:
                    _print_hit(h, hof, sync_client)
                    _seen.add(h["tx"])
        else:
            console.print("[dim]No matching bets today before now.[/dim]\n")
            # Still seed seen set
            seed = await _fetch(client, {"limit": PAGE_SIZE})
            for t in seed:
                _seen.add(t.get("transactionHash", ""))

        console.rule("[cyan]LIVE FEED[/cyan]")
        await _poll_loop(client)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
