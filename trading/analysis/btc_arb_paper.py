#!/usr/bin/env python3
"""
BTC 15-min Up/Down — paper arb trader (CLOB edition).

How the arb works:
  - Each 15-min window is a binary market: one side pays $1, the other $0.
  - CLOB best ask (Up) + CLOB best ask (Down) = what you'd pay to own both sides.
  - If that sum < $1.00, you're guaranteed a profit regardless of which way BTC moves.
  - Maker orders on Polymarket have 0% fee — so any sum < 1.00 is real edge.
  - The bot we reverse-engineered places maker limit orders; we simulate that here.

Sizing: proportional to edge.
  edge=1% → $BASE_PAYOUT target payout ($50 default)
  edge=2% → $100, capped at MAX_PAYOUT

Run: python3 btc_arb_paper.py
"""
import asyncio
import json
import time
from datetime import datetime

import httpx
from rich.console import Console
from rich.text import Text
from rich.table import Table
from rich import box

GAMMA_API     = "https://gamma-api.polymarket.com/markets"
CLOB_API      = "https://clob.polymarket.com"
POLL_SECS     = 4        # refresh interval
MAKER_FEE     = 0.00     # maker orders: 0% fee
TAKER_FEE     = 0.02     # shown for reference only
ARB_THRESHOLD = 1.00     # with 0% maker fee, any sum < 1.00 is profitable
BASE_PAYOUT   = 50.0     # target payout at 1% edge
MAX_PAYOUT    = 500.0
EDGE_SCALE    = 0.01     # 1% → BASE_PAYOUT

console   = Console()
positions: dict[str, dict] = {}
session_pnl = 0.0
arb_count   = 0


# ── Market / CLOB helpers ──────────────────────────────────────────────────────

def _window_ts(offset: int = 0) -> int:
    now  = int(time.time())
    base = (now // 900) * 900
    return base + offset * 900


def _slug(ts: int) -> str:
    return f"btc-updown-15m-{ts}"


async def _fetch_market(client: httpx.AsyncClient, slug: str) -> dict | None:
    try:
        r = await client.get(GAMMA_API, params={"slug": slug}, timeout=8)
        r.raise_for_status()
        d = r.json()
        return d[0] if d else None
    except Exception:
        return None


async def _clob_best_ask(client: httpx.AsyncClient, token_id: str) -> float | None:
    """Best ask = lowest price someone will sell at = what you pay to BUY."""
    try:
        r = await client.get(f"{CLOB_API}/price",
                             params={"token_id": token_id, "side": "buy"}, timeout=6)
        r.raise_for_status()
        return float(r.json()["price"])
    except Exception:
        return None


async def _fetch_clob_prices(client: httpx.AsyncClient, market: dict) -> tuple[float, float] | None:
    """Return (ask_up, ask_down) from CLOB or fall back to AMM midpoint."""
    try:
        tokens   = json.loads(market.get("clobTokenIds", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))
        if len(tokens) < 2 or len(outcomes) < 2:
            return None
        up_idx   = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
        down_idx = 1 - up_idx
        ask_up, ask_down = await asyncio.gather(
            _clob_best_ask(client, tokens[up_idx]),
            _clob_best_ask(client, tokens[down_idx]),
        )
        if ask_up is None or ask_down is None:
            return None
        return ask_up, ask_down
    except Exception:
        return None


def _parse_end(market: dict) -> float:
    try:
        end = market.get("endDate", "")
        dt  = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return time.time() + 900


def _target_payout(edge: float) -> float:
    return min(BASE_PAYOUT * (edge / EDGE_SCALE), MAX_PAYOUT)


# ── Display ────────────────────────────────────────────────────────────────────

def _print_odds(title: str, ask_up: float, ask_down: float, is_arb: bool,
                source: str = "CLOB") -> None:
    s    = ask_up + ask_down
    edge = (1 - s) * 100        # maker fee = 0%
    now  = datetime.now().strftime("%H:%M:%S")
    t = Text()
    t.append(f"[{now}] ", "dim")
    t.append(f"[{source}] ", "dim cyan")
    t.append(f"{title[:42]:<42}  ", "white")
    t.append(f"Up={ask_up:.3f}  Down={ask_down:.3f}  ", "cyan")
    t.append(f"Sum={s:.3f}  ", "bold yellow" if s < 1.0 else "dim")
    if is_arb:
        t.append(f"★ ARB  edge={edge:.2f}%", "bold green")
    else:
        t.append(f"edge={edge:.2f}%  no arb", "dim")
    console.print(t)


def _print_entry(pos: dict) -> None:
    edge_dollar = pos["target_payout"] - (pos["spent_up"] + pos["spent_down"])
    console.rule(f"[bold green]★ PAPER ARB ENTRY  {datetime.now().strftime('%H:%M:%S')}[/bold green]")
    console.print(f"  Market   : {pos['title']}")
    console.print(f"  Buy Up   : {pos['target_payout']:.0f} shares @ {pos['up_price']:.3f}  → cost ${pos['spent_up']:.2f}")
    console.print(f"  Buy Down : {pos['target_payout']:.0f} shares @ {pos['down_price']:.3f}  → cost ${pos['spent_down']:.2f}")
    console.print(f"  Total in : [bold]${pos['spent_up']+pos['spent_down']:.2f}[/bold]")
    console.print(f"  Payout   : [bold]${pos['target_payout']:.2f}[/bold]  (whichever side wins)")
    console.print(f"  Locked in: [bold green]+${edge_dollar:.2f}[/bold green]  ({pos['edge_pct']:.2f}% guaranteed, 0% maker fee)")
    console.print()


def _print_resolution(pos: dict, winner: str, pnl: float) -> None:
    color = "green" if pnl >= 0 else "red"
    sign  = "+" if pnl >= 0 else ""
    console.rule(f"[bold {color}]RESOLVED  {pos['title'][:45]}[/bold {color}]")
    console.print(f"  Winner : {winner}")
    console.print(f"  Spent  : ${pos['spent_up']+pos['spent_down']:.2f}")
    console.print(f"  Payout : ${pos['target_payout']:.2f}  (maker fee 0%)")
    console.print(f"  Net P&L: [{color}]{sign}${pnl:.2f}[/{color}]")
    console.print()


def _print_summary() -> None:
    open_pos = [p for p in positions.values() if not p["resolved"]]
    closed   = [p for p in positions.values() if p["resolved"]]
    color    = "green" if session_pnl >= 0 else "red"
    sign     = "+" if session_pnl >= 0 else ""

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan",
                title="[bold]Open & Closed Positions[/bold]")
    tbl.add_column("Market",   width=42)
    tbl.add_column("Sum",      width=6)
    tbl.add_column("Edge",     width=6)
    tbl.add_column("Spent",    width=9)
    tbl.add_column("Payout",   width=9)
    tbl.add_column("Status",   width=8)
    tbl.add_column("P&L",      width=9)

    for pos in sorted(positions.values(), key=lambda p: p.get("window_end", 0)):
        s     = pos["up_price"] + pos["down_price"]
        spent = pos["spent_up"] + pos["spent_down"]
        if pos["resolved"]:
            pnl_str   = f"+${pos['pnl']:.2f}" if pos["pnl"] >= 0 else f"-${abs(pos['pnl']):.2f}"
            pnl_style = "green" if pos["pnl"] >= 0 else "red"
            tbl.add_row(pos["title"][:40], f"{s:.3f}", f"{pos['edge_pct']:.1f}%",
                        f"${spent:.2f}", f"${pos['target_payout']:.2f}",
                        Text("CLOSED", style="dim"), Text(pnl_str, style=pnl_style))
        else:
            tbl.add_row(pos["title"][:40], f"{s:.3f}", f"{pos['edge_pct']:.1f}%",
                        f"${spent:.2f}", f"${pos['target_payout']:.2f}",
                        Text("OPEN", style="yellow"), Text("pending", style="dim"))

    console.print(tbl)
    console.print(
        f"  Arbs: {arb_count}  Open: {len(open_pos)}  Closed: {len(closed)}  │  "
        f"Session P&L: [{color}]{sign}${session_pnl:.2f}[/{color}]"
    )
    console.print()


# ── Core loop ──────────────────────────────────────────────────────────────────

async def _check_resolutions(client: httpx.AsyncClient) -> None:
    global session_pnl
    for slug, pos in list(positions.items()):
        if pos["resolved"] or time.time() < pos["window_end"]:
            continue
        market = await _fetch_market(client, slug)
        if not market or not market.get("closed"):
            continue
        try:
            raw_prices = market.get("outcomePrices")
            prices     = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
            outcomes   = json.loads(market.get("outcomes", "[]"))
            up_idx     = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
            up_p       = float(prices[up_idx])
            winner     = "Up" if up_p >= 0.99 else "Down" if up_p <= 0.01 else "VOID"
        except Exception:
            winner = "UNKNOWN"

        spent = pos["spent_up"] + pos["spent_down"]
        # With 0% maker fee, payout = target_payout regardless of winner
        pnl              = pos["target_payout"] - spent
        pos["resolved"]  = True
        pos["pnl"]       = pnl
        session_pnl     += pnl
        _print_resolution(pos, winner, pnl)


async def _poll(client: httpx.AsyncClient) -> None:
    global arb_count

    for offset in [0, 1]:
        ts     = _window_ts(offset)
        slug   = _slug(ts)
        market = await _fetch_market(client, slug)
        if not market or market.get("closed"):
            continue

        title      = market.get("question", slug)
        window_end = _parse_end(market)

        # Prefer CLOB best-ask; fall back to AMM midpoint
        clob = await _fetch_clob_prices(client, market)
        if clob:
            ask_up, ask_down = clob
            source = "CLOB"
        else:
            # AMM fallback
            try:
                raw      = json.loads(market.get("outcomePrices", "[]"))
                outcomes = json.loads(market.get("outcomes", "[]"))
                up_idx   = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
                ask_up, ask_down = float(raw[up_idx]), float(raw[1 - up_idx])
                source = "AMM"
            except Exception:
                continue

        s      = ask_up + ask_down
        edge   = 1.0 - s          # maker fee = 0%
        is_arb = s < ARB_THRESHOLD and edge > 0

        _print_odds(title, ask_up, ask_down, is_arb, source)

        if is_arb and slug not in positions:
            target = _target_payout(edge)
            # shares = target (each share pays $1, so target shares → $target payout)
            pos = {
                "title":         title,
                "window_end":    window_end,
                "up_price":      ask_up,
                "down_price":    ask_down,
                "sum":           s,
                "edge_pct":      edge * 100,
                "target_payout": target,
                "shares":        target,
                "spent_up":      target * ask_up,
                "spent_down":    target * ask_down,
                "resolved":      False,
                "pnl":           0.0,
            }
            positions[slug] = pos
            arb_count += 1
            _print_entry(pos)


async def main() -> None:
    console.print("[bold cyan]BTC 15-min Arb — Paper Trader (CLOB)[/bold cyan]")
    console.print(f"  Price source   : CLOB best ask (no API key needed for reads)")
    console.print(f"  Fee model      : 0% maker  (2% taker shown for reference)")
    console.print(f"  Arb trigger    : CLOB ask_up + ask_down < {ARB_THRESHOLD:.2f}")
    console.print(f"  Base payout    : ${BASE_PAYOUT:.0f} per 1% edge  (max ${MAX_PAYOUT:.0f})")
    console.print(f"  Poll           : every {POLL_SECS}s")
    console.print()
    console.rule("[cyan]LIVE FEED[/cyan]")

    async with httpx.AsyncClient() as client:
        last_summary = 0.0
        while True:
            await _check_resolutions(client)
            await _poll(client)
            if time.time() - last_summary > 30:
                if positions:
                    console.rule("[dim]POSITIONS[/dim]")
                    _print_summary()
                last_summary = time.time()
            await asyncio.sleep(POLL_SECS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
        if positions:
            console.rule("FINAL SUMMARY")
            _print_summary()
