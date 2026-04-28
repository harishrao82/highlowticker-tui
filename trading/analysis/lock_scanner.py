#!/usr/bin/env python3
"""
lock_scanner.py — Find all locked (both-sides-filled) positions from today's fills.
Shows per-window, per-coin pairs with time of each fill and guaranteed profit.

Run: python lock_scanner.py
"""
import base64, os, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
import httpx
from rich.console import Console
from rich.table import Table

load_dotenv(Path("/Users/Harish/highlowticker-tui/.env"), override=True)
KALSHI_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None)

con = Console()

def hdrs(method, path):
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": _api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "Content-Type": "application/json"}


def fetch_today_fills():
    today = datetime.now(timezone.utc).date()
    fills, cursor = [], None
    with httpx.Client() as c:
        while True:
            params = {"limit": 100}
            if cursor: params["cursor"] = cursor
            r = c.get(f"{KALSHI_BASE}/portfolio/fills",
                      headers=hdrs("GET", "/trade-api/v2/portfolio/fills"),
                      params=params)
            data  = r.json()
            batch = data.get("fills", [])
            fills.extend(batch)
            cursor = data.get("cursor")
            if not cursor or not batch: break
            oldest = datetime.fromisoformat(batch[-1]["created_time"].replace("Z", "+00:00"))
            if oldest.date() < today: break
    return [f for f in fills
            if datetime.fromisoformat(f["created_time"].replace("Z", "+00:00")).date() == today]


def window_open(ticker):
    """Parse open time (ET) from ticker string."""
    try:
        parts   = ticker.split("-")
        close   = datetime.strptime(parts[1], "%d%b%y%H%M")
        open_et = close - timedelta(minutes=15)
        return open_et.strftime("%H:%M")
    except Exception:
        return ticker


def sym(ticker):
    for s in ("BTC", "ETH", "SOL", "XRP"):
        if f"KX{s}" in ticker: return s
    return "?"


def main():
    con.print("[bold cyan]Lock Scanner — fetching today's fills…[/bold cyan]")
    fills = fetch_today_fills()
    con.print(f"[dim]Found {len(fills)} total fills today[/dim]\n")

    # Group: ticker → price → list of (ts, side)
    by_ticker: dict[str, dict[float, list]] = defaultdict(lambda: defaultdict(list))

    for f in fills:
        ticker = f["ticker"]
        side   = f["side"]
        price  = float(f["yes_price_dollars"] if side == "yes" else f["no_price_dollars"])
        ts     = datetime.fromisoformat(f["created_time"].replace("Z", "+00:00"))
        ts_et  = ts + timedelta(hours=-4)
        by_ticker[ticker][round(price, 2)].append((ts_et, side))

    # Find locks: price levels with both YES and NO filled
    locks = []  # (ticker, price, yes_time, no_time, locked_profit)

    for ticker, prices in by_ticker.items():
        for price, entries in prices.items():
            sides = {e[1] for e in entries}
            if "yes" in sides and "no" in sides:
                yes_t = min(e[0] for e in entries if e[1] == "yes")
                no_t  = min(e[0] for e in entries if e[1] == "no")
                profit = round(1.0 - price * 2, 2)
                if profit > 0:
                    locks.append((ticker, price, yes_t, no_t, profit))

    if not locks:
        con.print("[yellow]No locked positions found today.[/yellow]")
        return

    # Sort by ticker, then price descending
    locks.sort(key=lambda x: (x[0], -x[1]))

    # Group by window for display
    by_window: dict[str, list] = defaultdict(list)
    for lock in locks:
        key = f"{sym(lock[0])}  {window_open(lock[0])}"
        by_window[key].append(lock)

    total_locked = 0.0

    for window_key, wlocks in sorted(by_window.items()):
        tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        tbl.add_column("Price",    width=7,  justify="left")
        tbl.add_column("YES fill", width=11, justify="left")
        tbl.add_column("NO fill",  width=11, justify="left")
        tbl.add_column("Gap",      width=8,  justify="right")
        tbl.add_column("Locked",   width=8,  justify="right")

        window_total = 0.0
        for _, price, yes_t, no_t, profit in wlocks:
            gap_s  = abs((yes_t - no_t).total_seconds())
            gap    = f"{int(gap_s)}s"
            # colour gap: green=fast reversal, yellow=slow
            gap_col = "green" if gap_s < 120 else "yellow"
            tbl.add_row(
                f"[bold]${price:.2f}[/bold]",
                yes_t.strftime("%H:%M:%S"),
                no_t.strftime("%H:%M:%S"),
                f"[{gap_col}]{gap}[/{gap_col}]",
                f"[bold green]+${profit:.2f}[/bold green]",
            )
            window_total += profit

        total_locked += window_total
        con.print(f"[bold]{window_key}[/bold]   {len(wlocks)} locks   "
                  f"[bold green]+${window_total:.2f} guaranteed[/bold green]")
        con.print(tbl)

    con.print(f"\n[bold cyan]Total locked profit today: [green]+${total_locked:.2f}[/green] "
              f"across {len(locks)} pairs[/bold cyan]")
    con.print("[dim](Guaranteed regardless of outcome — one side always pays $1.00)[/dim]")


if __name__ == "__main__":
    main()
