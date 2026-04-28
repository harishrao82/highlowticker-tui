#!/usr/bin/env python3
"""
kalshi_portfolio_check.py — quick sanity check for the portfolio-value
fetch and the tiered shares-per-entry lookup used by kalshi_momentum_live.

Safe to run anytime (read-only, no orders placed). Uses the same auth +
endpoint as the live trader so if this works, the live trader's balance
fetch works too.
"""
import base64
import os
import time
from datetime import datetime

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()
con = Console()

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
PATH        = "/trade-api/v2/portfolio/balance"

# Must match SHARE_TIERS in kalshi_momentum_live.py
SHARE_TIERS: list[tuple[float, int]] = [
    (500.0, 15),
    (400.0, 12),
    (220.0, 10),
    (150.0,  5),
    (  0.0,  1),
]

def shares_for_portfolio(value: float) -> int:
    for threshold, n in SHARE_TIERS:
        if value >= threshold:
            return n
    return 1


def main() -> None:
    api_key = os.environ["KALSHI_API_KEY"]
    pk = serialization.load_pem_private_key(
        os.environ["KALSHI_API_SECRET"].encode(), password=None
    )
    ts  = str(round(time.time() * 1000))
    msg = ts + "GET" + PATH
    sig = pk.sign(
        msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    headers = {
        "KALSHI-ACCESS-KEY":       api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }

    try:
        r = httpx.get(KALSHI_BASE + "/portfolio/balance",
                      headers=headers, timeout=8)
    except Exception as e:
        con.print(f"[red]request failed: {e}[/red]")
        return

    con.print(f"[dim]status: {r.status_code}[/dim]")
    if r.status_code != 200:
        con.print(f"[red]{r.text[:500]}[/red]")
        return

    j = r.json()
    cash      = float(j.get("balance") or 0) / 100.0
    positions = float(j.get("portfolio_value") or 0) / 100.0
    total     = cash + positions
    updated   = j.get("updated_ts")

    con.print(f"\n[bold cyan]Kalshi Portfolio[/bold cyan]")
    con.print(f"  cash balance        : ${cash:,.2f}")
    con.print(f"  open position value : ${positions:,.2f}")
    con.print(f"  [bold]total portfolio     : ${total:,.2f}[/bold]")
    if updated:
        con.print(f"  updated             : "
                  f"{datetime.fromtimestamp(int(updated))}  "
                  f"(unix {updated})")

    # Tier lookup
    shares = shares_for_portfolio(total)
    con.print(f"\n[bold]Current tier → shares per entry: "
              f"[bold green]{shares}[/bold green][/bold]")

    con.print(f"\n[bold]Share tier table[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    tbl.add_column("Portfolio threshold", justify="right")
    tbl.add_column("Shares / entry", justify="right")
    tbl.add_column("", justify="left")
    for threshold, n in SHARE_TIERS:
        marker = "[green]← you are here[/green]" if n == shares else ""
        if threshold > 0:
            tbl.add_row(f"≥ ${threshold:,.0f}", str(n), marker)
        else:
            prev = SHARE_TIERS[-2][0]
            tbl.add_row(f"< ${prev:,.0f}", str(n), marker)
    con.print(tbl)

    # Show max downside at this tier
    max_entry_px = 0.90
    max_trades_per_window = 4 * 7    # 4 coins × 7 checkpoints (theoretical)
    per_trade_max = max_entry_px * shares
    per_window_max = per_trade_max * max_trades_per_window
    con.print(f"\n[dim]Risk ceiling at this tier:[/dim]")
    con.print(f"  [dim]max per trade:     ${per_trade_max:.2f}[/dim]")
    con.print(f"  [dim]max per window:    ${per_window_max:.2f} "
              f"(all 4 coins × 7 checkpoints, never happens in practice)[/dim]")


if __name__ == "__main__":
    main()
