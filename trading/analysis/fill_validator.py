#!/usr/bin/env python3
"""
fill_validator.py — Validate that all expected price levels filled for each
symbol in each 15-min window today.

For each (symbol, window), shows fills high→low and highlights any gaps.

Run:  python fill_validator.py
"""
import base64, os, time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv(Path("/Users/Harish/highlowticker-tui/.env"), override=True)

KALSHI_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
ET_OFFSET    = timedelta(hours=-4)

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

# Expected price ranges per symbol (must match expire_maker.py config)
EXPECTED = {
    "BTC": (0.02, 0.40),
    "ETH": (0.02, 0.40),
    "SOL": (0.02, 0.43),
    "XRP": (0.02, 0.34),
}

console = Console()


def hdrs(method, path):
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
        "Content-Type":            "application/json",
    }


def sym_from_ticker(ticker):
    for s in ("BTC", "ETH", "SOL", "XRP"):
        if f"KX{s}" in ticker:
            return s
    return "?"


def window_label(ticker):
    """Extract HH:MM ET open time from ticker. Format: KXBTC15M-26APR090845-45"""
    try:
        # parts[1] = "26APR090845", parts[2] = "45"
        # The datetime in parts[1] is the close time
        parts   = ticker.split("-")
        dt_part = parts[1]   # e.g. "26APR090845"
        close   = datetime.strptime(dt_part, "%d%b%y%H%M")
        open_   = close - timedelta(minutes=15)
        return open_.strftime("%H:%M")
    except Exception:
        return ticker


# ── Fetch today's fills ───────────────────────────────────────────────────────

console.print("[bold cyan]Fetching today's fills…[/bold cyan]")
today  = datetime.now(timezone.utc).date()
fills  = []
cursor = None

with httpx.Client() as c:
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        r      = c.get(f"{KALSHI_BASE}/portfolio/fills",
                       headers=hdrs("GET", "/trade-api/v2/portfolio/fills"),
                       params=params)
        data   = r.json()
        batch  = data.get("fills", [])
        fills.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break
        oldest = datetime.fromisoformat(batch[-1]["created_time"].replace("Z", "+00:00"))
        if oldest.date() < today:
            break

fills = [f for f in fills
         if datetime.fromisoformat(f["created_time"].replace("Z", "+00:00")).date() == today]

if not fills:
    console.print("[yellow]No fills today.[/yellow]")
    raise SystemExit

console.print(f"  {len(fills)} fills found\n")

# ── Group by (sym, window) ────────────────────────────────────────────────────
# key = (sym, window_label)  value = {price: [fill_times]}

groups: dict[tuple, dict[float, list[str]]] = defaultdict(lambda: defaultdict(list))

for f in fills:
    ticker = f["ticker"]
    sym    = sym_from_ticker(ticker)
    win    = window_label(ticker)
    side   = f["side"]
    price  = float(f["yes_price_dollars"] if side == "yes" else f["no_price_dollars"])
    ts     = datetime.fromisoformat(f["created_time"].replace("Z", "+00:00"))
    ts_et  = ts.astimezone(timezone(ET_OFFSET)).strftime("%H:%M:%S")
    price  = round(price, 2)
    groups[(sym, win)][price].append(ts_et)

# ── Validate and print ────────────────────────────────────────────────────────

for (sym, win) in sorted(groups.keys()):
    filled_prices = groups[(sym, win)]
    lo, hi        = EXPECTED.get(sym, (0.02, 0.45))
    expected_set  = {round(p / 100, 2) for p in range(int(lo * 100), int(hi * 100) + 1)}
    filled_set    = set(filled_prices.keys())
    missing       = sorted(expected_set - filled_set, reverse=True)
    extra         = sorted(filled_set - expected_set, reverse=True)

    status = "[green]✓ COMPLETE[/green]" if not missing else f"[red]✗ {len(missing)} MISSING[/red]"
    console.print(f"[bold]{sym}  {win} ET[/bold]  {status}")

    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    tbl.add_column("Price",   width=7,  justify="right")
    tbl.add_column("Status",  width=10, justify="left")
    tbl.add_column("Fill time(s)", width=30, justify="left")

    # All prices high→low
    all_prices = sorted(expected_set | filled_set, reverse=True)
    for p in all_prices:
        p = round(p, 2)
        if p in filled_prices:
            times = ", ".join(sorted(filled_prices[p]))
            if p not in expected_set:
                # filled but not expected (outside configured range)
                tbl.add_row(f"${p:.2f}", "[yellow]EXTRA[/yellow]", f"[dim]{times}[/dim]")
            else:
                tbl.add_row(f"${p:.2f}", "[green]FILLED[/green]", f"[dim]{times}[/dim]")
        else:
            tbl.add_row(f"[red]${p:.2f}[/red]", "[red]MISSING[/red]", "")

    console.print(tbl)

    if missing:
        console.print(f"  [red]Missing: {', '.join(f'${p:.2f}' for p in missing)}[/red]")
    if extra:
        console.print(f"  [yellow]Outside range: {', '.join(f'${p:.2f}' for p in extra)}[/yellow]")
    console.print()
