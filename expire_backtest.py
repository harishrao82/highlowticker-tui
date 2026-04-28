#!/usr/bin/env python3
"""
expire_backtest.py — Simulate expire_maker.py on historical data.

Per window, per symbol: resting YES+NO limit buys at EVERY cent from
LOW_LIMIT to HIGH_LIMIT. Whichever side hits first at each price level
gets filled → hold to expiry. Multiple fills per window (one per price level).

Run:  python expire_backtest.py
"""
import sqlite3
from datetime import datetime
from pathlib import Path
import numpy as np
from rich.console import Console
from rich.table import Table

DB         = Path.home() / ".btc_windows.db"
SYMBOLS    = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}
LOW_LIMIT  = 0.01
HIGH_LIMIT = 0.49

db  = sqlite3.connect(DB)
con = Console()

PRICES = [round(p / 100, 2) for p in range(int(LOW_LIMIT * 100), int(HIGH_LIMIT * 100) + 1)]


def load_arrays(wid):
    ticks = db.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks "
        "WHERE window_id=? AND elapsed_sec < 900 ORDER BY elapsed_sec", (wid,)
    ).fetchall()
    yes_arr = np.full(900, np.nan)
    no_arr  = np.full(900, np.nan)
    for sec, ya, na in ticks:
        if 0 <= sec < 900:
            if ya is not None: yes_arr[sec] = ya
            if na is not None: no_arr[sec]  = na
    ly, ln = 0.5, 0.5
    for i in range(900):
        if np.isnan(yes_arr[i]): yes_arr[i] = ly
        else: ly = yes_arr[i]
        if np.isnan(no_arr[i]):  no_arr[i]  = ln
        else: ln = no_arr[i]
    return yes_arr, no_arr


def simulate_window(yes_arr, no_arr, winner):
    """
    For each price level, find which side fills first → P&L.
    Returns total_pnl, fills list [(price, side, pnl), ...]
    """
    total_pnl = 0.0
    fills = []
    for p in PRICES:
        yes_mask = yes_arr <= p
        no_mask  = no_arr  <= p
        t_yes = int(np.argmax(yes_mask)) if yes_mask.any() else 9999
        t_no  = int(np.argmax(no_mask))  if no_mask.any()  else 9999
        if t_yes == 9999 and t_no == 9999:
            continue
        if t_yes <= t_no:
            side = "YES"
            won  = (winner == "yes")
        else:
            side = "NO"
            won  = (winner == "no")
        pnl = round(1.0 - p, 2) if won else round(-p, 2)
        total_pnl += pnl
        fills.append((p, side, pnl))
    return round(total_pnl, 2), fills


# ── Collect per-window results ────────────────────────────────────────────────
all_wts: set[int] = set()
timeline: dict[int, dict[str, float | None]] = {}

# Per-price-level accumulators: {sym: {price: {trades, yes_fills, no_fills, wins, net}}}
price_stats: dict[str, dict[float, dict]] = {
    sym: {p: {"trades": 0, "yes": 0, "no": 0, "wins": 0, "net": 0.0}
          for p in PRICES}
    for sym in SYMBOLS
}

for sym, prefix in SYMBOLS.items():
    rows = db.execute(
        "SELECT w.id, w.winner, w.window_start_ts FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts",
        (prefix + "%",)
    ).fetchall()

    for wid, winner, wts in rows:
        all_wts.add(wts)
        yes_arr, no_arr = load_arrays(wid)
        total_pnl, fills = simulate_window(yes_arr, no_arr, winner)

        if wts not in timeline:
            timeline[wts] = {}
        timeline[wts][sym] = total_pnl if fills else None

        for price, side, pnl in fills:
            s = price_stats[sym][price]
            s["trades"] += 1
            s["yes"]    += 1 if side == "YES" else 0
            s["no"]     += 1 if side == "NO"  else 0
            s["wins"]   += 1 if pnl > 0 else 0
            s["net"]     = round(s["net"] + pnl, 4)

db.close()

# ── Print table ───────────────────────────────────────────────────────────────
syms = list(SYMBOLS.keys())
con.print(
    f"\n[bold cyan]Expire Maker Backtest[/bold cyan]"
    f"  limits {LOW_LIMIT:.2f}–{HIGH_LIMIT:.2f}  "
    f"({len(PRICES)} price levels per window)\n"
)

tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
tbl.add_column("Window", width=14, justify="left")
for s in syms:
    tbl.add_column(s, width=9, justify="right")
tbl.add_column("Net", width=9, justify="right")

running = {s: 0.0 for s in syms}

for wts in sorted(all_wts):
    label = datetime.fromtimestamp(wts).strftime("%m-%d %H:%M")
    row   = timeline.get(wts, {})
    cells = []
    net   = 0.0
    traded = False

    for s in syms:
        pnl = row.get(s)
        if pnl is None:
            cells.append("[dim]–[/dim]")
        else:
            running[s] = round(running[s] + pnl, 2)
            net = round(net + pnl, 2)
            traded = True
            c = "green" if pnl > 0 else "red"
            cells.append(f"[{c}]{pnl:+.2f}[/{c}]")

    net_str = f"[{'green' if net>0 else 'red'}]{net:+.2f}[/{'green' if net>0 else 'red'}]" if traded else "[dim]–[/dim]"
    tbl.add_row(label, *cells, net_str)

# Totals
grand = round(sum(running.values()), 2)
gc = "green" if grand > 0 else "red"
total_cells = [
    f"[{'green' if v>0 else 'red'}]{v:+.2f}[/{'green' if v>0 else 'red'}]"
    for v in (running[s] for s in syms)
]
tbl.add_row("[bold]TOTAL[/bold]", *total_cells,
            f"[{gc}][bold]{grand:+.2f}[/bold][/{gc}]")

con.print(tbl)

# ── Per-price-level table ─────────────────────────────────────────────────────
con.print(
    f"\n[bold cyan]Win Rate by Limit Price[/bold cyan]"
    f"  (real fills only — each cell: trades · win% vs break-even · net)\n"
)

ptbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
ptbl.add_column("Price", width=7, justify="left")
for s in syms:
    ptbl.add_column(s, width=22, justify="right")

# Totals per sym across all prices
sym_totals = {s: {"trades": 0, "wins": 0, "net": 0.0} for s in syms}

for p in reversed(PRICES):
    cells = []
    any_data = False
    for s in syms:
        st = price_stats[s][p]
        if st["trades"] == 0:
            cells.append("[dim]–[/dim]")
            continue
        any_data = True
        win_pct  = st["wins"] / st["trades"]
        edge     = win_pct - p          # positive = above break-even
        ec       = "green" if edge >= 0 else "red"
        nc       = "green" if st["net"] >= 0 else "red"
        sym_totals[s]["trades"] += st["trades"]
        sym_totals[s]["wins"]   += st["wins"]
        sym_totals[s]["net"]     = round(sym_totals[s]["net"] + st["net"], 4)
        cells.append(
            f"{st['trades']}t  "
            f"[{ec}]{win_pct:.0%}[/{ec}][dim]/{p:.0%}[/dim]  "
            f"[{nc}]{st['net']:+.2f}[/{nc}]"
        )
    if any_data:
        ptbl.add_row(f"[bold]${p:.2f}[/bold]", *cells)

# Totals row
total_cells = []
for s in syms:
    t = sym_totals[s]
    if t["trades"] == 0:
        total_cells.append("–")
        continue
    wp  = t["wins"] / t["trades"]
    nc  = "green" if t["net"] >= 0 else "red"
    total_cells.append(
        f"{t['trades']}t  "
        f"[bold]{wp:.0%}[/bold]  "
        f"[{nc}][bold]{t['net']:+.2f}[/bold][/{nc}]"
    )
ptbl.add_row("[bold]TOTAL[/bold]", *total_cells)

con.print(ptbl)
