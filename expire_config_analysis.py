#!/usr/bin/env python3
"""
expire_config_analysis.py — recommend (price range, cancel cutoff) for expire_maker_simple.

Cutoff rule: at time T, cancel ALL unfilled orders on both sides.
Fills only count if they happen before T. Max window length = T, not 900.

Outputs:
  1) Per-coin table: paired% at each (price, cutoff T)
  2) Per-coin summary: for each cutoff, total locks/singles/net $/window across
     prices with ≥X% paired rate, then recommended [low,high] + best T.
"""
import sqlite3
from pathlib import Path
import numpy as np
from rich.console import Console
from rich.table import Table

DB       = Path.home() / ".btc_windows.db"
SYMBOLS  = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}
PRICES   = [round(p / 100, 2) for p in range(2, 50)]
CUTOFFS  = [180, 300, 420, 540, 660, 780, 900]  # seconds (3m..15m)
PAIRED_THRESHOLD = 0.60  # only include a price in recommended range if ≥60% paired

con = Console()
db  = sqlite3.connect(DB)


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


# ── Phase 1: extract (yes_t, no_t, winner) per (sym, price, window) ──────────
data: dict = {sym: {p: [] for p in PRICES} for sym in SYMBOLS}
window_counts: dict[str, int] = {}

for sym, prefix in SYMBOLS.items():
    rows = db.execute(
        "SELECT w.id, w.winner FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts",
        (prefix + "%",)
    ).fetchall()
    window_counts[sym] = len(rows)
    con.print(f"[dim]{sym}: loading {len(rows)} windows…[/dim]")
    for wid, winner in rows:
        yes_arr, no_arr = load_arrays(wid)
        for p in PRICES:
            yes_mask = yes_arr <= p
            no_mask  = no_arr  <= p
            yes_t = int(np.argmax(yes_mask)) if yes_mask.any() else 10_000
            no_t  = int(np.argmax(no_mask))  if no_mask.any()  else 10_000
            data[sym][p].append((yes_t, no_t, winner))

db.close()


def evaluate(sym: str, p: float, T: int):
    """Simulate hard cutoff at time T. Only fills before T count."""
    locks = singles = nones = 0
    net = 0.0
    for yes_t, no_t, winner in data[sym][p]:
        yes_hit = yes_t < T
        no_hit  = no_t  < T
        if yes_hit and no_hit:
            locks += 1
            net += (1.0 - 2 * p)
        elif yes_hit:
            singles += 1
            net += (1.0 - p) if winner == "yes" else -p
        elif no_hit:
            singles += 1
            net += (1.0 - p) if winner == "no" else -p
        else:
            nones += 1
    return locks, singles, nones, round(net, 2)


# ── Phase 2: paired% grid per coin ───────────────────────────────────────────
for sym in SYMBOLS:
    con.print(f"\n[bold cyan]{sym}[/bold cyan]  paired% = locks / (locks + singles)  "
              f"[dim]({window_counts[sym]} windows)[/dim]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    tbl.add_column("Price", width=6)
    for T in CUTOFFS:
        tbl.add_column(f"T={T//60}m", width=8, justify="right")
    for p in reversed(PRICES):
        row = [f"[bold]${p:.2f}[/bold]"]
        for T in CUTOFFS:
            locks, singles, nones, net = evaluate(sym, p, T)
            both = locks + singles
            if both == 0:
                row.append("—")
            else:
                pct = locks / both
                col = "green" if pct >= 0.75 else "yellow" if pct >= 0.5 else "red"
                row.append(f"[{col}]{pct:.0%}[/{col}] {locks}")
        tbl.add_row(*row)
    con.print(tbl)


# ── Phase 3: per-(coin,T) net $/window per individual price ─────────────────
con.print(f"\n[bold]Net $/window per price level[/bold]  "
          f"[dim](green = positive EV at that cutoff)[/dim]")

for sym in SYMBOLS:
    con.print(f"\n[bold cyan]{sym}[/bold cyan]  ({window_counts[sym]} windows)")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    tbl.add_column("Price", width=6)
    for T in CUTOFFS:
        tbl.add_column(f"T={T//60}m", width=9, justify="right")
    for p in reversed(PRICES):
        row = [f"[bold]${p:.2f}[/bold]"]
        for T in CUTOFFS:
            locks, singles, nones, net = evaluate(sym, p, T)
            per_win = net / window_counts[sym]
            if abs(per_win) < 0.001:
                row.append("[dim]  0[/dim]")
            else:
                col = "green" if per_win > 0 else "red"
                row.append(f"[{col}]{per_win:+.3f}[/{col}]")
        tbl.add_row(*row)
    con.print(tbl)

# ── Phase 4: best-EV config per coin (include every positive-EV price) ──────
con.print(f"\n[bold]Best-EV config per coin[/bold]  "
          f"[dim](include every price that is net positive at cutoff T)[/dim]\n")

for sym in SYMBOLS:
    con.print(f"[bold cyan]{sym}[/bold cyan]  ({window_counts[sym]} windows)")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    tbl.add_column("T")
    tbl.add_column("Profitable prices", justify="right")
    tbl.add_column("Range", justify="right")
    tbl.add_column("Locks", justify="right")
    tbl.add_column("Singles", justify="right")
    tbl.add_column("Paired %", justify="right")
    tbl.add_column("Net $/win", justify="right")

    best = None
    for T in CUTOFFS:
        pos_prices = []
        total_locks = 0
        total_singles = 0
        total_net = 0.0
        for p in PRICES:
            locks, singles, nones, net = evaluate(sym, p, T)
            if net > 0:
                pos_prices.append(p)
                total_locks  += locks
                total_singles += singles
                total_net    += net
        if not pos_prices:
            tbl.add_row(f"{T//60}m", "0", "—", "0", "0", "—", "$0.000")
            continue
        both_total  = total_locks + total_singles
        paired_pct  = total_locks / both_total if both_total else 0
        net_per_win = total_net / window_counts[sym]
        pos_prices_sorted = sorted(pos_prices)
        range_str = f"${pos_prices_sorted[0]:.2f}–${pos_prices_sorted[-1]:.2f}"
        tbl.add_row(
            f"{T//60}m",
            str(len(pos_prices)),
            range_str,
            str(total_locks),
            str(total_singles),
            f"{paired_pct:.0%}",
            f"${net_per_win:+.3f}",
        )
        if best is None or net_per_win > best[0]:
            best = (net_per_win, T, sorted(pos_prices))

    con.print(tbl)
    if best:
        net_per_win, T, prices = best
        con.print(f"  [green]→ Best:  T={T}s ({T//60}m)  "
                  f"net ${net_per_win:+.3f}/window[/green]")
        con.print(f"  [green]   prices: {[f'{p:.2f}' for p in prices]}[/green]\n")
