#!/usr/bin/env python3
"""
expire_backtest_simple.py — Backtest for expire_maker_simple.py

Per window, per symbol: resting YES+NO limit buys at EVERY cent from
LOW_LIMIT to HIGH_LIMIT. No cancel, no replenish. Both sides stay open.

Per price level across all windows:
  - How many windows did both YES+NO fill (LOCK)?
  - Lock rate = locks / windows_with_any_fill
  - Lock profit = locks × (1 - 2×price)
  - Single loss  = singles that lost × price
  - Net = lock_profit - single_losses + single_wins

Run:  python expire_backtest_simple.py
"""
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
from rich.console import Console
from rich.table import Table

DB         = Path.home() / ".btc_windows.db"
SYMBOLS    = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}
LOW_LIMIT  = 0.02
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
    Simple: YES+NO at every price. No cancel, no replenish.
    Returns list of (price, type, pnl, t1, t2):
      LOCK -> t1 = first fill sec, t2 = second fill sec
      YES/NO -> t1 = fill sec, t2 = None
      NONE -> t1 = t2 = None
    """
    results = []
    for p in PRICES:
        yes_mask = yes_arr <= p
        no_mask  = no_arr  <= p
        yes_hit  = yes_mask.any()
        no_hit   = no_mask.any()
        yes_t = int(np.argmax(yes_mask)) if yes_hit else None
        no_t  = int(np.argmax(no_mask))  if no_hit  else None

        if yes_hit and no_hit:
            first_t  = min(yes_t, no_t)
            second_t = max(yes_t, no_t)
            pnl = round(1.0 - p * 2, 2)
            results.append((p, "LOCK", pnl, first_t, second_t))
        elif yes_hit:
            won = (winner == "yes")
            pnl = round(1.0 - p, 2) if won else round(-p, 2)
            results.append((p, "YES", pnl, yes_t, None))
        elif no_hit:
            won = (winner == "no")
            pnl = round(1.0 - p, 2) if won else round(-p, 2)
            results.append((p, "NO", pnl, no_t, None))
        else:
            results.append((p, "NONE", 0.0, None, None))
    return results


# ── Collect data ─────────────────────────────────────────────────────────────

SLOTS = ["all", "8am-4pm", "4pm-8am"]

def empty_price_stats():
    return {p: {"locks": 0, "yes_only": 0, "no_only": 0, "total": 0,
                "lock_profit": 0.0, "single_pnl": 0.0,
                "lock_first_times": [], "lock_second_times": [],
                "single_times": []}
            for p in PRICES}

# stats[slot][sym][price]
stats: dict[str, dict[str, dict[float, dict]]] = {
    slot: {sym: empty_price_stats() for sym in SYMBOLS}
    for slot in SLOTS
}

window_count: dict[str, dict[str, int]] = {
    slot: {sym: 0 for sym in SYMBOLS} for slot in SLOTS
}

def get_slot(window_start_ts: int) -> str:
    dt_et = datetime.utcfromtimestamp(window_start_ts) + timedelta(hours=-4)
    hour  = dt_et.hour
    return "8am-4pm" if 8 <= hour < 16 else "4pm-8am"

def record(slot, sym, price, typ, pnl, t1, t2):
    s = stats[slot][sym][price]
    if typ == "LOCK":
        s["locks"] += 1
        s["total"] += 1
        s["lock_profit"] = round(s["lock_profit"] + pnl, 4)
        s["lock_first_times"].append(t1)
        s["lock_second_times"].append(t2)
    elif typ in ("YES", "NO"):
        if typ == "YES":
            s["yes_only"] += 1
        else:
            s["no_only"] += 1
        s["total"] += 1
        s["single_pnl"] = round(s["single_pnl"] + pnl, 4)
        s["single_times"].append(t1)

for sym, prefix in SYMBOLS.items():
    rows = db.execute(
        "SELECT w.id, w.winner, w.window_start_ts FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts",
        (prefix + "%",)
    ).fetchall()

    window_count["all"][sym] = len(rows)

    for wid, winner, wts in rows:
        slot = get_slot(wts)
        window_count[slot][sym] = window_count[slot].get(sym, 0) + 1

        yes_arr, no_arr = load_arrays(wid)
        results = simulate_window(yes_arr, no_arr, winner)

        for price, typ, pnl, t1, t2 in results:
            record("all", sym, price, typ, pnl, t1, t2)
            record(slot, sym, price, typ, pnl, t1, t2)

db.close()

# ── Print tables ─────────────────────────────────────────────────────────────

def fmt_sec(sec):
    if sec is None:
        return "—"
    s = int(round(sec))
    return f"{s//60}:{s%60:02d}"

def fmt_time_stats(times):
    if not times:
        return "—"
    arr = np.asarray(times)
    avg = fmt_sec(float(arr.mean()))
    med = fmt_sec(float(np.median(arr)))
    lo  = fmt_sec(float(arr.min()))
    hi  = fmt_sec(float(arr.max()))
    return f"avg {avg}\nmed {med}\n{lo}–{hi}"

def print_slot(slot_name, slot_stats, slot_wc):
    con.print(f"\n[bold]{'═' * 60}[/bold]")
    con.print(f"[bold cyan]  {slot_name}[/bold cyan]")
    con.print(f"[bold]{'═' * 60}[/bold]")

    for sym in SYMBOLS:
        n_windows = slot_wc[sym]
        if n_windows == 0:
            continue
        con.print(f"\n[bold cyan]{sym}[/bold cyan]  ({n_windows} windows)\n")

        tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        tbl.add_column("Price", width=7, justify="left")
        tbl.add_column("Locks", width=12, justify="right")
        tbl.add_column("Lock Rate", width=10, justify="right")
        tbl.add_column("1st @", width=13, justify="right")
        tbl.add_column("2nd @", width=13, justify="right")
        tbl.add_column("Lock $", width=10, justify="right")
        tbl.add_column("Singles", width=12, justify="right")
        tbl.add_column("Sgl @", width=13, justify="right")
        tbl.add_column("Single $", width=10, justify="right")
        tbl.add_column("Net $", width=10, justify="right")

        total_lock_profit = 0.0
        total_single_pnl  = 0.0

        for p in reversed(PRICES):
            s = slot_stats[sym][p]
            if s["total"] == 0:
                continue

            locks = s["locks"]
            rate  = locks / s["total"] if s["total"] > 0 else 0
            net   = round(s["lock_profit"] + s["single_pnl"], 2)

            total_lock_profit += s["lock_profit"]
            total_single_pnl  += s["single_pnl"]

            rc = "green" if rate >= 0.5 else "yellow" if rate >= 0.3 else "red"
            nc = "green" if net >= 0 else "red"
            lc = "green" if s["lock_profit"] > 0 else "dim"
            sc = "green" if s["single_pnl"] >= 0 else "red"

            tbl.add_row(
                f"[bold]${p:.2f}[/bold]",
                f"{locks}/{s['total']}",
                f"[{rc}]{rate:.0%}[/{rc}]",
                f"[dim]{fmt_time_stats(s['lock_first_times'])}[/dim]",
                f"[dim]{fmt_time_stats(s['lock_second_times'])}[/dim]",
                f"[{lc}]+${s['lock_profit']:.2f}[/{lc}]",
                f"Y:{s['yes_only']} N:{s['no_only']}",
                f"[dim]{fmt_time_stats(s['single_times'])}[/dim]",
                f"[{sc}]{s['single_pnl']:+.2f}[/{sc}]",
                f"[{nc}]{net:+.2f}[/{nc}]",
            )

        grand = round(total_lock_profit + total_single_pnl, 2)
        gc = "green" if grand >= 0 else "red"
        tbl.add_row(
            "[bold]TOTAL[/bold]",
            "", "", "", "",
            f"[green]+${total_lock_profit:.2f}[/green]",
            "", "",
            f"[{'green' if total_single_pnl>=0 else 'red'}]{total_single_pnl:+.2f}[/{'green' if total_single_pnl>=0 else 'red'}]",
            f"[{gc}][bold]{grand:+.2f}[/bold][/{gc}]",
        )

        con.print(tbl)

    all_lock = sum(
        s["lock_profit"]
        for sym in SYMBOLS for s in slot_stats[sym].values()
    )
    all_single = sum(
        s["single_pnl"]
        for sym in SYMBOLS for s in slot_stats[sym].values()
    )
    all_net = round(all_lock + all_single, 2)
    nc = "green" if all_net >= 0 else "red"
    con.print(
        f"\n[bold]{slot_name} all coins:[/bold]"
        f"  Lock profit: [green]+${all_lock:.2f}[/green]"
        f"  Single P&L: [{'green' if all_single>=0 else 'red'}]{all_single:+.2f}[/{'green' if all_single>=0 else 'red'}]"
        f"  [bold][{nc}]Net: {all_net:+.2f}[/{nc}][/bold]"
    )

for slot in SLOTS:
    print_slot(slot, stats[slot], window_count[slot])
