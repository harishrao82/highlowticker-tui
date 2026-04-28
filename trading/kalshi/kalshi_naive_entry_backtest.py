#!/usr/bin/env python3
"""
kalshi_naive_entry_backtest.py — naive "buy a side at $0.50–$0.65 at time T"
backtest on Kalshi 15-min tick data.

For each resolved window, at elapsed time T, look at yes_ask and no_ask.
If one side is in [LOW, HIGH], buy it with fixed stake, hold to settlement.

Tests multiple T values and price bands to see if any naive signal has edge.
"""
import sqlite3
from pathlib import Path
from statistics import mean, median
from rich.console import Console
from rich.table import Table
import random

DB = Path.home() / ".btc_windows.db"
con = Console()
STAKE = 100.0

db = sqlite3.connect(DB)

windows = db.execute(
    "SELECT w.id, w.ticker, w.winner FROM windows w "
    "WHERE w.winner IS NOT NULL "
    "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100"
).fetchall()

con.print(f"[bold cyan]{len(windows)} resolved Kalshi windows[/bold cyan]\n")

def coin_of(ticker: str) -> str:
    # KXBTC15M-... → BTC
    return ticker[2:5]

# Pre-load ticks per window, keyed by elapsed_sec
tick_cache = {}
for wid, ticker, winner in windows:
    ticks = db.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks "
        "WHERE window_id=? AND elapsed_sec < 900",
        (wid,)
    ).fetchall()
    tick_cache[wid] = {t[0]: (t[1], t[2]) for t in ticks}


def sample_at_time(wid: int, T: int) -> tuple[float, float] | None:
    """Get (yes_ask, no_ask) at elapsed time T, using the last available tick ≤ T."""
    ticks = tick_cache[wid]
    # walk backwards to find last tick at or before T
    for t in range(T, -1, -1):
        if t in ticks:
            ya, na = ticks[t]
            if ya is not None and na is not None:
                return ya, na
    return None


def backtest(T: int, lo: float, hi: float) -> dict:
    results = []
    for wid, ticker, winner in windows:
        sample = sample_at_time(wid, T)
        if not sample: continue
        ya, na = sample
        # pick the side in the band (prefer yes if both match)
        side = None; entry = None
        if lo <= ya <= hi:
            side, entry = "yes", ya
        elif lo <= na <= hi:
            side, entry = "no", na
        else:
            continue
        won = (side == winner)
        pnl = STAKE * ((1 - entry) / entry) if won else -STAKE
        results.append({
            "coin": coin_of(ticker),
            "side": side, "entry": entry,
            "winner": winner, "won": won, "pnl": pnl,
        })
    if not results:
        return {"n": 0}
    pnls = [r["pnl"] for r in results]
    wins = sum(1 for r in results if r["won"])
    total = sum(pnls)
    staked = STAKE * len(results)
    return {
        "n": len(results),
        "win_rate": wins / len(results),
        "total_pnl": total,
        "staked": staked,
        "agg_return": total / staked,
        "mean_pnl": mean(pnls),
        "median_pnl": median(pnls),
        "results": results,
    }


# ── 1. Scan T and band combinations ────────────────────────────────────────
con.print("[bold]Scan: entry time T × price band[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
tbl.add_column("T")
tbl.add_column("Band")
tbl.add_column("N", justify="right")
tbl.add_column("Win%", justify="right")
tbl.add_column("Total P&L", justify="right")
tbl.add_column("Return%", justify="right")

times = [30, 60, 120, 180, 300]
bands = [(0.40, 0.55), (0.50, 0.65), (0.55, 0.70), (0.45, 0.60), (0.35, 0.50)]

for T in times:
    for lo, hi in bands:
        r = backtest(T, lo, hi)
        if r["n"] == 0:
            continue
        col = "green" if r["agg_return"] > 0.02 else "red" if r["agg_return"] < -0.02 else "white"
        tbl.add_row(
            f"{T}s", f"${lo:.2f}–${hi:.2f}",
            str(r["n"]), f"{r['win_rate']:.0%}",
            f"${r['total_pnl']:+,.0f}",
            f"[{col}]{r['agg_return']*100:+.2f}%[/{col}]",
        )
con.print(tbl)


# ── 2. Detailed view: the Scallops sweet-spot (T=60s, 0.50–0.65) ──────────
con.print("\n[bold]Detailed: T=60s, band $0.50–$0.65 (the Scallops sweet spot)[/bold]")
r = backtest(60, 0.50, 0.65)
if r["n"]:
    con.print(f"  N={r['n']}  win%={r['win_rate']:.1%}  "
              f"total P&L ${r['total_pnl']:+,.0f}  "
              f"staked ${r['staked']:,.0f}  "
              f"return {r['agg_return']*100:+.2f}%")

    # split by coin
    by_coin = {}
    for x in r["results"]:
        by_coin.setdefault(x["coin"], []).append(x)
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","N","Win%","Total P&L","Return%"):
        tbl.add_column(col, justify="right")
    for coin in sorted(by_coin):
        rs = by_coin[coin]
        wins = sum(1 for x in rs if x["won"])
        pnl = sum(x["pnl"] for x in rs)
        staked = STAKE * len(rs)
        col = "green" if pnl > 0 else "red"
        tbl.add_row(coin, str(len(rs)), f"{wins/len(rs):.0%}",
                    f"[{col}]${pnl:+,.0f}[/{col}]",
                    f"{pnl/staked*100:+.2f}%")
    con.print(tbl)


# ── 3. Baseline: buy EVERY window at whatever side is closest to 0.50 ──
con.print("\n[bold]Baseline: at T=60s, buy whichever side is closest to 0.50[/bold]")
baseline_results = []
for wid, ticker, winner in windows:
    sample = sample_at_time(wid, 60)
    if not sample: continue
    ya, na = sample
    if ya is None or na is None: continue
    # pick side closest to 0.50
    if abs(ya - 0.5) < abs(na - 0.5):
        side, entry = "yes", ya
    else:
        side, entry = "no", na
    if entry <= 0 or entry >= 1: continue
    won = (side == winner)
    pnl = STAKE * ((1 - entry) / entry) if won else -STAKE
    baseline_results.append({
        "coin": coin_of(ticker), "entry": entry, "pnl": pnl, "won": won,
    })

if baseline_results:
    total_pnl = sum(x["pnl"] for x in baseline_results)
    wins = sum(1 for x in baseline_results if x["won"])
    staked = STAKE * len(baseline_results)
    con.print(f"  N={len(baseline_results)}  "
              f"win%={wins/len(baseline_results):.1%}  "
              f"total P&L ${total_pnl:+,.0f}  "
              f"return {total_pnl/staked*100:+.2f}%")
    mean_entry = mean(x["entry"] for x in baseline_results)
    con.print(f"  median entry: {median(x['entry'] for x in baseline_results):.3f}  "
              f"mean: {mean_entry:.3f}")


# ── 4. Sanity: what does "always buy YES" and "always buy NO" get us? ────
con.print("\n[bold]Sanity controls at T=60s[/bold]")
for strat in ("always_yes", "always_no"):
    rs = []
    for wid, ticker, winner in windows:
        sample = sample_at_time(wid, 60)
        if not sample: continue
        ya, na = sample
        if strat == "always_yes":
            side, entry = "yes", ya
        else:
            side, entry = "no", na
        if entry is None or entry <= 0 or entry >= 1: continue
        won = (side == winner)
        pnl = STAKE * ((1 - entry)/entry) if won else -STAKE
        rs.append(pnl)
    if rs:
        wins = sum(1 for x in rs if x > 0)
        con.print(f"  {strat:12s}: N={len(rs)}  win%={wins/len(rs):.0%}  "
                  f"total ${sum(rs):+,.0f}  return {sum(rs)/(STAKE*len(rs))*100:+.2f}%")

db.close()
