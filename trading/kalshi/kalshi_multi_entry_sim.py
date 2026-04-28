#!/usr/bin/env python3
"""
kalshi_multi_entry_sim.py — backtest a multi-entry momentum strategy on Kalshi.

For each resolved window, iterate through a set of (T, threshold) checkpoints.
At each checkpoint, if |delta| >= threshold, place a simulated entry using the
yes_ask or no_ask at that time. Each entry settles independently at window end.

We compare:
  (1) single-entry at T=300s, threshold 0.10%
  (2) multi-entry across 5 checkpoints, same or varying thresholds
  (3) multi-entry with COOLDOWN: skip checkpoints that fire the same side
      as the previous one within N seconds (avoid double-up)
  (4) multi-entry with SIDE-CHANGE only: only re-fire when the side flips
"""
import sqlite3
import json
import random
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from rich.console import Console
from rich.table import Table

DB    = Path.home() / ".btc_windows.db"
CACHE = Path.home() / ".kalshi_btc_1m_cache.json"
STAKE = 100.0    # $ per entry
con   = Console()

SYMBOLS = {
    "BTC": ("KXBTC15M", "BTC-USD"),
    "ETH": ("KXETH15M", "ETH-USD"),
    "SOL": ("KXSOL15M", "SOL-USD"),
    "XRP": ("KXXRP15M", "XRP-USD"),
}

# ── Load price cache (populated by the earlier momentum sim) ────────────────
cache: dict[str, dict[int, float]] = {}
if CACHE.exists():
    raw = json.loads(CACHE.read_text())
    for p, m in raw.items():
        cache[p] = {int(k): float(v) for k, v in m.items()}
for p in ("BTC-USD","ETH-USD","SOL-USD","XRP-USD"):
    cache.setdefault(p, {})


def minute_start(ts: int) -> int: return ts - (ts % 60)
def price_at(product: str, ts: int) -> float | None:
    return cache[product].get(minute_start(ts))


# ── Load windows ───────────────────────────────────────────────────────────
db = sqlite3.connect(DB)
all_windows = []
for coin, (prefix, cb) in SYMBOLS.items():
    rows = db.execute(
        "SELECT w.id, w.ticker, w.winner, w.window_start_ts "
        "FROM windows w WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100",
        (prefix + "%",)
    ).fetchall()
    for wid, ticker, winner, wts in rows:
        all_windows.append({
            "wid": wid, "coin": coin, "winner": winner, "wts": wts,
            "product": cb, "ticker": ticker,
        })
con.print(f"[bold cyan]{len(all_windows)} Kalshi windows loaded[/bold cyan]\n")


# ── Preload ticks per window for quick ask lookups ─────────────────────────
tick_cache: dict[int, dict[int, tuple[float, float]]] = {}
for w in all_windows:
    rows = db.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks WHERE window_id=?",
        (w["wid"],)
    ).fetchall()
    tick_cache[w["wid"]] = {
        r[0]: (r[1], r[2]) for r in rows if r[1] is not None and r[2] is not None
    }


def ask_at(wid: int, T: int) -> tuple[float, float] | None:
    """Look back up to 60s for the most recent ask snapshot at or before T."""
    ticks = tick_cache[wid]
    for t in range(T, max(0, T - 60) - 1, -1):
        if t in ticks:
            return ticks[t]
    return None


# ── Checkpoints to scan ─────────────────────────────────────────────────────
CHECKPOINTS = [
    (60,  0.05),
    (120, 0.10),
    (180, 0.10),
    (300, 0.10),
    (420, 0.10),
    (600, 0.05),
]


def simulate_entry(w: dict, T: int, thr_pct: float) -> dict | None:
    open_p = price_at(w["product"], w["wts"])
    now_p  = price_at(w["product"], w["wts"] + T)
    if not open_p or not now_p: return None
    delta = (now_p - open_p) / open_p * 100.0
    if abs(delta) < thr_pct: return None
    side = "yes" if delta > 0 else "no"
    asks = ask_at(w["wid"], T)
    if not asks: return None
    ya, na = asks
    entry = ya if side == "yes" else na
    if entry <= 0 or entry >= 1: return None
    won = (side == w["winner"])
    pnl = STAKE * (1 - entry) / entry if won else -STAKE
    return {"T": T, "thr": thr_pct, "side": side, "entry": entry,
            "won": won, "pnl": pnl, "delta": delta}


def run_strategy(name: str, picker) -> dict:
    """picker(window) -> list[dict] of entries for that window."""
    entries = []
    for w in all_windows:
        entries.extend(picker(w))
    if not entries:
        return {"name": name, "n_entries": 0}
    pnls = [e["pnl"] for e in entries]
    wins = sum(1 for e in entries if e["won"])
    total = sum(pnls)
    staked = STAKE * len(entries)
    return {
        "name": name,
        "n_entries": len(entries),
        "n_windows_with_entry": len({id(w) for w in all_windows
                                     if len(picker(w)) > 0}),
        "win_rate": wins / len(entries),
        "total_pnl": total, "staked": staked,
        "agg_return": total / staked,
        "mean_pnl": mean(pnls),
        "entries": entries,
    }


# ── Strategy pickers ────────────────────────────────────────────────────────

def pick_single(w):
    e = simulate_entry(w, 300, 0.10)
    return [e] if e else []

def pick_all_checkpoints(w):
    out = []
    for T, thr in CHECKPOINTS:
        e = simulate_entry(w, T, thr)
        if e: out.append(e)
    return out

def pick_cooldown(w, cooldown_sec=180):
    """Fire at each checkpoint but skip if we just entered the same side."""
    out = []
    last_t_by_side = {"yes": -9999, "no": -9999}
    for T, thr in CHECKPOINTS:
        e = simulate_entry(w, T, thr)
        if not e: continue
        if T - last_t_by_side[e["side"]] < cooldown_sec:
            continue
        out.append(e)
        last_t_by_side[e["side"]] = T
    return out

def pick_side_change(w):
    """Fire at a checkpoint only if the side FLIPPED from last fire."""
    out = []
    last_side = None
    for T, thr in CHECKPOINTS:
        e = simulate_entry(w, T, thr)
        if not e: continue
        if e["side"] == last_side:
            continue
        out.append(e)
        last_side = e["side"]
    return out

def pick_first_fire(w):
    """Fire at the earliest checkpoint whose threshold is met, then stop."""
    for T, thr in CHECKPOINTS:
        e = simulate_entry(w, T, thr)
        if e: return [e]
    return []


strategies = [
    ("single T=300s/0.10%",        pick_single),
    ("multi all checkpoints",      pick_all_checkpoints),
    ("multi + 180s cooldown",      lambda w: pick_cooldown(w, 180)),
    ("multi + 300s cooldown",      lambda w: pick_cooldown(w, 300)),
    ("multi side-change only",     pick_side_change),
    ("first fire only",            pick_first_fire),
]


# ── Report ─────────────────────────────────────────────────────────────────
results = {}
for name, picker in strategies:
    results[name] = run_strategy(name, picker)

con.print("[bold]Strategy comparison (all coins, stake $100 per entry)[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Strategy","Entries","Windows fired","Win%","Return%","Total P&L","Mean/entry"):
    tbl.add_column(col, justify="right")

# We also want entries-per-fired-window as a proxy for "how many times you stack"
for name, picker in strategies:
    r = results[name]
    if not r.get("n_entries"):
        continue
    # count windows with at least one entry for this picker
    n_wins = sum(1 for w in all_windows if picker(w))
    r["n_wins"] = n_wins
    stack = r["n_entries"] / n_wins if n_wins else 0
    col = "green" if r["agg_return"] > 0 else "red"
    tbl.add_row(
        name, str(r["n_entries"]),
        f"{n_wins} ({stack:.1f}×)",
        f"{r['win_rate']:.0%}",
        f"[{col}]{r['agg_return']*100:+.2f}%[/{col}]",
        f"${r['total_pnl']:+,.0f}",
        f"${r['mean_pnl']:+.2f}",
    )
con.print(tbl)


# ── Per-strategy: break down by coin ──────────────────────────────────────
con.print("\n[bold]Per-coin breakdown  —  'multi + 180s cooldown' strategy[/bold]")
picker = lambda w: pick_cooldown(w, 180)
by_coin = defaultdict(list)
for w in all_windows:
    for e in picker(w):
        by_coin[w["coin"]].append(e)
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Coin","Entries","Win%","Return%","Total P&L"):
    tbl.add_column(col, justify="right")
for coin in sorted(by_coin):
    es = by_coin[coin]
    pnls = [e["pnl"] for e in es]
    wins = sum(1 for e in es if e["won"])
    total = sum(pnls)
    ret = total / (STAKE * len(es))
    col = "green" if ret > 0 else "red"
    tbl.add_row(coin, str(len(es)), f"{wins/len(es):.0%}",
                f"[{col}]{ret*100:+.2f}%[/{col}]",
                f"${total:+,.0f}")
con.print(tbl)


# ── Per-checkpoint contribution within the multi strategy ─────────────────
con.print("\n[bold]Per-checkpoint contribution (multi all checkpoints)[/bold]")
by_cp = defaultdict(list)
for w in all_windows:
    for e in pick_all_checkpoints(w):
        by_cp[(e["T"], e["thr"])].append(e)
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("T","Thr","Entries","Win%","Return%"):
    tbl.add_column(col, justify="right")
for (T, thr) in sorted(by_cp):
    es = by_cp[(T, thr)]
    pnls = [e["pnl"] for e in es]
    wins = sum(1 for e in es if e["won"])
    ret = sum(pnls) / (STAKE * len(es))
    col = "green" if ret > 0 else "red"
    tbl.add_row(f"{T}s", f"{thr:.2f}%", str(len(es)),
                f"{wins/len(es):.0%}",
                f"[{col}]{ret*100:+.2f}%[/{col}]")
con.print(tbl)


# ── Window-level capital usage (how much at risk per window) ──────────────
con.print("\n[bold]Capital at risk per window (multi + 180s cooldown)[/bold]")
picker = lambda w: pick_cooldown(w, 180)
window_totals = []
for w in all_windows:
    es = picker(w)
    if not es: continue
    total_stake = STAKE * len(es)
    total_pnl = sum(e["pnl"] for e in es)
    window_totals.append({"stake": total_stake, "pnl": total_pnl})
if window_totals:
    pnls = [x["pnl"] for x in window_totals]
    stakes = [x["stake"] for x in window_totals]
    con.print(f"  windows with entry: {len(window_totals)}")
    con.print(f"  stake/window  mean ${mean(stakes):.0f}  "
              f"median ${median(stakes):.0f}  max ${max(stakes):.0f}")
    con.print(f"  pnl/window    mean ${mean(pnls):+.2f}  "
              f"median ${median(pnls):+.2f}  "
              f"min ${min(pnls):+.2f}  max ${max(pnls):+.2f}")
    n_loss_ge_200 = sum(1 for x in window_totals if x["pnl"] <= -200)
    con.print(f"  windows losing ≥ $200: {n_loss_ge_200}")

db.close()
