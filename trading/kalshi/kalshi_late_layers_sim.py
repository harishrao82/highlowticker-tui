#!/usr/bin/env python3
"""
kalshi_late_layers_sim.py — backtest "late-window" entry layers on Kalshi data.

Layer 3 — Late contrarian probe
  At T >= LAYER3_T_MIN, if min(yes_ask, no_ask) <= LAYER3_MAX_PRICE,
  buy that side. Reasoning: cheap longshots — small risk, big payoff if right.

Layer 4 — Late conviction load
  At T >= LAYER4_T_MIN, if max(yes_ask, no_ask) >= LAYER4_MIN_PRICE,
  buy the high side. Reasoning: market is highly confident; we ride it.

Layer 5 — Pair lock arbitrage (sanity check)
  Whenever yes_ask + no_ask < LAYER5_MAX_SUM, buy both → guaranteed profit
  if it ever happens (almost never on tight Kalshi spreads).

For each, simulate one buy per (window, condition-fire) and compute P&L
vs. resolved winner.
"""
import sqlite3
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from rich.console import Console
from rich.table import Table

DB    = Path.home() / ".btc_windows.db"
STAKE = 100.0
con   = Console()

# ── Layer params ───────────────────────────────────────────────────────────
LAYER3_T_MIN     = 600    # only fire after 10 min
LAYER3_MAX_PRICE = 0.15   # buy if cheap side ≤ 15¢
LAYER3_COOLDOWN  = 60     # at most one fire per 60s window of cheap prices

LAYER4_T_MIN     = 720    # only fire after 12 min
LAYER4_MIN_PRICE = 0.85   # buy if expensive side ≥ 85¢
LAYER4_MAX_PRICE = 0.95   # but skip if too rich (keeps EV positive)
LAYER4_COOLDOWN  = 60

LAYER5_MAX_SUM   = 0.97   # buy both sides if their asks sum to less than this

# ── Load Kalshi windows ────────────────────────────────────────────────────
db = sqlite3.connect(DB)
windows = db.execute(
    "SELECT w.id, w.ticker, w.winner FROM windows w "
    "WHERE w.winner IS NOT NULL "
    "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100"
).fetchall()
con.print(f"[bold cyan]{len(windows)} resolved Kalshi windows[/bold cyan]\n")


def coin_of(ticker: str) -> str:
    return ticker[2:5]


def load_ticks(wid: int):
    rows = db.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks WHERE window_id=? ORDER BY elapsed_sec",
        (wid,)
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows
            if r[1] is not None and r[2] is not None]


# ── Simulators ─────────────────────────────────────────────────────────────

def sim_layer3(wid: int, ticker: str, winner: str):
    """At T≥600s, if min(yes_ask, no_ask) ≤ 0.15, buy 1× of cheap side."""
    fires = []
    last_fire_t = -10_000
    for t, ya, na in load_ticks(wid):
        if t < LAYER3_T_MIN: continue
        if t - last_fire_t < LAYER3_COOLDOWN: continue
        cheap_side, cheap_px = ("yes", ya) if ya < na else ("no", na)
        if cheap_px <= 0.01: continue   # too cheap to be real
        if cheap_px > LAYER3_MAX_PRICE: continue
        won = (cheap_side == winner)
        pnl = STAKE * (1 - cheap_px) / cheap_px if won else -STAKE
        fires.append({"t": t, "side": cheap_side, "entry": cheap_px,
                      "won": won, "pnl": pnl})
        last_fire_t = t
    return fires


def sim_layer4(wid: int, ticker: str, winner: str):
    """At T≥720s, if max(yes_ask, no_ask) ≥ 0.85 (≤0.95), buy that side."""
    fires = []
    last_fire_t = -10_000
    for t, ya, na in load_ticks(wid):
        if t < LAYER4_T_MIN: continue
        if t - last_fire_t < LAYER4_COOLDOWN: continue
        rich_side, rich_px = ("yes", ya) if ya > na else ("no", na)
        if rich_px < LAYER4_MIN_PRICE: continue
        if rich_px > LAYER4_MAX_PRICE: continue
        won = (rich_side == winner)
        pnl = STAKE * (1 - rich_px) / rich_px if won else -STAKE
        fires.append({"t": t, "side": rich_side, "entry": rich_px,
                      "won": won, "pnl": pnl})
        last_fire_t = t
    return fires


def sim_layer5(wid: int, ticker: str, winner: str):
    """If yes_ask + no_ask < 0.97 (rare on Kalshi), buy both = guaranteed lock."""
    fires = []
    fired = False
    for t, ya, na in load_ticks(wid):
        if fired: break
        if (ya + na) < LAYER5_MAX_SUM and ya > 0.01 and na > 0.01:
            # buy both: net cost = (ya + na), payoff = $1 (whichever wins)
            pnl = STAKE * (1 - (ya + na)) / (ya + na)  # locked positive
            fires.append({"t": t, "yes_ask": ya, "no_ask": na,
                          "sum": ya + na, "pnl": pnl, "won": True})
            fired = True
    return fires


# ── Run all sims ──────────────────────────────────────────────────────────
def run(name, simfn):
    rows = []
    for wid, ticker, winner in windows:
        fires = simfn(wid, ticker, winner)
        for f in fires:
            f["coin"] = coin_of(ticker)
            rows.append(f)
    return rows


con.print("[dim]Simulating Layer 3 (cheap probes)…[/dim]")
l3 = run("L3", sim_layer3)
con.print("[dim]Simulating Layer 4 (conviction loads)…[/dim]")
l4 = run("L4", sim_layer4)
con.print("[dim]Simulating Layer 5 (pair-lock arb)…[/dim]")
l5 = run("L5", sim_layer5)


def summarize(name, rows):
    if not rows:
        con.print(f"\n[bold]{name}[/bold]  no fires")
        return
    n = len(rows)
    wins = sum(1 for r in rows if r["won"])
    pnls = [r["pnl"] for r in rows]
    total = sum(pnls)
    staked = STAKE * n
    con.print(f"\n[bold]{name}[/bold]  N={n}  win%={wins/n:.0%}  "
              f"total ${total:+,.0f}  return {total/staked*100:+.2f}%  "
              f"mean ${mean(pnls):+.2f}  median ${median(pnls):+.2f}")


summarize("Layer 3 — late cheap probes", l3)
summarize("Layer 4 — late conviction loads", l4)
summarize("Layer 5 — pair-lock arbitrage", l5)

# Per-coin breakdowns
def by_coin(name, rows):
    if not rows: return
    con.print(f"\n[bold]{name} per coin[/bold]")
    bc = defaultdict(list)
    for r in rows: bc[r["coin"]].append(r)
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","N","Win%","Return%","Mean P&L"):
        tbl.add_column(col, justify="right")
    for coin in sorted(bc):
        rs = bc[coin]
        wins = sum(1 for r in rs if r["won"])
        pnls = [r["pnl"] for r in rs]
        ret = sum(pnls)/(STAKE*len(rs))
        col = "green" if ret > 0 else "red"
        tbl.add_row(coin, str(len(rs)), f"{wins/len(rs):.0%}",
                    f"[{col}]{ret*100:+.2f}%[/{col}]",
                    f"${mean(pnls):+.2f}")
    con.print(tbl)


by_coin("Layer 3", l3)
by_coin("Layer 4", l4)


# Layer 3: entry-price band breakdown — does win rate vary with how cheap?
if l3:
    con.print("\n[bold]Layer 3 by entry-price band[/bold]")
    bands = [(0.00,0.05),(0.05,0.10),(0.10,0.15)]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Entry","N","Win%","Return%","Mean P&L"):
        tbl.add_column(col, justify="right")
    for lo, hi in bands:
        rs = [r for r in l3 if lo <= r["entry"] < hi]
        if not rs: continue
        wins = sum(1 for r in rs if r["won"])
        ret = sum(r["pnl"] for r in rs)/(STAKE*len(rs))
        col = "green" if ret > 0 else "red"
        tbl.add_row(f"<{hi:.2f}", str(len(rs)),
                    f"{wins/len(rs):.0%}",
                    f"[{col}]{ret*100:+.2f}%[/{col}]",
                    f"${mean(r['pnl'] for r in rs):+.2f}")
    con.print(tbl)


# Layer 4: by entry-price band
if l4:
    con.print("\n[bold]Layer 4 by entry-price band[/bold]")
    bands = [(0.85,0.88),(0.88,0.91),(0.91,0.95)]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Entry","N","Win%","Return%","Mean P&L"):
        tbl.add_column(col, justify="right")
    for lo, hi in bands:
        rs = [r for r in l4 if lo <= r["entry"] < hi]
        if not rs: continue
        wins = sum(1 for r in rs if r["won"])
        ret = sum(r["pnl"] for r in rs)/(STAKE*len(rs))
        col = "green" if ret > 0 else "red"
        tbl.add_row(f"{lo:.2f}–{hi:.2f}", str(len(rs)),
                    f"{wins/len(rs):.0%}",
                    f"[{col}]{ret*100:+.2f}%[/{col}]",
                    f"${mean(r['pnl'] for r in rs):+.2f}")
    con.print(tbl)


# Layer 4 by entry T (does waiting longer matter?)
if l4:
    con.print("\n[bold]Layer 4 by entry T (elapsed_sec at fire)[/bold]")
    bands = [(720,780),(780,840),(840,900)]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Elapsed","N","Win%","Return%"):
        tbl.add_column(col, justify="right")
    for lo, hi in bands:
        rs = [r for r in l4 if lo <= r["t"] < hi]
        if not rs: continue
        wins = sum(1 for r in rs if r["won"])
        ret = sum(r["pnl"] for r in rs)/(STAKE*len(rs))
        col = "green" if ret > 0 else "red"
        tbl.add_row(f"{lo}-{hi}s", str(len(rs)),
                    f"{wins/len(rs):.0%}",
                    f"[{col}]{ret*100:+.2f}%[/{col}]")
    con.print(tbl)

db.close()
