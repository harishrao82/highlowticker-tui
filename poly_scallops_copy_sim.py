#!/usr/bin/env python3
"""
poly_scallops_copy_sim.py — realistic "copy the bot's 60s lean" P&L simulation.

For each two-sided resolved Scallops window:
  1. Look at all bot trades in the first 60 seconds.
  2. Compute lean_60 = side with more cumulative shares at t=60s.
  3. Use the avg price the bot paid on that leaning side during [0, 60s] as
     the entry price for our simulated $STAKE buy of that side.
  4. At settlement: if that side won → we get (stake / entry_price), else 0.
  5. Sum P&L across all windows; per coin.

This is a realistic single-trade copy strategy. If profitable after accounting
for spread/slippage, it's deployable capital.
"""
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from rich.console import Console
from rich.table import Table
import random

STATE = Path.home() / ".btc_strategy_state.json"
con   = Console()
STAKE = 100.0   # dollars per window, constant

def coin_from_key(k): return k.split("-")[0].upper()
def window_start_ts(k): return int(k.rsplit("-",1)[1])

def elapsed_sec(trade_time, w_start_ts):
    try:
        h, m, s = map(int, trade_time.split(":"))
    except Exception:
        return None
    start_dt = datetime.fromtimestamp(w_start_ts)
    day = start_dt.replace(hour=h, minute=m, second=s)
    delta = (day - start_dt).total_seconds()
    if delta < -3600: delta += 86400
    if delta < -60 or delta > 1800: return None
    return int(delta)

data = json.loads(STATE.read_text())
shadow = data["shadow"]

sim_rows = []
for k, sh in shadow.items():
    if not sh.get("resolved"): continue
    winner = sh.get("winner")
    if winner not in ("Up","Down"): continue

    trades = sh.get("trades") or []
    wts = window_start_ts(k)
    ts_trades = []
    for t in trades:
        e = elapsed_sec(t.get("time",""), wts)
        if e is None: continue
        ts_trades.append((e, t["side"], float(t.get("price",0)), float(t["shares"])))
    if not ts_trades: continue
    ts_trades.sort(key=lambda x: x[0])

    # first-minute bot trades
    first_min = [t for t in ts_trades if t[0] <= 60]
    if not first_min:
        continue

    cum_up_sh   = 0.0; cum_dn_sh   = 0.0
    cum_up_cost = 0.0; cum_dn_cost = 0.0
    for e, side, px, sh_count in first_min:
        if side == "Up":
            cum_up_sh   += sh_count
            cum_up_cost += px * sh_count
        else:
            cum_dn_sh   += sh_count
            cum_dn_cost += px * sh_count

    if cum_up_sh == cum_dn_sh:
        continue  # no lean at 60s
    lean = "Up" if cum_up_sh > cum_dn_sh else "Down"

    # entry price = bot's avg buy price on the leaning side in first minute
    if lean == "Up":
        entry_px = cum_up_cost / cum_up_sh
    else:
        entry_px = cum_dn_cost / cum_dn_sh
    if entry_px <= 0 or entry_px >= 1:
        continue  # degenerate

    # simulate our $STAKE buy on lean side at entry_px
    shares    = STAKE / entry_px
    gross_win = shares * 1.0  # $1/share at settlement if lean wins
    pnl       = (gross_win - STAKE) if lean == winner else -STAKE

    sim_rows.append({
        "key": k, "coin": coin_from_key(k),
        "winner": winner, "lean": lean,
        "entry_px": entry_px, "pnl": pnl,
        "won": lean == winner,
    })

con.print(f"[bold cyan]Simulated {len(sim_rows)} window trades "
          f"(stake ${STAKE:.0f}/window)[/bold cyan]\n")

def bootstrap_ci(vals, n_boot=5000):
    if not vals: return (0,0,0)
    rng = random.Random(42)
    n = len(vals)
    ms = []
    for _ in range(n_boot):
        samp = [vals[rng.randrange(n)] for _ in range(n)]
        ms.append(sum(samp) / n)
    ms.sort()
    return (mean(vals), ms[int(n_boot*0.025)], ms[int(n_boot*0.975)])

def summarize(group, rs):
    if not rs: return None
    pnls = [r["pnl"] for r in rs]
    wins = sum(1 for r in rs if r["won"])
    total_pnl  = sum(pnls)
    staked     = STAKE * len(rs)
    agg_return = total_pnl / staked
    m, lo, hi  = bootstrap_ci(pnls)
    return dict(name=group, n=len(rs), win_rate=wins/len(rs),
                total_pnl=total_pnl, staked=staked,
                agg_return=agg_return,
                mean_pnl=m, ci=(lo,hi))


# ── 1. Overall ─────────────────────────────────────────────────────────────
s = summarize("ALL", sim_rows)
con.print("[bold]Overall  —  copy bot's 60s lean, stake $100/window[/bold]")
con.print(f"  N = {s['n']}")
con.print(f"  win rate = {s['win_rate']:.1%}")
con.print(f"  total staked = ${s['staked']:,.0f}")
con.print(f"  [bold]total P&L = ${s['total_pnl']:+,.0f}[/bold]  ({s['agg_return']*100:+.1f}% per stake)")
con.print(f"  mean P&L/window = ${s['mean_pnl']:+.2f}  "
          f"95% CI [${s['ci'][0]:+.2f}, ${s['ci'][1]:+.2f}]")

# ── 2. Per coin ────────────────────────────────────────────────────────────
con.print("\n[bold]Per coin[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Coin","N","Win%","Total P&L","Staked","Return%","Mean/win","95% CI"):
    tbl.add_column(col, justify="right")

by_coin = defaultdict(list)
for r in sim_rows: by_coin[r["coin"]].append(r)
for coin in sorted(by_coin):
    s = summarize(coin, by_coin[coin])
    if not s: continue
    col = "green" if s["mean_pnl"] > 0 else "red"
    tbl.add_row(coin, str(s["n"]), f"{s['win_rate']:.0%}",
                f"${s['total_pnl']:+,.0f}", f"${s['staked']:,.0f}",
                f"{s['agg_return']*100:+.1f}%",
                f"[{col}]${s['mean_pnl']:+.2f}[/{col}]",
                f"[${s['ci'][0]:+.2f}, ${s['ci'][1]:+.2f}]")
con.print(tbl)

# ── 3. By entry price bucket — when is the bot's lean most reliable? ─────
con.print("\n[bold]By entry price bucket (BTC only — the main profit coin)[/bold]")
btc = by_coin.get("BTC", [])
buckets = [
    ("<0.20",  lambda p: p < 0.20),
    ("0.20–0.35", lambda p: 0.20 <= p < 0.35),
    ("0.35–0.50", lambda p: 0.35 <= p < 0.50),
    ("0.50–0.65", lambda p: 0.50 <= p < 0.65),
    ("0.65–0.80", lambda p: 0.65 <= p < 0.80),
    (">=0.80", lambda p: p >= 0.80),
]
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Entry px","N","Win%","Mean/win","Agg return"):
    tbl.add_column(col, justify="right")
for name, pred in buckets:
    rs = [r for r in btc if pred(r["entry_px"])]
    s = summarize(name, rs)
    if not s or s["n"] < 5: continue
    col = "green" if s["mean_pnl"] > 0 else "red"
    tbl.add_row(name, str(s["n"]), f"{s['win_rate']:.0%}",
                f"[{col}]${s['mean_pnl']:+.2f}[/{col}]",
                f"{s['agg_return']*100:+.1f}%")
con.print(tbl)

# ── 4. Time-robustness check (did the edge exist throughout the period?) ─
con.print("\n[bold]Time-robustness — split BTC by window_start_ts[/bold]")
if btc:
    btc_sorted = sorted(btc, key=lambda r: window_start_ts(r["key"]))
    halves = [("First half", btc_sorted[:len(btc_sorted)//2]),
              ("Second half", btc_sorted[len(btc_sorted)//2:])]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Half","N","Win%","Total P&L","Return%","Mean/win","95% CI"):
        tbl.add_column(col, justify="right")
    for name, rs in halves:
        s = summarize(name, rs)
        if not s: continue
        col = "green" if s["mean_pnl"] > 0 else "red"
        tbl.add_row(name, str(s["n"]), f"{s['win_rate']:.0%}",
                    f"${s['total_pnl']:+,.0f}", f"{s['agg_return']*100:+.1f}%",
                    f"[{col}]${s['mean_pnl']:+.2f}[/{col}]",
                    f"[${s['ci'][0]:+.2f}, ${s['ci'][1]:+.2f}]")
    con.print(tbl)

# ── 5. Scale projection for $100k bankroll ──────────────────────────────
con.print("\n[bold]Bankroll sizing check[/bold]")
btc_s = summarize("BTC", btc)
if btc_s and btc_s["agg_return"] > 0:
    N = btc_s["n"]
    mean_px = median(r["entry_px"] for r in btc)
    # windows per day (assume ~96 if all 15-min BTC windows)
    all_coin_n = len(sim_rows)
    con.print(f"  Dataset: {N} BTC windows + {all_coin_n - N} other coin windows")
    con.print(f"  BTC median entry price: {mean_px:.3f}")
    con.print(f"  BTC per-window return: {btc_s['agg_return']*100:+.2f}%")
    con.print(f"  Scaling naive: 96 BTC windows/day × {btc_s['agg_return']*100:.2f}% × stake")
    for stake in (100, 500, 1000, 5000, 10_000):
        daily = 96 * btc_s["agg_return"] * stake
        monthly = daily * 30
        con.print(f"    @ ${stake:,}/window: ~${daily:+,.0f}/day  ~${monthly:+,.0f}/mo")
    con.print("\n  [yellow]Caveats: assumes you can match bot's entry prices (optimistic),[/yellow]")
    con.print("  [yellow]  no slippage, no fees, full BTC fill rate, stationary edge.[/yellow]")
