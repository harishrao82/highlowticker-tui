#!/usr/bin/env python3
"""
poly_btc_momentum_sim.py — simulate a pure BTC-delta strategy on Polymarket
BTC windows, without copying Scallops.

For each window, at time T:
  - Look up BTC price at T and at window open
  - delta_pct = (btc_T - btc_open) / btc_open * 100
  - If |delta_pct| >= THRESHOLD: buy the momentum side (Up if +, Down if -)
  - Use the bot's avg first-minute entry price on THAT side as proxy for our fill
    (this is an optimistic but reasonable proxy — bot IS buying at market)
  - Hold to settlement

Scan T and threshold combinations.
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
CACHE = Path.home() / ".scallops_btc_1m_cache.json"
con   = Console()
STAKE = 100.0

def window_start_ts(k): return int(k.rsplit("-",1)[1])
def elapsed_sec(tt, wts):
    try: h,m,s = map(int, tt.split(":"))
    except: return None
    sdt = datetime.fromtimestamp(wts)
    day = sdt.replace(hour=h, minute=m, second=s)
    delta = (day - sdt).total_seconds()
    if delta < -3600: delta += 86400
    if delta < -60 or delta > 1800: return None
    return int(delta)

data   = json.loads(STATE.read_text())
shadow = data["shadow"]
price_by_min = {int(k): float(v) for k, v in json.loads(CACHE.read_text()).items()}
def price_at(ts): return price_by_min.get(ts - (ts % 60))

# Build window records
windows = []
for k, sh in shadow.items():
    if not k.startswith("btc-updown-15m-"): continue
    if not sh.get("resolved"): continue
    trades = sh.get("trades") or []
    if not trades: continue
    wts = window_start_ts(k)
    ts_trades = []
    for t in trades:
        e = elapsed_sec(t.get("time",""), wts)
        if e is None: continue
        ts_trades.append({"elapsed": e, "side": t["side"],
                          "price": float(t.get("price",0)),
                          "shares": float(t.get("shares",0))})
    if not ts_trades: continue
    open_px = price_at(wts)
    if not open_px: continue
    windows.append({"key": k, "wts": wts, "winner": sh.get("winner"),
                    "open_px": open_px,
                    "trades": sorted(ts_trades, key=lambda t: t["elapsed"])})

con.print(f"[bold cyan]{len(windows)} BTC windows with BTC price data[/bold cyan]\n")


def bot_avg_price_on(w, side, max_elapsed):
    """Avg price the bot paid on `side` within [0, max_elapsed]."""
    tot_sh = 0.0; tot_cost = 0.0
    for t in w["trades"]:
        if t["elapsed"] > max_elapsed: break
        if t["side"] == side and t["price"] > 0:
            tot_sh   += t["shares"]
            tot_cost += t["price"] * t["shares"]
    return tot_cost / tot_sh if tot_sh > 0 else None


def bot_price_anywhere(w, side):
    """Fallback: first bot price on `side` anywhere in the window."""
    for t in w["trades"]:
        if t["side"] == side and t["price"] > 0:
            return t["price"]
    return None


def bootstrap_ci(vals, n_boot=3000):
    if not vals: return (0, 0, 0)
    rng = random.Random(42)
    n = len(vals)
    ms = []
    for _ in range(n_boot):
        samp = [vals[rng.randrange(n)] for _ in range(n)]
        ms.append(sum(samp)/n)
    ms.sort()
    return (mean(vals), ms[int(n_boot*0.025)], ms[int(n_boot*0.975)])


def simulate(T_sec: int, threshold_pct: float, price_window: int = 120):
    """Run sim. price_window = how far into window to look for bot avg price."""
    results = []
    for w in windows:
        btc_now = price_at(w["wts"] + T_sec)
        if not btc_now: continue
        delta = (btc_now - w["open_px"]) / w["open_px"] * 100
        if abs(delta) < threshold_pct:
            continue
        side = "Up" if delta > 0 else "Down"
        entry = bot_avg_price_on(w, side, price_window) or bot_price_anywhere(w, side)
        if not entry or entry <= 0 or entry >= 1:
            continue
        won = (side == w["winner"])
        pnl = STAKE * (1 - entry) / entry if won else -STAKE
        results.append({"won": won, "pnl": pnl, "entry": entry,
                        "delta": delta, "side": side})
    if not results:
        return None
    pnls = [r["pnl"] for r in results]
    wins = sum(1 for r in results if r["won"])
    total = sum(pnls)
    staked = STAKE * len(results)
    m, lo, hi = bootstrap_ci(pnls)
    return dict(n=len(results), win_rate=wins/len(results),
                total=total, staked=staked,
                agg_return=total/staked,
                mean_pnl=m, ci=(lo, hi),
                results=results)


# ── 1. Scan T × threshold grid ─────────────────────────────────────────────
con.print("[bold]Scan: entry time T × momentum threshold[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("T","Thresh","N","Win%","Total P&L","Return%","Mean/win","95% CI"):
    tbl.add_column(col, justify="right")

Ts = [30, 60, 90, 120, 180, 300]
thresholds = [0.0, 0.02, 0.05, 0.10, 0.15, 0.20]

best = None
for T in Ts:
    for thr in thresholds:
        r = simulate(T, thr, price_window=60)
        if not r or r["n"] < 30: continue
        col = "green" if r["agg_return"] > 0.05 else "yellow" if r["agg_return"] > 0 else "red"
        tbl.add_row(
            f"{T}s", f"{thr:.2f}%", str(r["n"]),
            f"{r['win_rate']:.0%}",
            f"${r['total']:+,.0f}",
            f"[{col}]{r['agg_return']*100:+.2f}%[/{col}]",
            f"${r['mean_pnl']:+.2f}",
            f"[${r['ci'][0]:+.2f}, ${r['ci'][1]:+.2f}]",
        )
        if best is None or r["agg_return"] > best["agg_return"]:
            best = {"T": T, "thr": thr, **r}
con.print(tbl)

if best:
    con.print(f"\n[bold green]Best grid cell:[/bold green]  "
              f"T={best['T']}s  threshold={best['thr']}%  "
              f"return={best['agg_return']*100:+.2f}%  "
              f"N={best['n']}  win%={best['win_rate']:.1%}")


# ── 2. Deep dive on the strongest cell ─────────────────────────────────────
if best:
    r = best
    con.print(f"\n[bold]Deep dive: T={r['T']}s, threshold={r['thr']}%[/bold]")

    entries = sorted(x["entry"] for x in r["results"])
    def pct(xs, p): return xs[int(len(xs)*p)]
    con.print(f"  Entry price dist (bot's avg first-min price on lean side):")
    con.print(f"    p10 {pct(entries,0.1):.3f}  p50 {pct(entries,0.5):.3f}  p90 {pct(entries,0.9):.3f}")

    # Split by entry price band
    bands = [(0.00, 0.35), (0.35, 0.50), (0.50, 0.65), (0.65, 0.80), (0.80, 1.00)]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Entry band","N","Win%","Return%","Mean/win"):
        tbl.add_column(col, justify="right")
    for lo, hi in bands:
        rs = [x for x in r["results"] if lo <= x["entry"] < hi]
        if len(rs) < 10: continue
        pnls = [x["pnl"] for x in rs]
        wins = sum(1 for x in rs if x["won"])
        total = sum(pnls)
        ret = total / (STAKE * len(rs))
        col = "green" if ret > 0 else "red"
        tbl.add_row(f"${lo:.2f}–${hi:.2f}", str(len(rs)),
                    f"{wins/len(rs):.0%}",
                    f"[{col}]{ret*100:+.1f}%[/{col}]",
                    f"${mean(pnls):+.2f}")
    con.print(tbl)


# ── 3. Time-stability check ───────────────────────────────────────────────
if best:
    r = best
    # Re-run the same sim and split results by window_start order
    results_ordered = []
    for w in windows:
        btc_now = price_at(w["wts"] + r["T"])
        if not btc_now: continue
        delta = (btc_now - w["open_px"]) / w["open_px"] * 100
        if abs(delta) < r["thr"]: continue
        side = "Up" if delta > 0 else "Down"
        entry = bot_avg_price_on(w, side, 60) or bot_price_anywhere(w, side)
        if not entry or entry <= 0 or entry >= 1: continue
        won = (side == w["winner"])
        pnl = STAKE * (1 - entry)/entry if won else -STAKE
        results_ordered.append({"wts": w["wts"], "pnl": pnl, "won": won})
    results_ordered.sort(key=lambda x: x["wts"])

    halves = [("First half", results_ordered[:len(results_ordered)//2]),
              ("Second half", results_ordered[len(results_ordered)//2:])]
    con.print(f"\n[bold]Time-stability of best cell (T={r['T']}s, thr={r['thr']}%)[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Half","N","Win%","Total P&L","Return%"):
        tbl.add_column(col, justify="right")
    for name, rs in halves:
        if not rs: continue
        pnls = [x["pnl"] for x in rs]
        wins = sum(1 for x in rs if x["won"])
        total = sum(pnls)
        ret = total / (STAKE * len(rs))
        col = "green" if ret > 0 else "red"
        tbl.add_row(name, str(len(rs)), f"{wins/len(rs):.0%}",
                    f"${total:+,.0f}",
                    f"[{col}]{ret*100:+.2f}%[/{col}]")
    con.print(tbl)
