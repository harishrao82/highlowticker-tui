#!/usr/bin/env python3
"""
poly_scallops_commit_profit.py — does Idolized-Scallops' commit-time predict
window profitability?

Commit time = earliest elapsed_sec where current lean_sign == final lean_sign
and stays there.

For each resolved two-sided window:
  - bucket by commit_time: early (<=60s), mid (60-300s), late (>300s)
  - compute per-window P&L, spent, return-on-cost
  - compute mean/median/win% per bucket
  - bootstrap 95% CI on per-window P&L means

Also: does the CURRENT lean at 60s (observable in real-time, no need to wait
for the full window) predict enough to trade on?
"""
import json
import random
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median, stdev
from rich.console import Console
from rich.table import Table

STATE = Path.home() / ".btc_strategy_state.json"
con   = Console()

def coin_from_key(k: str) -> str:
    return k.split("-")[0].upper()

def window_start_ts(k: str) -> int:
    return int(k.rsplit("-", 1)[1])

def elapsed_sec(trade_time: str, w_start_ts: int) -> int | None:
    try:
        h, m, s = map(int, trade_time.split(":"))
    except Exception:
        return None
    start_dt = datetime.fromtimestamp(w_start_ts)
    day      = start_dt.replace(hour=h, minute=m, second=s)
    delta    = (day - start_dt).total_seconds()
    if delta < -3600: delta += 86400
    if delta < -60 or delta > 1800: return None
    return int(delta)

data   = json.loads(STATE.read_text())
shadow = data["shadow"]

# ── build windows ───────────────────────────────────────────────────────────
windows = []
for k, sh in shadow.items():
    if not sh.get("resolved"): continue
    su = float(sh.get("shares_up") or 0)
    sd = float(sh.get("shares_down") or 0)
    if su == 0 or sd == 0: continue  # skip one-sided
    xu = float(sh.get("spent_up") or 0)
    xd = float(sh.get("spent_down") or 0)
    pnl = float(sh.get("pnl") or 0)
    winner = sh.get("winner")
    spent = xu + xd
    if spent <= 0: continue

    trades = sh.get("trades") or []
    wts = window_start_ts(k)
    ts_trades = []
    for t in trades:
        e = elapsed_sec(t.get("time",""), wts)
        if e is None: continue
        ts_trades.append((e, t["side"], float(t["shares"])))
    if not ts_trades: continue
    ts_trades.sort(key=lambda x: x[0])

    cum_up = 0.0; cum_dn = 0.0
    series = []
    for e, side, shares in ts_trades:
        if side == "Up": cum_up += shares
        else:            cum_dn += shares
        series.append((e, cum_up, cum_dn))

    final_lean = "Up" if cum_up > cum_dn else "Down"

    # commit time = elapsed time right after the LAST wrong-lean tick
    last_wrong = -1
    for i, (e, cu, cd) in enumerate(series):
        is_correct = (cu > cd and final_lean == "Up") or (cd > cu and final_lean == "Down")
        if not is_correct:
            last_wrong = i
    if last_wrong == -1:
        commit = series[0][0]
    elif last_wrong == len(series) - 1:
        commit = series[-1][0]
    else:
        commit = series[last_wrong + 1][0]

    # lean at 60s (observable in real time)
    lean_60 = None
    for e, cu, cd in series:
        if e <= 60:
            if cu > cd: lean_60 = "Up"
            elif cd > cu: lean_60 = "Down"
        else:
            break

    windows.append({
        "key": k, "coin": coin_from_key(k),
        "winner": winner,
        "final_lean": final_lean,
        "spent": spent, "pnl": pnl,
        "ret": pnl / spent,
        "commit": commit,
        "lean_60": lean_60,
        "final_matches_winner": (final_lean == winner),
    })

con.print(f"[bold cyan]{len(windows)} two-sided resolved Scallops windows[/bold cyan]\n")

def bootstrap_ci(values, n_boot=5000, alpha=0.05):
    if not values: return (0, 0, 0)
    means = []
    rng = random.Random(42)
    n = len(values)
    for _ in range(n_boot):
        samp = [values[rng.randrange(n)] for _ in range(n)]
        means.append(sum(samp) / n)
    means.sort()
    return (mean(values), means[int(n_boot * alpha / 2)], means[int(n_boot * (1 - alpha/2))])


def summarize(group_name, ws):
    if not ws:
        return None
    pnls   = [w["pnl"]   for w in ws]
    spents = [w["spent"] for w in ws]
    rets   = [w["ret"]   for w in ws]
    total_pnl   = sum(pnls)
    total_spent = sum(spents)
    win_n   = sum(1 for p in pnls if p > 0)
    agg_return = total_pnl / total_spent if total_spent else 0
    mean_pnl, lo_pnl, hi_pnl = bootstrap_ci(pnls)
    mean_ret, lo_ret, hi_ret = bootstrap_ci(rets)
    return {
        "name": group_name, "n": len(ws),
        "total_pnl": total_pnl, "total_spent": total_spent,
        "mean_pnl": mean_pnl, "ci_pnl": (lo_pnl, hi_pnl),
        "mean_ret": mean_ret, "ci_ret": (lo_ret, hi_ret),
        "agg_return": agg_return,
        "win_rate": win_n / len(ws),
        "median_pnl": median(pnls),
    }


# ── 1. Bucket by commit time ──────────────────────────────────────────────
BUCKETS = [
    ("early (≤60s)",   lambda c: c <= 60),
    ("mid (61–300s)",  lambda c: 60 < c <= 300),
    ("late (301–600s)",lambda c: 300 < c <= 600),
    ("very late (>600s)", lambda c: c > 600),
]

con.print("[bold]Commit-time buckets — all coins[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Bucket","N","Win%","Total P&L","Total Spent","Agg Return","Mean P&L/win","95% CI","Median"):
    tbl.add_column(col, justify="right")

for name, pred in BUCKETS:
    ws = [w for w in windows if pred(w["commit"])]
    s = summarize(name, ws)
    if not s: continue
    ci_lo, ci_hi = s["ci_pnl"]
    ci_str = f"[${ci_lo:+.1f}, ${ci_hi:+.1f}]"
    col = "green" if s["mean_pnl"] > 0 else "red"
    tbl.add_row(
        s["name"], str(s["n"]), f"{s['win_rate']:.0%}",
        f"${s['total_pnl']:+,.0f}", f"${s['total_spent']:,.0f}",
        f"{s['agg_return']*100:+.1f}%",
        f"[{col}]${s['mean_pnl']:+.1f}[/{col}]", ci_str,
        f"${s['median_pnl']:+.1f}",
    )
con.print(tbl)

# ── 2. Same, per coin ─────────────────────────────────────────────────────
con.print("\n[bold]Commit-time buckets — per coin[/bold]")
by_coin = defaultdict(list)
for w in windows:
    by_coin[w["coin"]].append(w)

for coin in sorted(by_coin):
    ws_all = by_coin[coin]
    con.print(f"\n[cyan]{coin}[/cyan]  ({len(ws_all)} windows)")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Bucket","N","Win%","Tot P&L","Tot Spent","Agg Ret","Mean/win","95% CI"):
        tbl.add_column(col, justify="right")
    for name, pred in BUCKETS:
        ws = [w for w in ws_all if pred(w["commit"])]
        s = summarize(name, ws)
        if not s or s["n"] < 5:
            continue
        ci_lo, ci_hi = s["ci_pnl"]
        col = "green" if s["mean_pnl"] > 0 else "red"
        tbl.add_row(
            s["name"], str(s["n"]), f"{s['win_rate']:.0%}",
            f"${s['total_pnl']:+,.0f}", f"${s['total_spent']:,.0f}",
            f"{s['agg_return']*100:+.1f}%",
            f"[{col}]${s['mean_pnl']:+.1f}[/{col}]",
            f"[${ci_lo:+.1f}, ${ci_hi:+.1f}]",
        )
    con.print(tbl)


# ── 3. Observable-at-60s signal: lean_60 == winner, and P&L when true ───
con.print("\n[bold]'Observable at 60s' strategy — condition on lean_60[/bold]")
con.print("[dim]Question: if at 60s into a window the bot's lean matches the eventual winner,[/dim]")
con.print("[dim]          would copying that lean at 60s have been profitable?[/dim]")

tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Group","N","Winner match%","Mean P&L/win","95% CI","Agg Return"):
    tbl.add_column(col, justify="right")

# scenario A: we copy lean at 60s for every window where lean_60 is non-null
observable = [w for w in windows if w["lean_60"] is not None]
right = [w for w in observable if w["lean_60"] == w["winner"]]
wrong = [w for w in observable if w["lean_60"] != w["winner"]]

# The bot's P&L in those windows tells us what copying would yield IF we had
# matched their sizing. We use per-window return as a proxy.
s_all   = summarize("All observable", observable)
s_right = summarize("lean_60 = winner", right)
s_wrong = summarize("lean_60 ≠ winner", wrong)
for s in (s_all, s_right, s_wrong):
    if not s: continue
    ci_lo, ci_hi = s["ci_pnl"]
    col = "green" if s["mean_pnl"] > 0 else "red"
    tbl.add_row(
        s["name"], str(s["n"]),
        f"{s['win_rate']:.0%}",
        f"[{col}]${s['mean_pnl']:+.1f}[/{col}]",
        f"[${ci_lo:+.1f}, ${ci_hi:+.1f}]",
        f"{s['agg_return']*100:+.1f}%",
    )
con.print(tbl)


# ── 4. "Early commit AND lean_60 matches final lean" — strongest filter ──
con.print("\n[bold]Strongest filter: early commit (≤60s) AND lean_60 == final_lean[/bold]")
subset = [w for w in windows if w["commit"] <= 60 and w["lean_60"] == w["final_lean"]]
s = summarize("early+consistent", subset)
if s:
    ci_lo, ci_hi = s["ci_pnl"]
    con.print(f"  N={s['n']}  win%={s['win_rate']:.0%}  "
              f"total P&L ${s['total_pnl']:+,.0f}  "
              f"spent ${s['total_spent']:,.0f}  "
              f"agg return {s['agg_return']*100:+.1f}%  "
              f"mean P&L/win ${s['mean_pnl']:+.1f}  "
              f"95% CI [${ci_lo:+.1f}, ${ci_hi:+.1f}]")

# ── 5. Winners vs losers by commit bucket — frequency table ──────────────
con.print("\n[bold]Distribution check[/bold]  (how many windows in each bucket?)")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Bucket","All coins","BTC","ETH","SOL","XRP"):
    tbl.add_column(col, justify="right")
for name, pred in BUCKETS:
    row = [name, str(sum(1 for w in windows if pred(w["commit"])))]
    for coin in ("BTC","ETH","SOL","XRP"):
        row.append(str(sum(1 for w in by_coin[coin] if pred(w["commit"]))))
    tbl.add_row(*row)
con.print(tbl)
