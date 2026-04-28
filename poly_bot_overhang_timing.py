#!/usr/bin/env python3
"""
poly_bot_overhang_timing.py — when does the bot commit to a direction?

For each two-sided resolved window:
  - Sort trades by elapsed_sec
  - Build cumulative shares_up / shares_down time series
  - At fixed sample times, record:
      * lean_sign = sign(cum_up - cum_dn)   (which side bigger right now)
      * |lean|    = |cum_up - cum_dn| / max(cum_up, cum_dn)  (how committed)
  - Winner-match: does lean_sign at time t equal the winning side?

Outputs:
  1) Accuracy vs time curve — % of windows where current lean matches winner
  2) "Final-sign reached" time — when does bot first settle on its ending lean?
  3) Sample trajectories (a few winners and a few losers)
"""
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
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
    if delta < -3600:
        delta += 86400
    if delta < -60 or delta > 1800:
        return None
    return int(delta)


data   = json.loads(STATE.read_text())
shadow = data["shadow"]

# ── Phase 1: build per-window time series ──────────────────────────────────
SAMPLE_TIMES = list(range(30, 901, 30))  # every 30s from 30 to 900

windows = []
for k, sh in shadow.items():
    if not sh.get("resolved"):
        continue
    if not (float(sh.get("shares_up") or 0) > 0 and float(sh.get("shares_down") or 0) > 0):
        continue
    trades = sh.get("trades") or []
    if not trades:
        continue
    wts = window_start_ts(k)
    ts_trades = []
    for t in trades:
        e = elapsed_sec(t.get("time", ""), wts)
        if e is None:
            continue
        ts_trades.append((e, t["side"], float(t["shares"])))
    if not ts_trades:
        continue
    ts_trades.sort(key=lambda x: x[0])

    # cumulative sums
    cum_up = 0.0
    cum_dn = 0.0
    series = []  # list of (elapsed, cum_up, cum_dn)
    for e, side, sh_count in ts_trades:
        if side == "Up":
            cum_up += sh_count
        else:
            cum_dn += sh_count
        series.append((e, cum_up, cum_dn))

    # final lean
    final_lean = "Up" if cum_up > cum_dn else "Down"
    winner = sh.get("winner")

    # sample the series at SAMPLE_TIMES
    samples: dict[int, tuple[float, float]] = {}
    idx = 0
    last = (0.0, 0.0)
    for T in SAMPLE_TIMES:
        while idx < len(series) and series[idx][0] <= T:
            last = (series[idx][1], series[idx][2])
            idx += 1
        samples[T] = last

    windows.append({
        "key":        k,
        "coin":       coin_from_key(k),
        "winner":     winner,
        "final_lean": final_lean,
        "cum_up":     cum_up,
        "cum_dn":     cum_dn,
        "series":     series,
        "samples":    samples,
        "overhang_matches_winner": (final_lean == winner),
    })

con.print(f"[bold cyan]Loaded {len(windows)} two-sided resolved windows[/bold cyan]\n")

# ── 2. Accuracy vs time curve ──────────────────────────────────────────────
con.print("[bold]Lean-sign accuracy vs window elapsed time[/bold]  "
          "[dim](% of windows where sign(cum_up − cum_dn) matches winner)[/dim]")

tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
tbl.add_column("Elapsed", justify="right")
tbl.add_column("N w/ lean", justify="right")
tbl.add_column("Lean = winner", justify="right")
tbl.add_column("Lean = loser", justify="right")
tbl.add_column("Accuracy", justify="right")
tbl.add_column("Median |imbalance|", justify="right")

for T in SAMPLE_TIMES:
    total = 0
    right = 0
    imbalances = []
    for w in windows:
        cu, cd = w["samples"][T]
        if cu == 0 and cd == 0:
            continue
        lean = "Up" if cu > cd else ("Down" if cd > cu else None)
        if lean is None:
            continue
        total += 1
        if lean == w["winner"]:
            right += 1
        if max(cu, cd) > 0:
            imbalances.append(abs(cu - cd) / max(cu, cd))
    wrong = total - right
    acc = right / total if total else 0
    med_imb = median(imbalances) if imbalances else 0
    col = "green" if acc >= 0.58 else "yellow" if acc >= 0.54 else "red"
    tbl.add_row(f"{T}s", str(total), str(right), str(wrong),
                f"[{col}]{acc:.1%}[/{col}]", f"{med_imb:.1%}")
con.print(tbl)


# ── 3. When does the bot first commit to its final lean? ──────────────────
con.print("\n[bold]Commit-time distribution[/bold]  "
          "[dim](earliest elapsed time at which current lean_sign == final lean_sign "
          "and stays there)[/dim]")

commit_times = []
for w in windows:
    series = w["series"]
    final_lean = w["final_lean"]
    # find last time the lean was wrong (or zero) before settling
    # commit_time = first t where, from t onwards, sign(cum_up - cum_dn) stays == final_lean sign
    commit = None
    # walk backwards: find last index where lean != final_lean
    last_wrong = -1
    for i, (e, cu, cd) in enumerate(series):
        if (cu > cd and final_lean == "Up") or (cd > cu and final_lean == "Down"):
            pass  # correct at this point
        else:
            last_wrong = i
    if last_wrong == len(series) - 1:
        commit = series[-1][0]  # only at the very end
    elif last_wrong == -1:
        commit = series[0][0]   # committed at first trade
    else:
        commit = series[last_wrong + 1][0]
    commit_times.append(commit)

con.print(f"  N={len(commit_times)}  "
          f"median {median(commit_times)}s  "
          f"mean {mean(commit_times):.0f}s  "
          f"p25 {sorted(commit_times)[len(commit_times)//4]}s  "
          f"p75 {sorted(commit_times)[3*len(commit_times)//4]}s")

# histogram
buckets = defaultdict(int)
for t in commit_times:
    b = (t // 60) * 60
    buckets[b] += 1
con.print("\n  Commit time histogram (60s buckets):")
for b in sorted(buckets):
    bar = "█" * (buckets[b] * 40 // len(commit_times))
    con.print(f"    {b:4d}–{b+59:<4d}s  {buckets[b]:4d}  {bar}")


# ── 4. Accuracy curve split by outcome (winners vs losers) ─────────────────
con.print("\n[bold]Accuracy vs time — SPLIT by final outcome[/bold]")
con.print("  [dim](shows whether early lean is informative vs just noise in the losers)[/dim]")

win_w = [w for w in windows if w["overhang_matches_winner"]]
lose_w = [w for w in windows if not w["overhang_matches_winner"]]
con.print(f"  Winner overhang: {len(win_w)}   Loser overhang: {len(lose_w)}")

tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
tbl.add_column("Elapsed", justify="right")
tbl.add_column("Winners: lean→winner %", justify="right")
tbl.add_column("Losers: lean→winner %", justify="right")
tbl.add_column("Gap", justify="right")

for T in SAMPLE_TIMES:
    def pct_correct(wset):
        tot = 0; right = 0
        for w in wset:
            cu, cd = w["samples"][T]
            if cu == cd:
                continue
            lean = "Up" if cu > cd else "Down"
            tot += 1
            if lean == w["winner"]:
                right += 1
        return (right / tot) if tot else 0.0, tot
    p_w, n_w = pct_correct(win_w)
    p_l, n_l = pct_correct(lose_w)
    gap = p_w - p_l
    tbl.add_row(
        f"{T}s",
        f"{p_w:.0%} (n={n_w})",
        f"{p_l:.0%} (n={n_l})",
        f"{gap:+.0%}",
    )
con.print(tbl)


# ── 5. A few sample trajectories ───────────────────────────────────────────
con.print("\n[bold]Sample trajectories[/bold]  [dim](3 winning-overhang, 3 losing-overhang)[/dim]")
import random
random.seed(42)

def print_trajectory(w):
    con.print(f"\n[cyan]{w['key']}[/cyan]  coin={w['coin']}  winner={w['winner']}  "
              f"final lean={w['final_lean']}  "
              f"match={'✓' if w['overhang_matches_winner'] else '✗'}")
    con.print("  elapsed  cum_up    cum_dn    lean")
    for e, cu, cd in w["series"][:20]:
        lean = "Up  " if cu > cd else "Down" if cd > cu else "----"
        mark = "←" if (lean.strip() == w["winner"]) else " "
        con.print(f"   {e:4d}    {cu:8.1f}  {cd:8.1f}  {lean} {mark}")
    if len(w["series"]) > 20:
        con.print(f"   ...   ({len(w['series']) - 20} more trades)")

for w in random.sample(win_w, min(3, len(win_w))):
    print_trajectory(w)
for w in random.sample(lose_w, min(3, len(lose_w))):
    print_trajectory(w)
