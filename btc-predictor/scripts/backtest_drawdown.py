"""Max drawdown comparison: n-5 fixed-1 vs proportional sizing.

Re-uses the simulate_three_sizes() from backtest_proportional_size.py:
walks chronologically through the window history, tracks cumulative P&L
for each sizing variant, and reports peak-to-trough drawdown.
"""
from __future__ import annotations

import json, math, sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from src.collector.kalshi_feed import find_optimal_thresholds
from scripts.backtest_proportional_size import (
    simulate_three_sizes, load_windows,
)


def max_drawdown(cum_pnl_series):
    """Returns dict with max_dd, peak_idx, trough_idx, peak_val, trough_val.
    cum_pnl_series is a list of (window_idx, cum_pnl) tuples in time order.
    """
    peak = cum_pnl_series[0][1]
    peak_idx = 0
    max_dd = 0.0
    dd_peak_idx = 0
    dd_trough_idx = 0
    for i, (idx, c) in enumerate(cum_pnl_series):
        if c > peak:
            peak = c
            peak_idx = i
        dd = peak - c
        if dd > max_dd:
            max_dd = dd
            dd_peak_idx = peak_idx
            dd_trough_idx = i
    # Recovery time (windows from trough to new peak, or None if not recovered)
    if max_dd == 0:
        return {"max_dd": 0.0}
    recovery_idx = None
    for j in range(dd_trough_idx, len(cum_pnl_series)):
        if cum_pnl_series[j][1] >= cum_pnl_series[dd_peak_idx][1]:
            recovery_idx = j; break
    return {
        "max_dd":        max_dd,
        "peak_idx":      dd_peak_idx,
        "trough_idx":    dd_trough_idx,
        "peak_val":      cum_pnl_series[dd_peak_idx][1],
        "trough_val":    cum_pnl_series[dd_trough_idx][1],
        "duration":      dd_trough_idx - dd_peak_idx,
        "recovery_idx":  recovery_idx,
        "recovered_in":  (recovery_idx - dd_trough_idx) if recovery_idx else None,
    }


def main():
    wins = load_windows()
    print(f"Windows: {len(wins)}")
    print("Precomputing n-5 thresholds…")
    th_by_idx = {}
    for i in range(5, len(wins)):
        prior5 = wins[i-5:i]
        th = find_optimal_thresholds([(w["pts"], w["yw"]) for w in prior5])
        if th: th_by_idx[i] = (th["k_yes"], th["k_no"], th["d_yes"], th["d_no"])

    # Per-window P&L for each variant
    per_window = {"fixed": [], "prop_int": [], "prop_fp": []}
    cum = {"fixed": 0.0, "prop_int": 0.0, "prop_fp": 0.0}
    series = {"fixed": [], "prop_int": [], "prop_fp": []}

    for i in sorted(th_by_idx):
        th = th_by_idx[i]
        out = simulate_three_sizes(wins[i]["pts"], wins[i]["yw"], *th)
        for tag in ("fixed", "prop_int", "prop_fp"):
            pnl = out[tag]["pnl"]
            per_window[tag].append((i, pnl))
            cum[tag] += pnl
            series[tag].append((i, cum[tag]))

    print()
    print("="*78)
    print("MAX DRAWDOWN COMPARISON")
    print("="*78)
    print(f"{'variant':<22} {'final P&L':>12} {'peak P&L':>12} "
          f"{'max DD':>10} {'DD%':>7} {'dur':>5} {'recovery':>10}")
    print("-"*78)
    import datetime as dt
    def fmt_win(idx):
        et = dt.datetime.fromtimestamp(wins[idx]["w"], dt.timezone.utc).astimezone()
        return et.strftime("%m/%d %H:%M")
    for tag, label in [("fixed",    "A) fixed = 1"),
                        ("prop_int", "B) int round(d/Δ) "),
                        ("prop_fp",  "C) fp d/Δ no cap   ")]:
        s = series[tag]
        dd = max_drawdown(s)
        final = s[-1][1] if s else 0
        peak  = max(c for _, c in s) if s else 0
        if dd.get("max_dd", 0) == 0:
            print(f"{label:<22} ${final:>+11.2f} ${peak:>+11.2f}     $0.00      —     —          —")
            continue
        pct = dd["max_dd"] / peak * 100 if peak else 0
        rec = dd["recovered_in"]
        rec_s = f"{rec} wins" if rec is not None else "ongoing"
        print(f"{label:<22} ${final:>+11.2f} ${peak:>+11.2f} ${dd['max_dd']:>+9.2f} {pct:>6.1f}% {dd['duration']:>4} {rec_s:>10}")

    print()
    print("=== Drawdown details by variant ===")
    for tag, label in [("fixed",    "A) FIXED-1"),
                        ("prop_int", "B) PROPORTIONAL INT (cap 5)"),
                        ("prop_fp",  "C) PROPORTIONAL FP (no cap)")]:
        s = series[tag]
        dd = max_drawdown(s)
        if dd.get("max_dd", 0) == 0: continue
        peak_win = fmt_win(s[dd["peak_idx"]][0])
        trough_win = fmt_win(s[dd["trough_idx"]][0])
        rec_win = fmt_win(s[dd["recovery_idx"]][0]) if dd["recovery_idx"] is not None else "(not recovered)"
        print(f"\n  {label}")
        print(f"    Peak P&L      ${dd['peak_val']:+.2f}   at window {peak_win} ET (#{s[dd['peak_idx']][0]})")
        print(f"    Trough P&L    ${dd['trough_val']:+.2f}   at window {trough_win} ET (#{s[dd['trough_idx']][0]})")
        print(f"    Drawdown      ${dd['max_dd']:.2f}   ({dd['max_dd']/dd['peak_val']*100 if dd['peak_val']>0 else 0:.1f}% of peak)")
        print(f"    DD duration   {dd['duration']} windows ({dd['duration']*15} min)")
        print(f"    Recovered     {('after ' + str(dd['recovered_in']) + ' windows → ' + rec_win) if dd['recovered_in'] is not None else 'NOT YET (still below peak)'}")

    # Tail-risk: worst single-window losses
    print()
    print("=== Top 5 single-window losses by variant ===")
    for tag, label in [("fixed", "A) FIXED-1"), ("prop_int", "B) PROP INT"), ("prop_fp", "C) PROP FP")]:
        sorted_losses = sorted(per_window[tag], key=lambda x: x[1])[:5]
        print(f"  {label}")
        for idx, pnl in sorted_losses:
            print(f"    {fmt_win(idx)}  ${pnl:+.2f}")


if __name__ == "__main__":
    main()
