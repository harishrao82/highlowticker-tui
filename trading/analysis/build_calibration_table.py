"""build_calibration_table.py — generate the (coin, side, T-zone, ask-bucket)
calibration table from ~/.btc_windows.db tick history.

For each cell, computes:
  - n: number of samples
  - wr: realized win rate at that ask level
  - implied: implied prob (= bucket midpoint)
  - edge: wr - implied  (positive = underpriced, +EV to buy)

Writes ~/.calibration_table.json. The calibration_trader reads this at
startup and periodically (so re-running this script picks up fresh data
without restarting the trader).

Run nightly via cron:
    0 4 * * *  cd ~/highlowticker-tui && python3 build_calibration_table.py
"""
import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB         = Path.home() / ".btc_windows.db"
OUT        = Path.home() / ".calibration_table.json"
MIN_N      = 80     # cells with fewer samples → ignored
MIN_EDGE   = 0.04   # cells with |edge| < 4pp → ignored

# Tick checkpoints we sample at (one per window per checkpoint)
CHECKPOINTS = [30, 60, 120, 180, 300, 420, 600, 720, 840]

# Dead hours (ET): windows starting in these hours are excluded from the
# calibration table because their realized WR was much worse than implied
# and was polluting cells.
ET = timezone(timedelta(hours=-4))
DEAD_HOURS_ET = {0, 5, 19, 20}


def time_zone(elapsed: int) -> str:
    if elapsed < 60:    return "T<60"
    if elapsed < 180:   return "T60-180"
    if elapsed < 420:   return "T180-420"
    if elapsed < 720:   return "T420-720"
    return "T720+"


def bucket_label(ask: float) -> str | None:
    if ask is None or ask <= 0 or ask >= 1: return None
    # 1¢ buckets at extremes (≥0.85 or ≤0.15), 5¢ buckets elsewhere
    if ask >= 0.85 or ask <= 0.15:
        c = round(ask * 100)
        return f"{c/100:.2f}"
    lo = int(ask * 20) / 20.0
    return f"{lo:.2f}-{lo+0.05:.2f}"


def bucket_midpoint(label: str) -> float:
    if "-" in label:
        lo, hi = label.split("-")
        return (float(lo) + float(hi)) / 2
    return float(label)


def main():
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute(
        "SELECT id, ticker, winner, window_start_ts FROM windows "
        "WHERE ticker LIKE 'KX%15M%' AND winner IN ('yes','no')"
    )
    window_meta = {}
    n_dropped_dead = 0
    for wid, tk, winner, ws in cur.fetchall():
        coin = tk[2:5] if tk and len(tk) >= 5 else None
        if coin not in ("BTC", "ETH", "SOL", "XRP"): continue
        if ws and datetime.fromtimestamp(ws, tz=ET).hour in DEAD_HOURS_ET:
            n_dropped_dead += 1
            continue
        window_meta[wid] = (coin, winner)
    print(f"resolved windows: {len(window_meta):,}  (dropped {n_dropped_dead} dead-hour windows: ET hours {sorted(DEAD_HOURS_ET)})")

    # (coin, side, T-zone, bucket) -> {n, won}
    samples = defaultdict(lambda: {"n": 0, "won": 0})

    placeholders = ",".join("?" * len(CHECKPOINTS))
    for wid, (coin, winner) in window_meta.items():
        cur.execute(
            f"SELECT elapsed_sec, yes_ask, no_ask FROM ticks WHERE window_id=? "
            f"AND yes_ask IS NOT NULL AND no_ask IS NOT NULL "
            f"AND elapsed_sec IN ({placeholders})",
            (wid, *CHECKPOINTS)
        )
        for el, ya, na in cur.fetchall():
            tz = time_zone(el)
            for side, ask in (("yes", ya), ("no", na)):
                b = bucket_label(ask)
                if not b: continue
                k = (coin, side, tz, b)
                samples[k]["n"] += 1
                samples[k]["won"] += int(winner == side)

    # Build cells dict, only those passing thresholds
    cells = {}
    for (coin, side, tz, b), v in samples.items():
        if v["n"] < MIN_N: continue
        wr = v["won"] / v["n"]
        implied = bucket_midpoint(b)
        edge = wr - implied
        if abs(edge) < MIN_EDGE: continue
        key = f"{coin}|{side}|{tz}|{b}"
        cells[key] = {
            "n": v["n"],
            "wr": round(wr, 4),
            "implied": round(implied, 4),
            "edge": round(edge, 4),
        }

    out = {
        "built_at": time.time(),
        "windows_scanned": len(window_meta),
        "thresholds": {"min_n": MIN_N, "min_edge": MIN_EDGE},
        "cells": cells,
        "checkpoints": CHECKPOINTS,
    }
    OUT.write_text(json.dumps(out, indent=2))

    # Summary
    pos = sum(1 for c in cells.values() if c["edge"] > 0)
    neg = sum(1 for c in cells.values() if c["edge"] < 0)
    print(f"built {OUT}")
    print(f"  cells: {len(cells)} ({pos} underpriced, {neg} overpriced)")
    print(f"\n=== Top 15 underpriced ===")
    rows = sorted(cells.items(), key=lambda x: -x[1]["edge"])
    print(f"  {'cell':40} {'n':>4} {'WR':>6} {'imp':>5} {'edge':>6}")
    for k, v in rows[:15]:
        print(f"  {k:40} {v['n']:>4} {v['wr']*100:>5.1f}% "
              f"{v['implied']*100:>4.1f}% {v['edge']*100:>+5.1f}pp")


if __name__ == "__main__":
    main()
