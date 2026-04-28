"""build_wr_table.py — derive (coin, side, delta_bucket) → multiplier table
from taken + skipped MOM trades.

Inputs (all in $HOME):
  ~/.kalshi_momentum_trades.jsonl   — taken entries (have won, ask_at_trigger)
  ~/.kalshi_filtered_signals.jsonl  — skipped signals (need cfb spot at close)
  ~/.cfb_proxy_log.jsonl            — per-coin spot price ticks

Output:
  ~/.mom_wr_table.json
    {
      "built_at": <unix_ts>,
      "n_samples": <int>,
      "thresholds": {...},
      "cells": {
         "BTC|yes|0.05-0.08": {"n": 83, "wr": 0.795, "avg_ask": 0.726,
                                "roi": 0.151, "mult": 1.5},
         ...
      }
    }

Sample inclusion: earliest signal per (coin, side, window_start_ts), T <= 180s,
with a resolvable winner.

Multiplier policy (conservative, start small):
  n >= 30:
    ROI > 0.10  → 1.5
    ROI < -0.08 → 0.0  (skip)
    else        → 1.0
  n < 30:       → 1.0  (insufficient data, neutral)

Rerun nightly via cron:
  0 3 * * *  cd /Users/Harish/highlowticker-tui && python3 build_wr_table.py
"""
import json
import time
from bisect import bisect_left
from collections import defaultdict
from pathlib import Path

CFB        = Path.home() / ".cfb_proxy_log.jsonl"
TAKEN      = Path.home() / ".kalshi_momentum_trades.jsonl"
SKIPPED    = Path.home() / ".kalshi_filtered_signals.jsonl"
OUT        = Path.home() / ".mom_wr_table.json"

T_MAX = 180          # only use entries decided within first 3 minutes
MIN_N_FOR_MULT = 30  # below this, default to 1.0× (insufficient data)
ROI_BOOST_THR  = 0.10
ROI_SKIP_THR   = -0.08

# Same bucket boundaries as the analysis scripts — must match runtime lookup.
DELTA_BUCKETS = [
    ("<0.015",     0.000, 0.015),
    ("0.015-0.03", 0.015, 0.030),
    ("0.03-0.05",  0.030, 0.050),
    ("0.05-0.08",  0.050, 0.080),
    ("0.08-0.15",  0.080, 0.150),
    ("0.15-0.30",  0.150, 0.300),
    ("0.30-0.50",  0.300, 0.500),
    (">=0.50",     0.500, 1e9),
]


def delta_bucket(d: float) -> str:
    a = abs(d)
    for label, lo, hi in DELTA_BUCKETS:
        if lo <= a < hi:
            return label
    return DELTA_BUCKETS[-1][0]


def main():
    # ── Load cfb (sorted ts/px per coin) ──
    print("Loading cfb price log…")
    cts: dict[str, list[float]] = defaultdict(list)
    cpx: dict[str, list[float]] = defaultdict(list)
    with open(CFB) as f:
        for line in f:
            try: r = json.loads(line)
            except Exception: continue
            c, m = r.get("coin"), r.get("mid")
            if not c or m is None: continue
            cts[c].append(r["ts"]); cpx[c].append(m)
    for c in cts:
        pairs = sorted(zip(cts[c], cpx[c]))
        cts[c] = [p[0] for p in pairs]; cpx[c] = [p[1] for p in pairs]
    print(f"  cfb covers {len(cts.get('BTC', []) ):,} ticks for BTC")

    def price_at(coin, ts, lag=120):
        a = cts.get(coin)
        if not a: return None
        i = bisect_left(a, ts)
        if i >= len(a): i = len(a) - 1
        if i > 0 and (a[i] - ts) > (ts - a[i-1]): i -= 1
        if abs(a[i] - ts) > lag: return None
        return cpx[coin][i]

    def resolve_won(coin, side, ws, open_price):
        if open_price is None: return None
        close = price_at(coin, ws + 900)
        if close is None: return None
        return int((close > open_price) if side == "yes" else (close < open_price))

    # ── Build sample set ──
    seen: dict[tuple, dict] = {}

    def add_sample(coin, side, ws, delta, ask, won, T):
        if won is None or T is None or T > T_MAX: return
        if not (coin and side and ws and ask and ask > 0): return
        k = (coin, side, ws)
        if k in seen and seen[k]["T"] <= T: return
        seen[k] = {"coin": coin, "side": side, "delta": delta,
                   "ask": ask, "won": int(bool(won)), "T": T}

    print("Loading taken trades…")
    n_taken = 0
    with open(TAKEN) as f:
        for line in f:
            try: r = json.loads(line)
            except Exception: continue
            if r.get("won") is None: continue
            T   = r.get("T_checkpoint") or r.get("t_checkpoint")
            ask = r.get("ask_at_trigger") or r.get("entry_price") or 0
            ws  = r.get("window_start_ts") or r.get("window_start")
            add_sample(r.get("coin"), r.get("side"), ws,
                       r.get("delta_pct", 0.0), ask, int(bool(r["won"])), T)
            n_taken += 1
    print(f"  taken: {n_taken:,}")

    print("Resolving skipped trades via cfb…")
    n_skipped, n_resolved = 0, 0
    with open(SKIPPED) as f:
        for line in f:
            try: r = json.loads(line)
            except Exception: continue
            n_skipped += 1
            ws = r.get("window_start_ts")
            if not ws: continue
            won = resolve_won(r.get("coin"), r.get("side"), ws, r.get("open_price"))
            if won is None: continue
            n_resolved += 1
            add_sample(r.get("coin"), r.get("side"), ws,
                       r.get("delta_pct", 0.0),
                       r.get("cur_ask") or 0,
                       won, r.get("T_checkpoint"))
    print(f"  skipped scanned: {n_skipped:,}  resolved: {n_resolved:,}")
    print(f"  earliest-per-window samples: {len(seen):,}")

    # ── Aggregate cells ──
    cells: dict[str, dict[str, float]] = {}
    bucket_n: dict[tuple, dict] = defaultdict(lambda: {"n": 0, "w": 0, "ask": 0.0, "d": 0.0})
    for v in seen.values():
        b = delta_bucket(v["delta"])
        k = (v["coin"], v["side"], b)
        bn = bucket_n[k]
        bn["n"] += 1; bn["w"] += v["won"]; bn["ask"] += v["ask"]; bn["d"] += v["delta"]

    n_boost = n_skip = n_neutral = n_thin = 0
    for (coin, side, bucket), bn in bucket_n.items():
        n = bn["n"]
        wr = bn["w"] / n
        ask = bn["ask"] / n
        avg_d = bn["d"] / n
        ev = wr * (1 - ask) - (1 - wr) * ask
        roi = ev / ask if ask > 0 else 0.0
        if n < MIN_N_FOR_MULT:
            mult = 1.0; tag = "thin"; n_thin += 1
        elif roi >= ROI_BOOST_THR:
            mult = 1.5; tag = "boost"; n_boost += 1
        elif roi <= ROI_SKIP_THR:
            mult = 0.0; tag = "skip"; n_skip += 1
        else:
            mult = 1.0; tag = "neutral"; n_neutral += 1
        cells[f"{coin}|{side}|{bucket}"] = {
            "n": n,
            "wr": round(wr, 4),
            "avg_ask": round(ask, 4),
            "avg_delta": round(avg_d, 5),
            "ev": round(ev, 4),
            "roi": round(roi, 4),
            "mult": mult,
            "tag": tag,
        }

    out = {
        "built_at": time.time(),
        "n_samples": len(seen),
        "thresholds": {
            "T_MAX": T_MAX,
            "MIN_N_FOR_MULT": MIN_N_FOR_MULT,
            "ROI_BOOST_THR": ROI_BOOST_THR,
            "ROI_SKIP_THR": ROI_SKIP_THR,
        },
        "delta_buckets": [b[0] for b in DELTA_BUCKETS],
        "cells": cells,
    }
    OUT.write_text(json.dumps(out, indent=2))

    # ── Print summary ──
    print()
    print(f"=== Built {OUT} ===")
    print(f"  cells: {len(cells)}  ({n_boost} boost, {n_skip} skip, "
          f"{n_neutral} neutral, {n_thin} thin)")
    print()
    print("=== Boost cells (mult=1.5) ===")
    rows = [(k, v) for k, v in cells.items() if v["tag"] == "boost"]
    rows.sort(key=lambda x: -x[1]["roi"])
    print(f"  {'cell':28} {'n':>5} {'WR':>6} {'avg_ask':>8} {'ROI':>7}")
    for k, v in rows:
        print(f"  {k:28} {v['n']:>5} {v['wr']:>6.1%} {v['avg_ask']:>8.3f} {v['roi']:>+7.1%}")
    print()
    print("=== Skip cells (mult=0) ===")
    rows = [(k, v) for k, v in cells.items() if v["tag"] == "skip"]
    rows.sort(key=lambda x: x[1]["roi"])
    print(f"  {'cell':28} {'n':>5} {'WR':>6} {'avg_ask':>8} {'ROI':>7}")
    for k, v in rows:
        print(f"  {k:28} {v['n']:>5} {v['wr']:>6.1%} {v['avg_ask']:>8.3f} {v['roi']:>+7.1%}")


if __name__ == "__main__":
    main()
