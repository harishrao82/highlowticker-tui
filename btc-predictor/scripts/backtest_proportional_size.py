"""Backtest: same n-5 trigger logic, but share size = edge / threshold.

Compares three variants on the same window history with walk-forward
n-5 thresholds:

  A) FIXED size=1 (baseline — what the live system does today)
  B) Proportional integer  share = round(delta / Δ_threshold), capped at MAX_SHARES
  C) Proportional fractional share = delta / Δ_threshold (no rounding)

Reports total P&L, trade count, share-weighted average, and the
size distribution under each variant.

Caveats:
  - Uses the same "taker at recorded ask" assumption as simulate_window,
    not the maker-first execution. To layer execution on top, see
    tune_execution.py — combination is a follow-up if results look good.
  - Kalshi accepts `count_fp` to 2 decimal places so variant C is
    realistic for live trading; variant B exists for legibility.
"""
from __future__ import annotations

import json, math, sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from src.collector.kalshi_feed import find_optimal_thresholds

PREDICTIONS_PATH = REPO_ROOT / "data" / "predictions.jsonl"

MIN_SEC      = 30
THROTTLE_SEC = 3
MAX_SHARES   = 5    # cap on variant B (rounded integer sizing)


# ── Outcome helper (Kalshi-grounded — mirrors _determine_outcome) ────
def yes_won_for(rows_sorted):
    last = rows_sorted[-1]
    ymid = last.get("kalshi_yes_mid")
    sec  = last.get("sample_sec")
    if ymid is not None and sec is not None and sec >= 870:
        return ymid > 0.5
    opx = rows_sorted[0].get("window_open_px")
    cpx = last.get("current_price")
    if opx is None or cpx is None:
        return False
    return cpx > opx


def simulate_three_sizes(rows, yes_won, k_yes, k_no, d_yes, d_no):
    """Run the same trigger logic three times — fixed, integer-prop, fp-prop.
    Returns a dict {fixed, prop_int, prop_fp} each with pnl, n_trades, shares_total.
    """
    out = {
        "fixed":    {"pnl": 0.0, "n_trades": 0, "shares": 0.0, "sizes": []},
        "prop_int": {"pnl": 0.0, "n_trades": 0, "shares": 0.0, "sizes": []},
        "prop_fp":  {"pnl": 0.0, "n_trades": 0, "shares": 0.0, "sizes": []},
    }
    last_y = last_n = -10_000

    for r in rows:
        s  = r.get("sample_sec")
        if s is None or s < MIN_SEC: continue
        yb = r.get("kalshi_yes_bid"); ya = r.get("kalshi_yes_ask")
        pg = r.get("p_green")
        if yb is None or ya is None or pg is None: continue

        ym = (yb + ya) / 2.0
        nm = 1.0 - ym
        pn = 1.0 - pg
        dy = pg - ym
        dn = pn - nm

        # YES side
        if dy > d_yes and ym > k_yes and s - last_y >= THROTTLE_SEC:
            payoff = 1.0 if yes_won else 0.0
            entry  = ya
            edge   = dy                       # actual edge
            thresh = d_yes                    # configured threshold
            size_fp  = edge / thresh          # variant C
            size_int = max(1, min(MAX_SHARES, round(size_fp)))  # variant B
            for tag, sz in (("fixed", 1), ("prop_int", size_int), ("prop_fp", size_fp)):
                out[tag]["pnl"]      += (payoff - entry) * sz
                out[tag]["n_trades"] += 1
                out[tag]["shares"]   += sz
                out[tag]["sizes"].append(sz)
            last_y = s
            continue

        # NO side
        if dn > d_no and nm > k_no and s - last_n >= THROTTLE_SEC:
            payoff = 1.0 if not yes_won else 0.0
            entry  = 1.0 - yb                 # NO_ask
            edge   = dn
            thresh = d_no
            size_fp  = edge / thresh
            size_int = max(1, min(MAX_SHARES, round(size_fp)))
            for tag, sz in (("fixed", 1), ("prop_int", size_int), ("prop_fp", size_fp)):
                out[tag]["pnl"]      += (payoff - entry) * sz
                out[tag]["n_trades"] += 1
                out[tag]["shares"]   += sz
                out[tag]["sizes"].append(sz)
            last_n = s
    return out


def load_windows():
    by_win = defaultdict(list)
    with open(PREDICTIONS_PATH) as f:
        for line in f:
            try: r = json.loads(line)
            except: continue
            w = r.get("window_open_ts")
            if w: by_win[w].append(r)
    wins = []
    for w, pts in by_win.items():
        if max(p.get("sample_sec", 0) for p in pts) < 870: continue
        pts_sorted = sorted(pts, key=lambda p: p.get("sample_sec", 0))
        opx = pts_sorted[0].get("window_open_px")
        cpx = pts_sorted[-1].get("current_price")
        if opx is None or cpx is None: continue
        if not (math.isfinite(opx) and math.isfinite(cpx)): continue
        wins.append({"w": w, "pts": pts_sorted, "yw": yes_won_for(pts_sorted)})
    wins.sort(key=lambda x: x["w"])
    return wins


def main():
    wins = load_windows()
    print(f"Clean windows in log: {len(wins)}  "
          f"({sum(1 for w in wins if w['yw'])} UP / {sum(1 for w in wins if not w['yw'])} DOWN)")

    # Precompute walk-forward n-5 thresholds for each window.
    print(f"Precomputing n-5 thresholds…")
    th_by_idx = {}
    for i in range(5, len(wins)):
        prior5 = wins[i-5:i]
        th = find_optimal_thresholds([(w["pts"], w["yw"]) for w in prior5])
        if th: th_by_idx[i] = (th["k_yes"], th["k_no"], th["d_yes"], th["d_no"])

    # Run sim with all three sizing variants.
    totals = {
        "fixed":    {"pnl": 0.0, "n_trades": 0, "shares": 0.0, "sizes": []},
        "prop_int": {"pnl": 0.0, "n_trades": 0, "shares": 0.0, "sizes": []},
        "prop_fp":  {"pnl": 0.0, "n_trades": 0, "shares": 0.0, "sizes": []},
    }
    for i in sorted(th_by_idx):
        th = th_by_idx[i]
        out = simulate_three_sizes(wins[i]["pts"], wins[i]["yw"], *th)
        for tag in totals:
            totals[tag]["pnl"]      += out[tag]["pnl"]
            totals[tag]["n_trades"] += out[tag]["n_trades"]
            totals[tag]["shares"]   += out[tag]["shares"]
            totals[tag]["sizes"].extend(out[tag]["sizes"])

    print()
    print("="*78)
    print("RESULTS (across", len(th_by_idx), "walk-forward windows)")
    print("="*78)
    print(f"{'variant':<20} {'n_trades':>9} {'total shares':>14} "
          f"{'total P&L':>10} {'$ / trade':>10} {'$ / share':>10}")
    print("-"*78)
    for tag, label in [("fixed",    "A) fixed = 1"),
                        ("prop_int", "B) int round(d/Δ)"),
                        ("prop_fp",  "C) fp d/Δ (capped)" if False else "C) fp d/Δ no cap")]:
        t = totals[tag]
        pps  = t["pnl"] / max(t["n_trades"], 1)
        ppsh = t["pnl"] / max(t["shares"], 0.0001)
        print(f"{label:<20} {t['n_trades']:>9} {t['shares']:>14.1f} "
              f"${t['pnl']:>+9.2f} ${pps:>+9.4f} ${ppsh:>+9.4f}")

    print()
    print("=== size distribution ===")
    for tag, label in [("prop_int", "B) integer rounding (capped at "+str(MAX_SHARES)+")"),
                       ("prop_fp",  "C) fractional (no cap)")]:
        sizes = totals[tag]["sizes"]
        from collections import Counter
        if tag == "prop_int":
            c = Counter(sizes)
            print(f"  {label}")
            for k in sorted(c):
                print(f"    {k} sh  → {c[k]:>5} trades  ({c[k]/len(sizes)*100:.1f}%)")
        else:
            # bucket fractional sizes into ranges
            buckets = Counter()
            for s in sizes:
                if   s < 1.0:  buckets["< 1.0"] += 1
                elif s < 1.5:  buckets["1.0-1.5"] += 1
                elif s < 2.0:  buckets["1.5-2.0"] += 1
                elif s < 3.0:  buckets["2.0-3.0"] += 1
                elif s < 5.0:  buckets["3.0-5.0"] += 1
                else:          buckets["5.0+"] += 1
            print(f"  {label}")
            for k in ["< 1.0", "1.0-1.5", "1.5-2.0", "2.0-3.0", "3.0-5.0", "5.0+"]:
                if buckets.get(k, 0) > 0:
                    print(f"    {k:<10} → {buckets[k]:>5} trades  ({buckets[k]/len(sizes)*100:.1f}%)")
            if sizes:
                print(f"    mean: {sum(sizes)/len(sizes):.2f}sh   median: {sorted(sizes)[len(sizes)//2]:.2f}sh   max: {max(sizes):.2f}sh")

    print()
    print("="*78)
    print("VERDICT")
    print("="*78)
    base = totals["fixed"]["pnl"]
    delta_int = totals["prop_int"]["pnl"] - base
    delta_fp  = totals["prop_fp"]["pnl"]  - base
    pct_int = delta_int / abs(base) * 100 if base else 0
    pct_fp  = delta_fp  / abs(base) * 100 if base else 0
    print(f"  Fixed (1 share):              ${base:+.2f}")
    print(f"  Proportional integer (B):     ${totals['prop_int']['pnl']:+.2f}    "
          f"Δ ${delta_int:+.2f} ({pct_int:+.1f}%)")
    print(f"  Proportional fractional (C):  ${totals['prop_fp']['pnl']:+.2f}    "
          f"Δ ${delta_fp:+.2f} ({pct_fp:+.1f}%)")
    print()
    print(f"  Share-weighted P&L per share:")
    for tag in ("fixed", "prop_int", "prop_fp"):
        t = totals[tag]
        ppsh = t["pnl"] / max(t["shares"], 0.0001)
        print(f"    {tag:>10}:  ${ppsh:+.4f}/sh   ({int(t['shares'])} shares total)")
    print()
    print(f"  Capital deployed (sum of entry × shares) tells the real story:")
    # Per-trade capital ≈ entry_price × shares. Tracking it would let us
    # compute return-on-capital. For now, P&L-per-share is a fair proxy
    # since entry prices cluster around the same range across variants.


if __name__ == "__main__":
    main()
