"""Should we market the unfilled remainder after the maker TTL expires,
or just drop the trade?

Runs the maker-first simulator twice on the full history (with the SAME
n-5 thresholds for trigger logic, fixed (0¢, 3s) execution):

  A) Drop unfilled trades — no market fallback. We only count maker fills.
  B) Market unfilled trades — market fallback at current ask + 5¢ cap.

Reports separate aggregates for maker-only fills vs market-only fills so
we can see whether the market fallback is a net positive or net negative.
"""
from __future__ import annotations

import json, math, sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from src.collector.kalshi_feed import find_optimal_thresholds
from scripts.tune_execution import (
    load_windows, fee, MARKET_CAP_CENTS, MIN_SEC, THROTTLE_SEC,
)

OFFSET_CENTS = 0
TTL_SEC      = 3


def simulate_window_split(
    rows, yes_won, k_yes, k_no, d_yes, d_no,
    market_fallback: bool,
):
    """Same as simulate_window_maker_first but reports per-path aggregates."""
    rows_by_sec = {r["sample_sec"]: r for r in rows if r.get("sample_sec") is not None}
    pnl_maker = pnl_market = 0.0
    n_maker = n_market = n_skipped = 0
    fees_maker = fees_market = 0.0
    last_y = last_n = -10_000

    def buy_yes(t, yb, ya):
        nonlocal pnl_maker, pnl_market, n_maker, n_market, n_skipped
        nonlocal fees_maker, fees_market
        maker_price = round(max(0.01, yb - OFFSET_CENTS * 0.01), 2)
        # Maker fill check
        for dt_s in range(1, TTL_SEC + 1):
            nr = rows_by_sec.get(t + dt_s)
            if nr is None: continue
            nyb = nr.get("kalshi_yes_bid")
            if nyb is not None and nyb <= maker_price:
                f = fee(maker_price, is_maker=True)
                pnl_maker += (1.0 if yes_won else 0.0) - maker_price - f
                fees_maker += f
                n_maker += 1
                return
        # Maker expired
        if not market_fallback:
            n_skipped += 1
            return
        fb = rows_by_sec.get(t + TTL_SEC)
        if fb is None: n_skipped += 1; return
        fb_ya = fb.get("kalshi_yes_ask")
        if fb_ya is None: n_skipped += 1; return
        cap = min(0.99, fb_ya + MARKET_CAP_CENTS * 0.01)
        if fb_ya > cap: n_skipped += 1; return
        f = fee(fb_ya, is_maker=False)
        pnl_market += (1.0 if yes_won else 0.0) - fb_ya - f
        fees_market += f
        n_market += 1

    def buy_no(t, yb, ya):
        nonlocal pnl_maker, pnl_market, n_maker, n_market, n_skipped
        nonlocal fees_maker, fees_market
        no_bid = round(1 - ya, 2)
        maker_price = round(max(0.01, no_bid - OFFSET_CENTS * 0.01), 2)
        threshold_ya = 1 - maker_price
        for dt_s in range(1, TTL_SEC + 1):
            nr = rows_by_sec.get(t + dt_s)
            if nr is None: continue
            nya = nr.get("kalshi_yes_ask")
            if nya is not None and nya >= threshold_ya:
                f = fee(maker_price, is_maker=True)
                pnl_maker += (1.0 if not yes_won else 0.0) - maker_price - f
                fees_maker += f
                n_maker += 1
                return
        if not market_fallback:
            n_skipped += 1
            return
        fb = rows_by_sec.get(t + TTL_SEC)
        if fb is None: n_skipped += 1; return
        fb_yb = fb.get("kalshi_yes_bid")
        if fb_yb is None: n_skipped += 1; return
        fb_no_ask = 1 - fb_yb
        cap = min(0.99, fb_no_ask + MARKET_CAP_CENTS * 0.01)
        if fb_no_ask > cap: n_skipped += 1; return
        f = fee(fb_no_ask, is_maker=False)
        pnl_market += (1.0 if not yes_won else 0.0) - fb_no_ask - f
        fees_market += f
        n_market += 1

    for r in rows:
        s  = r.get("sample_sec")
        if s is None or s < MIN_SEC: continue
        yb = r.get("kalshi_yes_bid"); ya = r.get("kalshi_yes_ask")
        pg = r.get("p_green")
        if yb is None or ya is None or pg is None: continue
        ym = (yb + ya) / 2.0; nm = 1.0 - ym; pn = 1.0 - pg
        dy = pg - ym; dn = pn - nm
        if dy > d_yes and ym > k_yes and s - last_y >= THROTTLE_SEC:
            buy_yes(s, yb, ya); last_y = s; continue
        if dn > d_no and nm > k_no and s - last_n >= THROTTLE_SEC:
            buy_no(s, yb, ya); last_n = s
    return {
        "pnl_maker": pnl_maker, "pnl_market": pnl_market,
        "n_maker": n_maker, "n_market": n_market, "n_skipped": n_skipped,
        "fees_maker": fees_maker, "fees_market": fees_market,
    }


def main():
    wins = load_windows()
    print(f"{len(wins)} clean windows")

    # n-5 thresholds for each window
    th_by_idx = {}
    for i in range(5, len(wins)):
        prior5 = wins[i-5:i]
        th = find_optimal_thresholds([(w["pts"], w["yw"]) for w in prior5])
        if th: th_by_idx[i] = (th["k_yes"], th["k_no"], th["d_yes"], th["d_no"])

    print(f"Running both variants on {len(th_by_idx)} windows…")
    print()

    drop_totals     = {"pnl_maker":0.0, "pnl_market":0.0, "n_maker":0, "n_market":0, "n_skipped":0, "fees_maker":0.0, "fees_market":0.0}
    fallback_totals = {"pnl_maker":0.0, "pnl_market":0.0, "n_maker":0, "n_market":0, "n_skipped":0, "fees_maker":0.0, "fees_market":0.0}

    for i in sorted(th_by_idx):
        th = th_by_idx[i]
        r_drop = simulate_window_split(wins[i]["pts"], wins[i]["yw"], *th, market_fallback=False)
        r_fall = simulate_window_split(wins[i]["pts"], wins[i]["yw"], *th, market_fallback=True)
        for k in drop_totals:
            drop_totals[k]     += r_drop[k]
            fallback_totals[k] += r_fall[k]

    print("="*78)
    print(f"A) DROP unfilled (maker-only, no market fallback)")
    print("="*78)
    pnl_total = drop_totals["pnl_maker"]
    n_total   = drop_totals["n_maker"]
    print(f"  Total P&L:        ${pnl_total:+.2f}")
    print(f"  Trades (maker):   {drop_totals['n_maker']}")
    print(f"  Trades skipped:   {drop_totals['n_skipped']}  ({drop_totals['n_skipped']/max(drop_totals['n_maker']+drop_totals['n_skipped'],1)*100:.0f}% of signals)")
    print(f"  Avg P&L/trade:    ${pnl_total/max(n_total,1):+.4f}")
    print(f"  Fees paid:        ${drop_totals['fees_maker']:.2f}")

    print()
    print("="*78)
    print(f"B) MARKET fallback (current design)")
    print("="*78)
    pnl_total_b = fallback_totals["pnl_maker"] + fallback_totals["pnl_market"]
    n_total_b   = fallback_totals["n_maker"] + fallback_totals["n_market"]
    print(f"  Total P&L:        ${pnl_total_b:+.2f}")
    print(f"    from maker:     ${fallback_totals['pnl_maker']:+.2f}  ({fallback_totals['n_maker']} trades)")
    print(f"    from market:    ${fallback_totals['pnl_market']:+.2f}  ({fallback_totals['n_market']} trades)")
    print(f"  Trades (total):   {n_total_b}")
    print(f"  Trades skipped:   {fallback_totals['n_skipped']}")
    print(f"  Avg P&L/trade:    ${pnl_total_b/max(n_total_b,1):+.4f}")
    print(f"    maker P&L/trade:  ${fallback_totals['pnl_maker']/max(fallback_totals['n_maker'],1):+.4f}")
    print(f"    market P&L/trade: ${fallback_totals['pnl_market']/max(fallback_totals['n_market'],1):+.4f}")
    print(f"  Fees paid:        ${fallback_totals['fees_maker']+fallback_totals['fees_market']:.2f}  "
          f"(maker ${fallback_totals['fees_maker']:.2f}, taker ${fallback_totals['fees_market']:.2f})")

    print()
    print("="*78)
    print("VERDICT")
    print("="*78)
    delta = pnl_total_b - pnl_total
    print(f"  Drop unfilled:    ${pnl_total:+.2f}")
    print(f"  Market fallback:  ${pnl_total_b:+.2f}")
    print(f"  Delta (B − A):    ${delta:+.2f}  →  "
          f"{'MARKET FALLBACK adds value' if delta > 0 else 'DROP is better'}")
    print(f"  Market-leg alone P&L:  ${fallback_totals['pnl_market']:+.2f}  ({fallback_totals['n_market']} trades, "
          f"avg ${fallback_totals['pnl_market']/max(fallback_totals['n_market'],1):+.4f}/trade)")


if __name__ == "__main__":
    main()
