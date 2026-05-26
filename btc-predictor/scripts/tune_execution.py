"""Backtest: maker-first execution tuning.

Compares two strategies for choosing (maker_offset_cents, maker_ttl_sec):

  1. Walk-forward n-5:   at each window i, grid-search the prior 5 windows for
                          the best execution params, apply to window i.
  2. In-sample one-shot: grid-search over the full window history, pick the
                          single (offset, ttl) that maximizes total P&L.

Both share the same trigger thresholds (themselves auto-tuned n-5).

Maker fill rule from per-second bid/ask snapshots:
    A maker BUY YES at price L fills if min(yes_bid[t+1 .. t+TTL]) <= L.
    NO side uses parity: NO_bid = 1 - yes_ask.

Caveats:
    - 1-sec snapshot resolution loses sub-second crosses
    - Assumes head-of-queue (optimistic fill rate by 10-20pp)
    - Use the COMPARISON across combos, not absolute fill counts.
"""
from __future__ import annotations

import json
import math
import sys
import datetime as dt
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from src.collector.kalshi_feed import find_optimal_thresholds

PREDICTIONS_PATH = REPO_ROOT / "data" / "predictions.jsonl"

# Execution parameter grid
OFFSET_CENTS_GRID = [0, 1, 2, 3]
TTL_SEC_GRID      = [3, 5, 8, 12, 20, 30]
MARKET_CAP_CENTS  = 5         # fixed; market fallback = current_ask + 5¢

# Trigger params — these stay throttled at the same constants as live
MIN_SEC      = 30
THROTTLE_SEC = 3

# Fee model (Kalshi schedule, in dollars per share):
#   fee = rate × price × (1 - price)
FEE_TAKER = 0.07
FEE_MAKER = 0.02


# ─── Outcome helper (Kalshi-grounded, mirrors _determine_outcome) ──────
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


def fee(price: float, is_maker: bool) -> float:
    rate = FEE_MAKER if is_maker else FEE_TAKER
    return rate * price * (1 - price)


# ─── The maker-first simulator ─────────────────────────────────────────
def simulate_window_maker_first(
    rows: list[dict], yes_won: bool,
    k_yes: float, k_no: float, d_yes: float, d_no: float,
    offset_cents: int, ttl_sec: int,
    market_cap_cents: int = MARKET_CAP_CENTS,
) -> dict:
    """Same trigger logic as simulate_window, with maker→market execution."""
    rows_by_sec = {r["sample_sec"]: r for r in rows if r.get("sample_sec") is not None}

    pnl = 0.0
    fees_total = 0.0
    savings_vs_taker = 0.0
    n_maker = n_market = n_nofill = 0
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

        # ── YES side ──────────────────────────────────────────────
        if dy > d_yes and ym > k_yes and s - last_y >= THROTTLE_SEC:
            entry, ex_fee, path = _exec_buy_yes(
                rows_by_sec, s, yb, ya, ttl_sec, offset_cents, market_cap_cents,
            )
            if path != "nofill":
                payoff = 1.0 if yes_won else 0.0
                trade_pnl = payoff - entry - ex_fee
                # Baseline = taker at recorded ask + 2¢ slip
                baseline_entry = min(0.99, ya + 0.02)
                baseline_fee   = fee(baseline_entry, is_maker=False)
                baseline_pnl   = payoff - baseline_entry - baseline_fee
                savings_vs_taker += trade_pnl - baseline_pnl
                pnl += trade_pnl
                fees_total += ex_fee
                if path == "maker": n_maker += 1
                else:               n_market += 1
            else:
                n_nofill += 1
            last_y = s
            continue

        # ── NO side ───────────────────────────────────────────────
        if dn > d_no and nm > k_no and s - last_n >= THROTTLE_SEC:
            entry, ex_fee, path = _exec_buy_no(
                rows_by_sec, s, yb, ya, ttl_sec, offset_cents, market_cap_cents,
            )
            if path != "nofill":
                payoff = 1.0 if not yes_won else 0.0
                trade_pnl = payoff - entry - ex_fee
                # Baseline: taker at NO ask (= 1 - yes_bid) + 2¢
                baseline_entry = min(0.99, (1 - yb) + 0.02)
                baseline_fee   = fee(baseline_entry, is_maker=False)
                baseline_pnl   = payoff - baseline_entry - baseline_fee
                savings_vs_taker += trade_pnl - baseline_pnl
                pnl += trade_pnl
                fees_total += ex_fee
                if path == "maker": n_maker += 1
                else:               n_market += 1
            else:
                n_nofill += 1
            last_n = s

    n_trades = n_maker + n_market
    return {
        "pnl": pnl, "n_trades": n_trades,
        "n_maker": n_maker, "n_market": n_market, "n_nofill": n_nofill,
        "fees": fees_total, "savings_vs_taker": savings_vs_taker,
    }


def _exec_buy_yes(rows_by_sec, t, yb, ya, ttl_sec, offset_cents, market_cap_cents):
    maker_price = round(max(0.01, yb - offset_cents * 0.01), 2)
    # Maker fill check
    for dt_s in range(1, ttl_sec + 1):
        nr = rows_by_sec.get(t + dt_s)
        if nr is None: continue
        nyb = nr.get("kalshi_yes_bid")
        if nyb is not None and nyb <= maker_price:
            return maker_price, fee(maker_price, is_maker=True), "maker"
    # Market fallback at t + ttl_sec
    fb = rows_by_sec.get(t + ttl_sec)
    if fb is None: return 0.0, 0.0, "nofill"
    fb_ya = fb.get("kalshi_yes_ask")
    if fb_ya is None: return 0.0, 0.0, "nofill"
    cap = min(0.99, fb_ya + market_cap_cents * 0.01)
    if fb_ya > cap: return 0.0, 0.0, "nofill"
    return fb_ya, fee(fb_ya, is_maker=False), "market"


def _exec_buy_no(rows_by_sec, t, yb, ya, ttl_sec, offset_cents, market_cap_cents):
    # NO_bid = 1 - yes_ask. Maker NO at (NO_bid - offset).
    no_bid = round(1 - ya, 2)
    maker_price = round(max(0.01, no_bid - offset_cents * 0.01), 2)
    # NO bid filled if yes_ask rises to >= (1 - maker_price)
    threshold_ya = 1 - maker_price
    for dt_s in range(1, ttl_sec + 1):
        nr = rows_by_sec.get(t + dt_s)
        if nr is None: continue
        nya = nr.get("kalshi_yes_ask")
        if nya is not None and nya >= threshold_ya:
            return maker_price, fee(maker_price, is_maker=True), "maker"
    # Market fallback at t+ttl: take NO at NO_ask = 1 - yes_bid
    fb = rows_by_sec.get(t + ttl_sec)
    if fb is None: return 0.0, 0.0, "nofill"
    fb_yb = fb.get("kalshi_yes_bid")
    if fb_yb is None: return 0.0, 0.0, "nofill"
    fb_no_ask = 1 - fb_yb
    cap = min(0.99, fb_no_ask + market_cap_cents * 0.01)
    if fb_no_ask > cap: return 0.0, 0.0, "nofill"
    return fb_no_ask, fee(fb_no_ask, is_maker=False), "market"


# ─── Window loading ────────────────────────────────────────────────────
def load_windows():
    by_win = defaultdict(list)
    with open(PREDICTIONS_PATH) as f:
        for line in f:
            try: r = json.loads(line)
            except: continue
            w = r.get("window_open_ts")
            if w: by_win[w].append(r)
    windows = []
    for w, pts in by_win.items():
        if max(p.get("sample_sec", 0) for p in pts) < 870: continue
        pts_sorted = sorted(pts, key=lambda p: p.get("sample_sec", 0))
        opx = pts_sorted[0].get("window_open_px")
        cpx = pts_sorted[-1].get("current_price")
        if opx is None or cpx is None: continue
        if not (math.isfinite(opx) and math.isfinite(cpx)): continue
        yw = yes_won_for(pts_sorted)
        windows.append({"w": w, "pts": pts_sorted, "yw": yw,
                        "et": dt.datetime.fromtimestamp(w, dt.timezone.utc)
                              .astimezone().strftime("%m/%d %H:%M ET")})
    windows.sort(key=lambda x: x["w"])
    return windows


# ─── Run the analysis ──────────────────────────────────────────────────
def main():
    print(f"Loading windows from {PREDICTIONS_PATH.name}…")
    wins = load_windows()
    print(f"  {len(wins)} clean windows  ({sum(1 for w in wins if w['yw'])} UP / {sum(1 for w in wins if not w['yw'])} DOWN)")
    print()

    # Step 1: Precompute n-5 thresholds for each window using walk-forward.
    print("Precomputing n-5 thresholds for each window…")
    th_by_idx = {}
    for i in range(5, len(wins)):
        prior5 = wins[i-5:i]
        th = find_optimal_thresholds([(w["pts"], w["yw"]) for w in prior5])
        if th:
            th_by_idx[i] = (th["k_yes"], th["k_no"], th["d_yes"], th["d_no"])
    print(f"  thresholds available for {len(th_by_idx)} windows (idx 5..{len(wins)-1})")
    print()

    # Step 2: Walk-forward execution tuning. At window i, grid-search exec params
    # over the prior 5 windows (using THEIR thresholds), pick best, apply to i.
    print("Step 2: walk-forward n-5 execution tuning…")
    wf_rows = []
    for i in range(10, len(wins)):    # need 5 thresholds + 5 prior eval windows
        best_exec = None
        best_pnl_sum = float("-inf")
        for off in OFFSET_CENTS_GRID:
            for ttl in TTL_SEC_GRID:
                total = 0.0
                for j in range(i-5, i):
                    if j not in th_by_idx: continue
                    th = th_by_idx[j]
                    r = simulate_window_maker_first(
                        wins[j]["pts"], wins[j]["yw"], *th,
                        offset_cents=off, ttl_sec=ttl,
                    )
                    total += r["pnl"]
                if total > best_pnl_sum:
                    best_pnl_sum = total
                    best_exec = (off, ttl)
        # Apply chosen exec to current window
        th = th_by_idx.get(i)
        if th is None: continue
        r = simulate_window_maker_first(
            wins[i]["pts"], wins[i]["yw"], *th,
            offset_cents=best_exec[0], ttl_sec=best_exec[1],
        )
        wf_rows.append({"i": i, "win": wins[i], "th": th, "exec": best_exec, **r})

    # Step 3: In-sample best single (offset, ttl) over the full history.
    print("Step 3: in-sample one-shot search over full history…")
    in_sample_grid = {}
    in_sample_breakdown = {}
    for off in OFFSET_CENTS_GRID:
        for ttl in TTL_SEC_GRID:
            total_pnl = 0.0
            total_trades = 0
            total_maker = 0
            total_market = 0
            total_savings = 0.0
            for i in range(5, len(wins)):
                th = th_by_idx.get(i)
                if th is None: continue
                r = simulate_window_maker_first(
                    wins[i]["pts"], wins[i]["yw"], *th,
                    offset_cents=off, ttl_sec=ttl,
                )
                total_pnl += r["pnl"]
                total_trades += r["n_trades"]
                total_maker += r["n_maker"]
                total_market += r["n_market"]
                total_savings += r["savings_vs_taker"]
            in_sample_grid[(off, ttl)] = total_pnl
            in_sample_breakdown[(off, ttl)] = {
                "pnl": total_pnl, "n_trades": total_trades,
                "n_maker": total_maker, "n_market": total_market,
                "savings_vs_taker": total_savings,
            }

    # ── Reports ───────────────────────────────────────────────────────
    print()
    print("="*78)
    print("RESULTS")
    print("="*78)

    # In-sample grid heatmap
    print()
    print(f"In-sample total P&L by (offset, TTL) across {len(th_by_idx)} windows:")
    print(f"             " + "  ".join(f"{ttl:>5}s" for ttl in TTL_SEC_GRID))
    for off in OFFSET_CENTS_GRID:
        cells = []
        for ttl in TTL_SEC_GRID:
            cells.append(f"{in_sample_grid[(off, ttl)]:>+6.1f}")
        print(f"  off {off}¢      " + "  ".join(cells))
    print()
    best_in = max(in_sample_grid, key=in_sample_grid.get)
    bb = in_sample_breakdown[best_in]
    print(f"In-sample BEST: offset={best_in[0]}¢, TTL={best_in[1]}s")
    print(f"  total P&L:   ${bb['pnl']:+.2f}")
    print(f"  n_trades:    {bb['n_trades']}  ({bb['n_maker']} maker / {bb['n_market']} market)")
    print(f"  fill ratio:  {bb['n_maker']/max(bb['n_trades'],1)*100:.0f}% maker")
    print(f"  savings vs taker baseline: ${bb['savings_vs_taker']:+.2f}")

    # Walk-forward per-window table (last 40 for readability)
    print()
    print("Walk-forward n-5 execution tuning (most recent 40 windows shown):")
    print(f"{'#':>3} {'window ET':<14} {'out':<3} {'thresholds':<22} {'(off,TTL)':<8} {'n_trades':>9} {'maker/mkt':>11} {'pnl':>8} {'cum':>9}")
    print("-"*110)
    cum = 0.0
    cum_in_sample = 0.0
    # Also compute cum for in-sample combo applied to each window
    cum_per_window = []
    for row in wf_rows:
        i = row["i"]; win = row["win"]; th = row["th"]; ex = row["exec"]
        cum += row["pnl"]
        # in-sample combo applied to same window
        ins_r = simulate_window_maker_first(
            win["pts"], win["yw"], *th,
            offset_cents=best_in[0], ttl_sec=best_in[1],
        )
        cum_in_sample += ins_r["pnl"]
        cum_per_window.append((row, ins_r, cum, cum_in_sample))
    for row, ins_r, c, c_is in cum_per_window[-40:]:
        win = row["win"]; th = row["th"]; ex = row["exec"]
        th_s = f"K{th[0]:.2f}/{th[1]:.2f} Δ{int(th[2]*100)}/{int(th[3]*100)}pp"
        out  = "UP" if win["yw"] else "DN"
        print(f"{row['i']:>3} {win['et']:<14} {out:<3} {th_s:<22} "
              f"({ex[0]:>1},{ex[1]:>2}s) {row['n_trades']:>9} "
              f"{row['n_maker']:>5}/{row['n_market']:<5} "
              f"{row['pnl']:>+8.3f} {c:>+9.2f}")

    # Totals
    wf_total_pnl = sum(r["pnl"] for r in wf_rows)
    wf_total_trades = sum(r["n_trades"] for r in wf_rows)
    wf_total_maker = sum(r["n_maker"] for r in wf_rows)
    wf_total_market = sum(r["n_market"] for r in wf_rows)
    wf_total_savings = sum(r["savings_vs_taker"] for r in wf_rows)
    print()
    print(f"Walk-forward TOTALS across {len(wf_rows)} windows:")
    print(f"  total P&L:   ${wf_total_pnl:+.2f}")
    print(f"  n_trades:    {wf_total_trades}  ({wf_total_maker} maker / {wf_total_market} market)")
    print(f"  fill ratio:  {wf_total_maker/max(wf_total_trades,1)*100:.0f}% maker")
    print(f"  savings vs taker baseline: ${wf_total_savings:+.2f}")
    print()

    # In-sample best applied to same set of windows (apples-to-apples)
    print(f"In-sample BEST ({best_in[0]}¢, {best_in[1]}s) applied to SAME {len(wf_rows)} windows:")
    ins_only_pnl = 0
    ins_only_trades = ins_only_maker = ins_only_market = 0
    for row in wf_rows:
        ins_r = simulate_window_maker_first(
            row["win"]["pts"], row["win"]["yw"], *row["th"],
            offset_cents=best_in[0], ttl_sec=best_in[1],
        )
        ins_only_pnl += ins_r["pnl"]
        ins_only_trades += ins_r["n_trades"]
        ins_only_maker += ins_r["n_maker"]
        ins_only_market += ins_r["n_market"]
    print(f"  total P&L:   ${ins_only_pnl:+.2f}")
    print(f"  n_trades:    {ins_only_trades}  ({ins_only_maker} maker / {ins_only_market} market)")
    print()

    print("="*78)
    print("VERDICT")
    print("="*78)
    delta = wf_total_pnl - ins_only_pnl
    print(f"Walk-forward n-5:    ${wf_total_pnl:+.2f}")
    print(f"In-sample one-shot:  ${ins_only_pnl:+.2f}    ({best_in[0]}¢, {best_in[1]}s)")
    print(f"Delta (WF − IS):     ${delta:+.2f}  →  "
          f"{'WALK-FORWARD wins' if delta > 0 else 'IN-SAMPLE wins'}")

    # Distribution of chosen (offset, ttl) by walk-forward
    print()
    print("Walk-forward CHOICES distribution (how often each (offset, TTL) was picked):")
    choice_count = defaultdict(int)
    for r in wf_rows: choice_count[r["exec"]] += 1
    for combo, n in sorted(choice_count.items(), key=lambda x: -x[1]):
        bar = "█" * int(n / len(wf_rows) * 50)
        print(f"  ({combo[0]}¢, {combo[1]:>2}s): {n:>3}  {bar}")


if __name__ == "__main__":
    main()
