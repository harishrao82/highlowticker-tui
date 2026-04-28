"""coinbase_vs_twin.py — compare our Coinbase-based signals against Twin's trades.

For each (coin, 5m_window) where both we and Twin produced a signal:
  - Did we get the same side?
  - How many seconds before/after Twin did we fire?
  - Did our signal predict the 5m outcome correctly?

This tells us whether our upstream-signal detector is a viable replacement
for tailing Twin (faster + own logic, no Polymarket lag).
"""
import json
import time
import bisect
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
SIG_LOG = Path.home() / ".coinbase_signals.jsonl"
TWIN_LOG = Path.home() / ".twin_live_trades.jsonl"
CFB_LOG = Path.home() / ".cfb_proxy_log.jsonl"


def load_signals():
    sigs = {}
    if not SIG_LOG.exists():
        return sigs
    with open(SIG_LOG) as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            key = (r["coin"], r["window_start_ts"])
            sigs[key] = r
    return sigs


def load_twin_5m_first():
    """Twin's first BUY trade per 5m window (any conviction)."""
    by_w = defaultdict(list)
    with open(TWIN_LOG) as f:
        for line in f:
            r = json.loads(line)
            if "5m" not in r.get("slug", ""):
                continue
            if r.get("side") != "BUY":
                continue
            if r.get("outcome") not in ("Up", "Down"):
                continue
            wst = r.get("window_start_ts")
            if not wst:
                continue
            by_w[(r["coin"], int(wst))].append(r)
    out = {}
    for k, ts in by_w.items():
        ts.sort(key=lambda t: t["trade_ts"])
        out[k] = ts[0]
    return out


def load_5m_winners():
    """Use cfb_proxy log to determine 5m settlement winners."""
    prices = defaultdict(list)
    with open(CFB_LOG) as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            mid = r.get("mid") or r.get("cfb")
            if not mid:
                continue
            prices[r["coin"]].append((float(r["ts"]), float(mid)))
    for c in prices:
        prices[c].sort()
    return prices


def price_at(prices, coin, ts):
    series = prices.get(coin, [])
    if not series:
        return None
    ts_list = [t for t, _ in series]
    i = bisect.bisect_left(ts_list, ts)
    cands = []
    if i < len(ts_list):
        cands.append(series[i])
    if i > 0:
        cands.append(series[i - 1])
    if not cands:
        return None
    best = min(cands, key=lambda x: abs(x[0] - ts))
    if abs(best[0] - ts) > 60:
        return None
    return best[1]


def main():
    sigs = load_signals()
    twin = load_twin_5m_first()
    prices = load_5m_winners()
    print(f"Coinbase signals:  {len(sigs)}")
    print(f"Twin 5m firsts:    {len(twin)}")
    print()

    # Joint windows
    joint = sorted(set(sigs.keys()) & set(twin.keys()), key=lambda k: k[1])
    print(f"Joint (coin, 5m window) where both fired: {len(joint)}")
    print()

    if not joint:
        print("No overlap yet — let the detector run for at least one 5m window.")
        return

    # Per-window comparison
    print(f"{'time_ET':>14} {'coin':>4} {'cb_side':>7} {'cb_T':>5} "
          f"{'tw_side':>7} {'tw_T':>5} {'tw_$':>5} {'lead':>6} "
          f"{'agree':>5} {'winner':>6} {'cb_W':>4} {'tw_W':>4}")
    print("-" * 90)

    cb_wins = 0; cb_n = 0
    tw_wins = 0; tw_n = 0
    agreed = 0; lead_first = 0
    leads = []

    for k in joint:
        coin, wst = k
        s = sigs[k]
        t = twin[k]

        # 5m winner
        po = price_at(prices, coin, wst)
        pc = price_at(prices, coin, wst + 300)
        winner = None
        if po and pc:
            winner = "Up" if pc > po else "Down" if pc < po else None

        cb_side = s["side"]
        tw_side = t["outcome"]
        agree = cb_side == tw_side
        if agree: agreed += 1

        # lead = how many seconds we fired BEFORE twin (positive = we were first)
        lead = t["trade_ts"] - s["ts"]
        leads.append(lead)
        if lead > 0: lead_first += 1

        cb_won = (winner == cb_side) if winner else None
        tw_won = (winner == tw_side) if winner else None
        if cb_won is not None:
            cb_n += 1; tw_n += 1
            if cb_won: cb_wins += 1
            if tw_won: tw_wins += 1

        dt = datetime.fromtimestamp(wst, tz=ET).strftime("%m/%d %I:%M%p")
        cb_w = "Y" if cb_won else "N" if cb_won is False else "-"
        tw_w = "Y" if tw_won else "N" if tw_won is False else "-"
        ag = "Y" if agree else "N"
        win_short = ("Up" if winner == "Up" else "Dn") if winner else "?"

        print(f"{dt:>14} {coin:>4} "
              f"{cb_side[0]:>7} {s['elapsed']:>5.0f} "
              f"{tw_side[0]:>7} {t.get('elapsed_in_window', 0):>5.0f} "
              f"{t['price']:>5.2f} {lead:>+5.1f}s "
              f"{ag:>5} {win_short:>6} {cb_w:>4} {tw_w:>4}")

    # Summary
    print()
    print("=" * 60)
    print(f"Joint windows:    {len(joint)}")
    print(f"Same side:        {agreed}/{len(joint)} = {agreed/len(joint)*100:.1f}%")
    print(f"Coinbase first:   {lead_first}/{len(joint)} = {lead_first/len(joint)*100:.1f}%")
    if leads:
        median_lead = sorted(leads)[len(leads) // 2]
        avg_lead = sum(leads) / len(leads)
        print(f"Lead time (Twin_ts - CB_ts):  median {median_lead:+.1f}s  avg {avg_lead:+.1f}s")
        print(f"  positive = we fired BEFORE Twin")
    if cb_n:
        print(f"\nCoinbase signal WR (5m): {cb_wins}/{cb_n} = {cb_wins/cb_n*100:.1f}%")
        print(f"Twin signal WR (5m):     {tw_wins}/{tw_n} = {tw_wins/tw_n*100:.1f}%")


if __name__ == "__main__":
    main()
