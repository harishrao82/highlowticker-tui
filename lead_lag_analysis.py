"""Lead-lag analysis: which price stream does Kalshi odds follow?

Compares Coinbase mid vs CFB-proxy (60s rolling blend) against Kalshi yes-mid,
per coin, over a range of lags. Reports peak correlation and the lag at which
it occurs. A positive lag means price LEADS odds by that many seconds.
"""
import json
import sys
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

SNAP_LOG = Path.home() / ".kalshi_status_snapshots.jsonl"
CFB_LOG  = Path.home() / ".cfb_proxy_log.jsonl"

# Limit window so script finishes quickly — last N hours
HOURS = float(sys.argv[1]) if len(sys.argv) > 1 else 24.0
LAG_RANGE = range(-10, 11)   # seconds; positive = price leads odds

import time
cutoff = time.time() - HOURS * 3600


def load_snapshots():
    rows = []
    with open(SNAP_LOG) as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            if r.get('ts', 0) < cutoff:
                continue
            if r.get('yes_ask') is None or r.get('no_ask') is None:
                continue
            if r.get('coinbase_px') is None or r.get('open_price') is None:
                continue
            rows.append(r)
    df = pd.DataFrame(rows)
    # Derived: yes-mid implied prob. yes_bid = 1 - no_ask.
    df['yes_mid'] = (df['yes_ask'] + (1.0 - df['no_ask'])) / 2.0
    df['ts_int'] = df['ts'].round().astype(int)
    return df


def load_cfb():
    rows = []
    with open(CFB_LOG) as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            if r.get('ts', 0) < cutoff:
                continue
            if r.get('cfb') is None:
                continue
            rows.append(r)
    df = pd.DataFrame(rows)
    df['ts_int'] = df['ts'].round().astype(int)
    # Dedup to one row per (coin, ts_int)
    df = df.groupby(['coin', 'ts_int'], as_index=False).last()
    return df[['coin', 'ts_int', 'cfb']]


def compute_lead_lag(df_coin, signal_col, lags):
    """Return {lag: pearson_r} for signal_col vs yes_mid, within each window."""
    # Work window-by-window so deltas reset at each open
    out = defaultdict(list)
    for ticker, grp in df_coin.groupby('ticker'):
        if len(grp) < 60:
            continue
        grp = grp.sort_values('ts_int').drop_duplicates('ts_int')
        # Use first row of window as the "open" for whichever signal we're testing
        open_val = grp[signal_col].iloc[0]
        open_odds = grp['yes_mid'].iloc[0]
        if not open_val or open_val <= 0:
            continue
        price_delta = (grp[signal_col].values - open_val) / open_val
        odds_delta = grp['yes_mid'].values - open_odds
        for lag in lags:
            if lag >= 0:
                # price at t-lag vs odds at t  →  price leads odds by `lag`
                x = price_delta[:len(price_delta)-lag] if lag > 0 else price_delta
                y = odds_delta[lag:]
            else:
                x = price_delta[-lag:]
                y = odds_delta[:len(odds_delta)+lag]
            if len(x) < 30:
                continue
            if np.std(x) == 0 or np.std(y) == 0:
                continue
            r = np.corrcoef(x, y)[0, 1]
            if not np.isnan(r):
                out[lag].append(r)
    return {lag: np.mean(rs) for lag, rs in out.items() if rs}


def main():
    print(f"Loading snapshots (last {HOURS}h)...", flush=True)
    snap = load_snapshots()
    print(f"  {len(snap):,} snapshot rows across {snap['coin'].nunique()} coins, "
          f"{snap['ticker'].nunique()} windows", flush=True)

    print("Loading cfb proxy log...", flush=True)
    cfb = load_cfb()
    print(f"  {len(cfb):,} cfb rows", flush=True)

    # Merge cfb onto snapshots by (coin, ts_int). Forward-fill small gaps.
    print("Joining...", flush=True)
    merged = snap.merge(cfb, on=['coin', 'ts_int'], how='left')
    merged = merged.sort_values(['ticker', 'ts_int'])
    merged['cfb'] = merged.groupby('ticker')['cfb'].ffill(limit=5)

    print(f"  merged rows: {len(merged):,}  "
          f"cfb coverage: {merged['cfb'].notna().mean():.1%}")
    print()

    print(f"{'coin':<6} {'signal':<10} {'peak_lag(s)':>12} {'peak_r':>8} "
          f"{'r@lag0':>8} {'n_windows':>10}")
    print("-" * 60)

    for coin in sorted(merged['coin'].dropna().unique()):
        sub = merged[(merged['coin'] == coin) & merged['cfb'].notna()]
        if len(sub) < 200:
            continue
        n_windows = sub['ticker'].nunique()

        cb_r = compute_lead_lag(sub, 'coinbase_px', LAG_RANGE)
        cfb_r = compute_lead_lag(sub, 'cfb', LAG_RANGE)

        for label, result in [('coinbase', cb_r), ('cfb_proxy', cfb_r)]:
            if not result:
                continue
            peak_lag = max(result, key=result.get)
            peak_r = result[peak_lag]
            r0 = result.get(0, float('nan'))
            print(f"{coin:<6} {label:<10} {peak_lag:>12} {peak_r:>8.4f} "
                  f"{r0:>8.4f} {n_windows:>10}")
        print()

    # Per-lag detail for BTC as reference
    print("\nFull lag curve (BTC):")
    sub = merged[(merged['coin'] == 'BTC') & merged['cfb'].notna()]
    cb_r = compute_lead_lag(sub, 'coinbase_px', LAG_RANGE)
    cfb_r = compute_lead_lag(sub, 'cfb', LAG_RANGE)
    print(f"{'lag':>5} {'coinbase_r':>12} {'cfb_r':>10} {'diff(cb-cfb)':>14}")
    for lag in LAG_RANGE:
        a = cb_r.get(lag, float('nan'))
        b = cfb_r.get(lag, float('nan'))
        print(f"{lag:>5} {a:>12.4f} {b:>10.4f} {(a-b):>14.4f}")


if __name__ == '__main__':
    main()
