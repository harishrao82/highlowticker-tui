"""Analyze whether mid-window hedging / loss-cutting adds value.

For each window at various elapsed checkpoints, record:
  - yes_mid (implied probability of YES winning)
  - actual winner
Then compute calibration: does yes_mid = actual win rate?

If yes_mid at T=300 says 0.30 for YES, but YES actually wins 40% of the
time from that state, then the market is UNDERPRICING YES at T=300 and
you should NOT sell/hedge — you should hold (or even add).

Conversely, if yes_mid says 0.70 but YES only wins 60%, the market is
OVERPRICING YES — you should sell or hedge.

The delta (actual_win_rate - implied_prob) at each (T, price_bucket) tells
you exactly when hedging or adding has edge.
"""
import sqlite3
from collections import defaultdict
from pathlib import Path

DB = Path.home() / ".btc_windows.db"

CHECKPOINTS = [30, 60, 120, 180, 300, 420, 540, 600, 720, 840]
PRICE_BUCKETS = [
    (0.00, 0.15), (0.15, 0.25), (0.25, 0.35), (0.35, 0.45),
    (0.45, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 0.85),
    (0.85, 1.01),
]

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

# Load all resolved windows
windows = con.execute(
    "SELECT id, ticker, winner FROM windows "
    "WHERE winner IN ('yes','no')").fetchall()
print(f"Resolved windows: {len(windows):,}")

def coin_from_ticker(tk):
    # KXBTC15M-... -> BTC,  KXETH15M-... -> ETH
    return tk.split('15M')[0].replace('KX','') if '15M' in tk else 'BTC'

data = defaultdict(list)

for w in windows:
    wid = w['id']; coin = coin_from_ticker(w['ticker']); winner = w['winner']
    yes_won = 1 if winner == 'yes' else 0
    for cp in CHECKPOINTS:
        row = con.execute(
            "SELECT yes_ask, no_ask FROM ticks "
            "WHERE window_id=? AND elapsed_sec=?", (wid, cp)).fetchone()
        if not row or row['yes_ask'] is None or row['no_ask'] is None:
            continue
        yes_mid = (row['yes_ask'] + (1.0 - row['no_ask'])) / 2.0
        for lo, hi in PRICE_BUCKETS:
            if lo <= yes_mid < hi:
                data[(coin, cp, (lo, hi))].append(yes_won)
                data[('ALL', cp, (lo, hi))].append(yes_won)
                break

con.close()

# Print calibration table
def bkt_label(lo, hi):
    return f"{lo:.2f}-{hi:.2f}"

print("\n" + "=" * 90)
print("CALIBRATION: implied_prob vs actual_win_rate at each (T, price_bucket)")
print("  + means market UNDERPRICES yes → hold/add, don't hedge")
print("  - means market OVERPRICES yes  → sell/hedge has edge")
print("=" * 90)

for coin_filter in ['ALL', 'BTC', 'ETH', 'SOL', 'XRP']:
    print(f"\n--- {coin_filter} ---")
    header = f"{'T':>5}  " + "  ".join(f"{bkt_label(lo,hi):>11}" for lo,hi in PRICE_BUCKETS)
    print(header)
    print("       " + "  ".join(f"{'impl→act(n)':>11}" for _ in PRICE_BUCKETS))
    print("-" * (6 + 13 * len(PRICE_BUCKETS)))
    for cp in CHECKPOINTS:
        cells = []
        for bkt in PRICE_BUCKETS:
            key = (coin_filter, cp, bkt)
            outcomes = data.get(key, [])
            if len(outcomes) < 5:
                cells.append(f"{'—':>11}")
                continue
            actual_wr = sum(outcomes) / len(outcomes)
            implied = (bkt[0] + bkt[1]) / 2
            delta = actual_wr - implied
            cells.append(f"{delta:>+5.1f}%({len(outcomes):>3})")
        print(f"T={cp:>3}  " + "  ".join(cells))

# Summary: which (T, bucket) combos have significant edge?
print("\n" + "=" * 90)
print("ACTIONABLE: cells with |delta| >= 5% and n >= 20")
print("=" * 90)
print(f"{'coin':<5} {'T':>5} {'bucket':<12} {'implied':>8} {'actual':>8} "
      f"{'delta':>8} {'n':>5} {'action':<20}")
print("-" * 80)
for (coin, cp, bkt), outcomes in sorted(data.items()):
    if len(outcomes) < 20: continue
    actual = sum(outcomes) / len(outcomes)
    implied = (bkt[0] + bkt[1]) / 2
    delta = actual - implied
    if abs(delta) < 0.05: continue
    if delta > 0:
        action = "HOLD/ADD (underpriced)"
    else:
        action = "SELL/HEDGE (overpriced)"
    print(f"{coin:<5} T={cp:>3} {bkt_label(bkt[0],bkt[1]):<12} "
          f"{implied:>7.1f}% {actual*100:>7.1f}% {delta*100:>+7.1f}% "
          f"{len(outcomes):>5} {action}")
