"""Trigger-level analysis of Scallops trades.

For every trade in a window we have book data for, extract:
  - Fill price vs own-side bid/ask (anchoring detection)
  - Own-side and opposite-side book movement over prior 5/15/30/60s
  - Coin price movement over same lookbacks
  - Spread and elapsed context

Then surface the patterns:
  1. Where do fills cluster relative to the book?  (bid-N, mid-N, ask-N ?)
  2. What is the book doing in the moments before a trade?  (dip vs chase)
  3. Do Up-buys and Down-buys show different trigger signatures?
  4. Do the patterns change with elapsed-in-window?
"""
import json
import bisect
from collections import defaultdict, Counter
from pathlib import Path
from statistics import median, mean

from poly_simulator import Simulator

TRADE_LOG = Path.home() / ".scallops_live_trades.jsonl"

sim = Simulator(); sim.load_book_log()
print(f"Books loaded: {len(sim.books):,}")

# Build per-window snap arrays for fast backward-lookups
def snap_series(book):
    """Return (ts_list, ask_list, bid_list)."""
    ts  = [s.ts for s in book._snaps]
    ask = [s.ask for s in book._snaps]
    # We don't have bids; use best_bid from snap directly (stored in poly log)
    # Fall back to 1 - opposite ask if needed (done at use site).
    # poly_simulator's Snap currently has .bid (we populated it).
    bid = [s.bid for s in book._snaps]
    return ts, ask, bid


# Load trades into list with (coin, wst, outcome) grouping
trades = []
with open(TRADE_LOG) as f:
    for line in f:
        r = json.loads(line)
        if r.get('outcome') not in ('Up','Down'): continue
        if r.get('coin') not in ('BTC','ETH','SOL','XRP'): continue
        if r.get('window_start_ts') is None or r.get('trade_ts') is None: continue
        key_self  = (r['coin'], int(r['window_start_ts']), r['outcome'])
        key_other = (r['coin'], int(r['window_start_ts']),
                     'Down' if r['outcome'] == 'Up' else 'Up')
        if key_self not in sim.books or key_other not in sim.books: continue
        trades.append(r)

print(f"Trades with book data for BOTH sides: {len(trades):,}\n")


def lookup(book, ts):
    s = book.at(ts)
    if not s: return (None, None)
    return (s.bid, s.ask)


def ask_at(book, ts):
    s = book.at(ts)
    return s.ask if s else None


# Build feature rows
rows = []
for r in trades:
    wst = int(r['window_start_ts'])
    coin = r['coin']
    side = r['outcome']          # 'Up' | 'Down'
    ts = r['trade_ts']
    price = float(r['price'])
    book_self  = sim.books[(coin, wst, side)]
    book_other = sim.books[(coin, wst, 'Down' if side == 'Up' else 'Up')]
    s_bid, s_ask = lookup(book_self,  ts)
    o_bid, o_ask = lookup(book_other, ts)
    if s_bid is None or s_ask is None or o_bid is None or o_ask is None:
        continue
    # Book lookbacks on SELF side
    def da(book, dt):
        now = ask_at(book, ts); past = ask_at(book, ts - dt)
        if now is None or past is None: return None
        return now - past
    feat = dict(
        coin=coin, side=side, elapsed=r.get('elapsed_in_window') or (ts-wst),
        price=price,
        s_bid=s_bid, s_ask=s_ask, o_bid=o_bid, o_ask=o_ask,
        # Fill price relative to various anchors
        rel_self_bid  = round(price - s_bid, 4),
        rel_self_ask  = round(price - s_ask, 4),
        rel_self_mid  = round(price - (s_bid+s_ask)/2, 4),
        rel_other_ask = round(price - o_ask, 4),
        rel_other_bid = round(price - o_bid, 4),
        # Book movement lookbacks (self side ask change)
        d_self_5   = da(book_self, 5),
        d_self_15  = da(book_self, 15),
        d_self_30  = da(book_self, 30),
        d_other_5  = da(book_other, 5),
        d_other_15 = da(book_other, 15),
        d_other_30 = da(book_other, 30),
        spread     = round(s_ask - s_bid, 4),
    )
    rows.append(feat)

print(f"Rows with full feature vector: {len(rows):,}\n")


# ---------- 1. Fill-price anchoring: where do fills cluster relative to book? ----------
def hist1d(xs, bins, label, unit='¢'):
    buckets = Counter()
    tot = 0
    for x in xs:
        if x is None: continue
        tot += 1
        for lo, hi in bins:
            if lo <= x < hi:
                buckets[(lo,hi)] += 1; break
        else:
            buckets[('out','out')] += 1
    print(f"\n  {label}  (n={tot})")
    for (lo,hi) in bins:
        n = buckets[(lo,hi)]
        bar = '█' * int(n/max(buckets.values())*30) if buckets else ''
        rng = f"[{lo:+.2f}, {hi:+.2f})"
        print(f"    {rng:<18}  {n:>5} {n/tot*100:>5.1f}%  {bar}")
    oo = buckets.get(('out','out'), 0)
    if oo: print(f"    out-of-range        {oo:>5} {oo/tot*100:>5.1f}%")

cent_bins = [(-0.15+i*0.02, -0.15+(i+1)*0.02) for i in range(15)]
print("=" * 70)
print("1. FILL-PRICE ANCHORING — where do fills sit relative to the book?")
print("=" * 70)
hist1d([r['rel_self_bid']  for r in rows], cent_bins, "fill - self_bid")
hist1d([r['rel_self_ask']  for r in rows], cent_bins, "fill - self_ask")
hist1d([r['rel_other_ask'] for r in rows], cent_bins, "fill - other_ask")


# ---------- 2. Book movement in the 5/15/30s before a trade ----------
print("\n" + "=" * 70)
print("2. BOOK MOMENTUM BEFORE TRADE — did self/other side just move?")
print("=" * 70)
def summarize(key, label):
    xs = [r[key] for r in rows if r[key] is not None]
    if not xs: return
    pos  = sum(1 for x in xs if x > 0.005)
    neg  = sum(1 for x in xs if x < -0.005)
    flat = len(xs) - pos - neg
    print(f"  {label:<30} pos:{pos/len(xs)*100:>5.1f}% "
          f"flat:{flat/len(xs)*100:>5.1f}% neg:{neg/len(xs)*100:>5.1f}% "
          f"median={median(xs):+.4f}  mean={mean(xs):+.4f}")

for w in (5, 15, 30):
    summarize(f'd_self_{w}',  f"Δ self_ask over {w}s")
    summarize(f'd_other_{w}', f"Δ other_ask over {w}s")
    print()


# ---------- 3. Same, split by side ----------
print("=" * 70)
print("3. BY SIDE — does Up vs Down firing show different triggers?")
print("=" * 70)
for side in ('Up','Down'):
    sub = [r for r in rows if r['side']==side]
    print(f"\n  SIDE={side}  (n={len(sub):,})")
    for w in (5, 15):
        xs = [r[f'd_self_{w}']  for r in sub if r[f'd_self_{w}']  is not None]
        ys = [r[f'd_other_{w}'] for r in sub if r[f'd_other_{w}'] is not None]
        if xs:
            print(f"    Δself_ask  over {w}s: median={median(xs):+.4f}  "
                  f"pos:{sum(1 for x in xs if x>0.005)/len(xs)*100:.0f}%  "
                  f"neg:{sum(1 for x in xs if x<-0.005)/len(xs)*100:.0f}%")
        if ys:
            print(f"    Δother_ask over {w}s: median={median(ys):+.4f}  "
                  f"pos:{sum(1 for x in ys if x>0.005)/len(ys)*100:.0f}%  "
                  f"neg:{sum(1 for x in ys if x<-0.005)/len(ys)*100:.0f}%")


# ---------- 4. Trade "types" via simple clustering of feature vector ----------
print("\n" + "=" * 70)
print("4. TRADE ARCHETYPES — classify each trade on book signature")
print("=" * 70)
archetypes = Counter()
for r in rows:
    d_self = r['d_self_15']; d_other = r['d_other_15']
    rel_bid = r['rel_self_bid']; spread = r['spread']
    # Classification: several non-overlapping buckets
    if rel_bid is not None and rel_bid < -0.005:
        tag = 'BELOW_BID — resting limit caught a dip'
    elif rel_bid is not None and abs(rel_bid) <= 0.005:
        tag = 'AT_BID     — maker fill at top-of-book'
    elif (d_self is not None and d_self <= -0.02):
        tag = 'SELF_DIP   — bought self after own ask dropped ≥2¢/15s'
    elif (d_other is not None and d_other >= 0.02):
        tag = 'OTHER_UP   — bought self after other ask rose ≥2¢/15s'
    elif (d_self is not None and d_self >= 0.02):
        tag = 'SELF_CHASE — bought self after own ask rose ≥2¢/15s'
    elif spread >= 0.05:
        tag = 'WIDE_SPRD  — bought into a wide spread'
    else:
        tag = 'OTHER'
    archetypes[tag] += 1

tot = sum(archetypes.values())
for tag, n in archetypes.most_common():
    bar = '█' * int(n/tot*40)
    print(f"  {tag:<50} {n:>5} {n/tot*100:>5.1f}%  {bar}")


# ---------- 5. Archetype by elapsed bucket — do early/late trades differ? ----------
print("\n" + "=" * 70)
print("5. ARCHETYPE × ELAPSED BUCKET")
print("=" * 70)
def ebkt(e):
    if e < 60: return '0-60s'
    if e < 180: return '60-180s'
    if e < 300: return '180-300s'
    if e < 600: return '300-600s'
    if e < 750: return '600-750s'
    return '750-900s'

mat = defaultdict(Counter)
for r in rows:
    d_self = r['d_self_15']; d_other = r['d_other_15']
    rel_bid = r['rel_self_bid']; spread = r['spread']
    if rel_bid is not None and rel_bid < -0.005: tag = 'BELOW_BID'
    elif rel_bid is not None and abs(rel_bid) <= 0.005: tag = 'AT_BID'
    elif d_self is not None and d_self <= -0.02: tag = 'SELF_DIP'
    elif d_other is not None and d_other >= 0.02: tag = 'OTHER_UP'
    elif d_self is not None and d_self >= 0.02: tag = 'SELF_CHASE'
    elif spread >= 0.05: tag = 'WIDE_SPRD'
    else: tag = 'OTHER'
    mat[ebkt(r['elapsed'])][tag] += 1

tags = ['BELOW_BID','AT_BID','SELF_DIP','OTHER_UP','SELF_CHASE','WIDE_SPRD','OTHER']
hdr = " ".join(f"{t:>10}" for t in tags)
print(f"  {'bucket':<12}{hdr}")
for b in ['0-60s','60-180s','180-300s','300-600s','600-750s','750-900s']:
    row = mat[b]; tot = sum(row.values())
    if not tot: continue
    cells = " ".join(f"{row[t]/tot*100:>9.1f}%" for t in tags)
    print(f"  {b:<12}{cells}   n={tot}")
