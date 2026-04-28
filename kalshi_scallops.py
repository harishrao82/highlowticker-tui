"""Kalshi port of the Scallops-style strategy (v4 rules).

Three components, same as poly clone:
  1. PRIMARY LADDER — dense grid at T=0 on both yes and no sides, clipped
     to strictly below the current side-ask at window open.
  2. ACTIVE QUOTE — tracks top-of-book bid; re-quotes when bid moves by
     ≥QUOTE_REPRICE_THR with minimum QUOTE_MIN_DWELL dwell.
  3. MOMENTUM ADD — every MOMENTUM_SCAN_INTERVAL, if own side's ask rose
     ≥MOMENTUM_THRESHOLD in MOMENTUM_LOOKBACK_SEC, fire on that side at
     current ask.  Respects MOMENTUM_COOLDOWN_SEC per side.

Kalshi semantics:
  - Each 15m market is a single ticker (e.g. KXBTC15M-...).
  - `yes` side = coin closes higher (= Polymarket 'Up').
  - `no`  side = coin closes lower  (= Polymarket 'Down').
  - Bid on yes ≈ 1 - no_ask;  bid on no ≈ 1 - yes_ask.
  - Kalshi tick size = 1¢, same as Polymarket.
"""
from dataclasses import dataclass
from kalshi_sim import KalshiSim, Window, Snap

# --- rule constants (same as poly v4) ---
def _grid(step):
    out = []; p = 0.10
    while p <= 0.86 + 1e-9:
        out.append(round(p, 2)); p += step
    return out

LADDER_PER_COIN = {
    'BTC': _grid(0.02),   # 39 rungs
    'ETH': _grid(0.04),   # 20 rungs
    'SOL': _grid(0.05),   # 16 rungs
    'XRP': _grid(0.06),   # 13 rungs
}

def _ladder_size(price: float, coin: str = '') -> int:
    """More shares on cheap rungs, fewer on expensive.
    BTC gets tighter ceiling (most efficient market)."""
    if coin == 'BTC':
        return 0   # BTC: no ladder (market too efficient), quote+momentum only
    # ETH/SOL/XRP
    if price <= 0.30: return 3
    if price <= 0.45: return 2
    if price <= 0.55: return 1
    return 0

QUOTE_SIZE    = 1
MOMENTUM_SIZE = 2

QUOTE_REPRICE_THR = 0.03
QUOTE_MIN_DWELL   = 30

MOMENTUM_SCAN_INTERVAL = 30
MOMENTUM_LOOKBACK_SEC  = 15
MOMENTUM_THRESHOLD     = 0.02
MOMENTUM_COOLDOWN_SEC  = 120

# --- Late-window calibration rules ---
# From hedge_timing_analysis.py over 2,940 windows:
#   - Leading side (0.55-0.75) wins 6-9% MORE than implied → add
#   - Losing side (<0.25 after T=600) wins 5-7% LESS than implied → dump
LATE_REBALANCE_T    = 600    # elapsed to evaluate
LATE_ADD_LO         = 0.55   # add if yes_mid in [LO, HI]
LATE_ADD_HI         = 0.75
LATE_ADD_SIZE       = 3      # shares to add on the leading side
LATE_DUMP_CEILING   = 0.25   # if either side mid < this, buy opposite as hedge
LATE_DUMP_SIZE      = 2      # shares for the hedge buy


@dataclass
class KPlanOrder:
    elapsed: int
    side: str              # 'yes' | 'no'
    price: float
    shares: int
    kind: str              # 'ladder' | 'quote' | 'momentum' | 'late_add' | 'late_dump'


def _ask(snap: Snap, side: str) -> float:
    return snap.yes_ask if side == 'yes' else snap.no_ask


def _bid(snap: Snap, side: str) -> float:
    """yes_bid ≈ 1 - no_ask;  no_bid ≈ 1 - yes_ask."""
    opp = snap.no_ask if side == 'yes' else snap.yes_ask
    if not opp or opp <= 0: return 0.0
    return round(1.0 - opp, 2)


def generate_plan(window: Window) -> list[KPlanOrder]:
    """Emit the Scallops-style order plan for one Kalshi window.

    Uses window.ticks (list of Snap(elapsed, yes_ask, no_ask)) for book state.
    Coin-price-delta is NOT needed — momentum is measured on the market's
    own ask movement, which already encodes price movement.
    """
    orders: list[KPlanOrder] = []
    if not window.ticks: return orders

    ladder = LADDER_PER_COIN.get(window.coin, LADDER_PER_COIN['BTC'])

    # --- 1. PRIMARY LADDER at T=0, clipped below current ask each side ---
    s0 = window.at(0) or window.ticks[0]
    for side in ('yes', 'no'):
        ask_open = _ask(s0, side)
        ceiling = (ask_open - 0.01) if ask_open and ask_open > 0 else 0.99
        for p in ladder:
            if p > ceiling: continue
            sz = _ladder_size(p, window.coin)
            if sz <= 0: continue
            orders.append(KPlanOrder(0, side, p, sz, 'ladder'))

    # --- 2. ACTIVE QUOTE — re-quote on ≥3¢ bid moves with 30s dwell ---
    for side in ('yes', 'no'):
        last_price = None; last_e = -9999
        for s in window.ticks:
            e = s.elapsed
            if e < 0 or e >= 900: continue
            b = _bid(s, side)
            if b <= 0 or b >= 1: continue
            if last_price is None:
                orders.append(KPlanOrder(e, side, b, QUOTE_SIZE, 'quote'))
                last_price = b; last_e = e
                continue
            if abs(b - last_price) >= QUOTE_REPRICE_THR and \
               (e - last_e) >= QUOTE_MIN_DWELL:
                orders.append(KPlanOrder(e, side, b, QUOTE_SIZE, 'quote'))
                last_price = b; last_e = e

    # --- 3. MOMENTUM ADD — scan every 30s after T=60 ---
    def ask_at_elapsed(side, elapsed):
        s = window.at(elapsed)
        return _ask(s, side) if s else None

    last_fire = {'yes': -10_000, 'no': -10_000}
    e = 60
    while e < 870:
        for side in ('yes', 'no'):
            if e - last_fire[side] < MOMENTUM_COOLDOWN_SEC: continue
            now_ask  = ask_at_elapsed(side, e)
            prev_ask = ask_at_elapsed(side, e - MOMENTUM_LOOKBACK_SEC)
            if now_ask is None or prev_ask is None: continue
            if now_ask - prev_ask >= MOMENTUM_THRESHOLD:
                orders.append(KPlanOrder(e, side, now_ask,
                                         MOMENTUM_SIZE, 'momentum'))
                last_fire[side] = e
        e += MOMENTUM_SCAN_INTERVAL

    # --- 4. LATE-WINDOW REBALANCE — data-driven from calibration ---
    # Check at multiple late checkpoints for persistence of signal
    for check_e in (LATE_REBALANCE_T, 660, 720):
        s = window.at(check_e)
        if s is None: continue
        yes_mid = (_ask(s, 'yes') + _bid(s, 'yes')) / 2 if _ask(s, 'yes') else None
        if yes_mid is None: continue

        # Determine leading / losing side
        if LATE_ADD_LO <= yes_mid <= LATE_ADD_HI:
            # YES leading 55-75%, underpriced → passive add at bid+1¢
            b = _bid(s, 'yes')
            if b and 0 < b < 0.95:
                orders.append(KPlanOrder(check_e, 'yes', round(b + 0.01, 2),
                                         LATE_ADD_SIZE, 'late_add'))
                break
        elif (1 - yes_mid) >= (1 - LATE_ADD_HI) and \
             (1 - yes_mid) <= (1 - LATE_ADD_LO):
            # NO leading → passive add at bid+1¢
            b = _bid(s, 'no')
            if b and 0 < b < 0.95:
                orders.append(KPlanOrder(check_e, 'no', round(b + 0.01, 2),
                                         LATE_ADD_SIZE, 'late_add'))
                break

    for check_e in (LATE_REBALANCE_T, 660, 720):
        s = window.at(check_e)
        if s is None: continue
        for side in ('yes', 'no'):
            mid = (_ask(s, side) + _bid(s, side)) / 2
            if mid < LATE_DUMP_CEILING:
                # This side is deep loser. Buy opposite at bid+1¢.
                opp = 'no' if side == 'yes' else 'yes'
                b = _bid(s, opp)
                if b and 0 < b < 0.95:
                    orders.append(KPlanOrder(check_e, opp, round(b + 0.01, 2),
                                             LATE_DUMP_SIZE, 'late_dump'))
                break
        else:
            continue
        break  # fire once

    return orders


# =====================================================================
# Backtest: run plan through KalshiSim, settle, aggregate PnL.
# =====================================================================
def backtest(sim: KalshiSim, coin: str | None = None, verbose=False,
             maker_fee: float = 0.0, taker_fee: float = 0.0):
    """Run backtest.  fee = per-share fee in dollars.
    Negative fee = rebate.  maker_fee applies to ladder/quote/late fills,
    taker_fee to momentum fills."""
    from collections import defaultdict
    results = []
    per_coin = defaultdict(lambda: {'windows':0, 'orders':0, 'fills':0,
                                     'yes_cost':0, 'no_cost':0,
                                     'yes_shares':0, 'no_shares':0,
                                     'pnl':0.0, 'wins':0, 'resolved':0,
                                     'fees':0.0})
    for tk, w in sim.windows.items():
        if coin and w.coin != coin: continue
        if not w.ticks or w.winner not in ('yes','no'): continue
        plan = generate_plan(w)
        if not plan: continue

        # Track fills per side
        fills = []
        quote_cancels_by_side = {'yes': [], 'no': []}
        for o in plan:
            if o.kind == 'quote':
                quote_cancels_by_side[o.side].append(o.elapsed)
        # Sort to allow index-based "next quote" lookup
        for k in quote_cancels_by_side:
            quote_cancels_by_side[k].sort()

        def next_quote_elapsed(side, e):
            seq = quote_cancels_by_side[side]
            import bisect
            i = bisect.bisect_right(seq, e)
            return seq[i] if i < len(seq) else 900

        for o in plan:
            if o.kind == 'quote':
                ttl = max(1, next_quote_elapsed(o.side, o.elapsed) - o.elapsed)
            else:
                ttl = 900 - o.elapsed
            f = sim.limit_buy(tk, o.side, o.elapsed, o.price, o.shares,
                              ttl_sec=ttl)
            if f.reason in ('ask_walk','bid_hit'):
                fills.append((o.side, o.price, o.shares, o.kind))

        # Aggregate per side, with fees
        yes_shares = sum(sh for s,_,sh,_ in fills if s == 'yes')
        no_shares  = sum(sh for s,_,sh,_ in fills if s == 'no')
        yes_cost   = sum(p*sh for s,p,sh,_ in fills if s == 'yes')
        no_cost    = sum(p*sh for s,p,sh,_ in fills if s == 'no')

        total_fees = 0.0
        for (_s, _p, _sh, _k) in fills:
            if _k == 'momentum':
                total_fees += taker_fee * _sh
            else:
                total_fees += maker_fee * _sh

        payoff = (yes_shares if w.winner == 'yes' else 0) + \
                 (no_shares  if w.winner == 'no'  else 0)
        cost = yes_cost + no_cost
        pnl  = payoff - cost - total_fees

        d = per_coin[w.coin]
        d['windows'] += 1
        d['orders'] += len(plan)
        d['fills'] += len(fills)
        d['yes_cost'] += yes_cost
        d['no_cost'] += no_cost
        d['yes_shares'] += yes_shares
        d['no_shares'] += no_shares
        d['pnl'] += pnl
        d['fees'] += total_fees
        d['resolved'] += 1
        if pnl > 0: d['wins'] += 1

        results.append(dict(ticker=tk, coin=w.coin, winner=w.winner,
                            orders=len(plan), fills=len(fills),
                            yes_shares=yes_shares, no_shares=no_shares,
                            yes_cost=yes_cost, no_cost=no_cost,
                            payoff=payoff, cost=cost, fees=total_fees,
                            pnl=pnl))

    return results, per_coin


def print_results(label, results, per_coin):
    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"{'='*80}")
    print(f"{'coin':<6} {'wins':>6} {'fills':>7} {'cost':>9} {'payoff':>9} "
          f"{'fees':>8} {'pnl':>9} {'winrate':>8} {'ROI':>7}")
    print("-" * 80)
    total = {'windows':0,'fills':0,'cost':0.0,'payoff':0.0,
             'pnl':0.0,'fees':0.0,'wins':0}
    for c in ('BTC','ETH','SOL','XRP'):
        d = per_coin[c]
        if not d['windows']: continue
        cost = d['yes_cost']+d['no_cost']
        payoff_sum = sum(r['payoff'] for r in results if r['coin']==c)
        wr = d['wins']/d['resolved']*100 if d['resolved'] else 0
        roi = d['pnl']/cost*100 if cost>0 else 0
        print(f"{c:<6} {d['windows']:>6,} {d['fills']:>7,} "
              f"${cost:>8.2f} ${payoff_sum:>8.2f} "
              f"${d['fees']:>7.2f} ${d['pnl']:>+8.2f} "
              f"{wr:>7.1f}% {roi:>+6.2f}%")
        total['windows'] += d['windows']
        total['fills']   += d['fills'];   total['cost']   += cost
        total['payoff']  += payoff_sum;   total['pnl']    += d['pnl']
        total['fees']    += d['fees'];    total['wins']   += d['wins']
    cost = total['cost']
    wr = total['wins']/total['windows']*100 if total['windows'] else 0
    roi = total['pnl']/cost*100 if cost>0 else 0
    print("-" * 80)
    print(f"{'TOTAL':<6} {total['windows']:>6,} {total['fills']:>7,} "
          f"${total['cost']:>8.2f} ${total['payoff']:>8.2f} "
          f"${total['fees']:>7.2f} ${total['pnl']:>+8.2f} "
          f"{wr:>7.1f}% {roi:>+6.2f}%")


if __name__ == '__main__':
    print("Loading Kalshi windows...", flush=True)
    sim = KalshiSim(); n = sim.load()
    print(f"  {n:,} windows")

    # Scenario 1: no fees
    r1, p1 = backtest(sim, maker_fee=0.0, taker_fee=0.0)
    print_results("NO FEES (baseline)", r1, p1)

    # Scenario 2: taker fee only (conservative)
    r2, p2 = backtest(sim, maker_fee=0.0, taker_fee=0.02)
    print_results("TAKER FEE ONLY (maker=0, taker=2¢/sh)", r2, p2)

    # Scenario 3: maker rebate + taker fee (realistic)
    r3, p3 = backtest(sim, maker_fee=-0.01, taker_fee=0.02)
    print_results("MAKER REBATE + TAKER FEE (maker=-1¢, taker=+2¢)", r3, p3)
