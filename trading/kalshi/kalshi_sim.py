"""Kalshi 15m crypto Up/Down simulator.

Data sources:
  - ~/.btc_windows.db      (windows + per-second yes_ask/no_ask ticks)
  - ~/.kalshi_fill_events.jsonl (real fills — used only for calibration)

Book model (top of book only, binary market):
  best yes ask = yes_ask                best yes bid ≈ 1 - no_ask
  best no  ask = no_ask                 best no  bid ≈ 1 - yes_ask

Fill model:
  MARKET BUY  side at ts:
      fill at the ask of that side, at the snapshot closest to ts
      (within MAX_SNAPSHOT_GAP_SEC).  Assumes unlimited depth — we don't
      have order-book sizes.

  LIMIT BUY   side at price P, placed at ts:
      walk forward through snapshots in the same window; fill at the
      FIRST snapshot whose side-ask <= P.  Fill price = that snapshot's
      ask (price improvement credited to you when the ask walks below P).
      This is a CONSERVATIVE maker model — it misses maker fills that
      would have happened via sub-second market-sell hits we can't see.
      Deadline defaults to window close.

Settlement:
  Winner comes from windows.winner.  YES shares pay $1 if winner='yes',
  NO shares pay $1 if winner='no'.  Cost basis = sum of fill notionals.
"""
import json
import sqlite3
import bisect
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

DB_PATH = Path.home() / ".btc_windows.db"
FILL_LOG = Path.home() / ".kalshi_fill_events.jsonl"

MAX_SNAPSHOT_GAP_SEC = 5.0       # for market-order fill lookup
WINDOW_LEN_SEC = 900             # default limit-order deadline


@dataclass
class Snap:
    elapsed: int
    yes_ask: float
    no_ask: float


@dataclass
class Fill:
    elapsed: float          # seconds into window when fill occurred
    price: float            # fill price (0 if no fill)
    shares: int             # shares filled
    reason: str             # 'market' | 'bid_hit' | 'ask_walk' |
                            # 'no_book' | 'canceled' | 'no_fill'


@dataclass
class Window:
    ticker: str
    coin: str
    window_start_ts: int
    floor_strike: float | None
    winner: str | None          # 'yes' / 'no' / None (unresolved)
    coin_open_price: float | None
    ticks: list[Snap] = field(default_factory=list)
    _elapsed: list[int] = field(default_factory=list, repr=False)

    def finalize(self):
        self.ticks.sort(key=lambda s: s.elapsed)
        self._elapsed = [s.elapsed for s in self.ticks]

    def at(self, elapsed: float) -> Snap | None:
        if not self._elapsed: return None
        i = bisect.bisect_left(self._elapsed, elapsed)
        cands = []
        if i < len(self._elapsed): cands.append(self.ticks[i])
        if i > 0: cands.append(self.ticks[i-1])
        if not cands: return None
        best = min(cands, key=lambda s: abs(s.elapsed - elapsed))
        if abs(best.elapsed - elapsed) > MAX_SNAPSHOT_GAP_SEC:
            return None
        return best

    def forward_from(self, elapsed: float):
        i = bisect.bisect_left(self._elapsed, elapsed)
        for j in range(i, len(self.ticks)):
            yield self.ticks[j]


class KalshiSim:
    def __init__(self):
        self.windows: dict[str, Window] = {}

    def load(self, coin: str | None = None, limit: int | None = None):
        """Load windows + ticks from btc_windows.db."""
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        q = "SELECT id, ticker, window_start_ts, floor_strike, winner, coin_open_price FROM windows"
        params = []
        if coin:
            q += " WHERE ticker LIKE ?"
            params.append(f"KX{coin.upper()}15M-%")
        q += " ORDER BY window_start_ts"
        if limit: q += f" LIMIT {int(limit)}"
        rows = con.execute(q, params).fetchall()
        id_to_window = {}
        for r in rows:
            tk = r['ticker']
            coin_code = tk.split('15M')[0].replace('KX','')  # BTC / ETH / SOL / XRP
            w = Window(ticker=tk, coin=coin_code,
                       window_start_ts=r['window_start_ts'],
                       floor_strike=r['floor_strike'],
                       winner=r['winner'],
                       coin_open_price=r['coin_open_price'])
            self.windows[tk] = w
            id_to_window[r['id']] = w
        # Bulk-fetch ticks
        ids = tuple(id_to_window.keys())
        if ids:
            placeholder = ','.join('?'*len(ids))
            rows = con.execute(
                f"SELECT window_id, elapsed_sec, yes_ask, no_ask "
                f"FROM ticks WHERE window_id IN ({placeholder})", ids).fetchall()
            for r in rows:
                w = id_to_window[r['window_id']]
                if r['yes_ask'] is None or r['no_ask'] is None: continue
                w.ticks.append(Snap(elapsed=r['elapsed_sec'],
                                    yes_ask=r['yes_ask'], no_ask=r['no_ask']))
        for w in self.windows.values():
            w.finalize()
        con.close()
        return len(self.windows)

    # ---- fill helpers ----
    def _ask(self, snap: Snap, side: str) -> float:
        return snap.yes_ask if side == 'yes' else snap.no_ask

    def market_buy(self, ticker: str, side: str, elapsed: float,
                   shares: int) -> Fill:
        w = self.windows.get(ticker)
        if w is None: return Fill(elapsed, 0, 0, 'no_book')
        s = w.at(elapsed)
        if s is None: return Fill(elapsed, 0, 0, 'no_book')
        p = self._ask(s, side)
        if not p or p <= 0: return Fill(elapsed, 0, 0, 'no_book')
        return Fill(elapsed=s.elapsed, price=p, shares=shares, reason='market')

    def _bid(self, snap: Snap, side: str) -> float:
        """Derived bid for a side: yes_bid ≈ 1 − no_ask."""
        opp = snap.no_ask if side == 'yes' else snap.yes_ask
        if not opp or opp <= 0: return 0.0
        return round(1.0 - opp, 2)

    def limit_buy(self, ticker: str, side: str, elapsed_start: float,
                  limit_price: float, shares: int,
                  deadline: float | None = None,
                  hold_sec: float = 0.0,
                  ttl_sec: float | None = None) -> Fill:
        """Passive limit buy.  Two fill paths, whichever comes first:

            BID_HIT    derived bid >= limit_price (our bid is at or inside
                       top of book) for `hold_sec` consecutive seconds.
                       Fill at limit_price.  Models maker fill.

            ASK_WALK   ask drops to <= limit_price.  Fill at limit_price.
                       Models the book walking into our bid aggressively.

        `ttl_sec`: if set, order cancels at elapsed_start + ttl_sec and
        returns reason='canceled'.  Otherwise runs to window close or
        `deadline`.
        """
        w = self.windows.get(ticker)
        if w is None: return Fill(elapsed_start, 0, 0, 'no_book')
        if deadline is None: deadline = WINDOW_LEN_SEC
        cancel_at = elapsed_start + ttl_sec if ttl_sec is not None else float('inf')
        effective_deadline = min(deadline, cancel_at)

        was_at_or_above = False

        for s in w.forward_from(elapsed_start):
            if s.elapsed > effective_deadline: break
            ask = self._ask(s, side)
            bid = self._bid(s, side)

            if ask and 0 < ask <= limit_price:
                return Fill(s.elapsed, limit_price, shares, 'ask_walk')

            # Maker path: bid reached our limit then crossed down.  Known
            # information limit: with top-of-book only, we can't tell
            # whether the cross-down hit us or someone ahead in queue, so
            # this model over-predicts fills by ~35% vs reality (see
            # fill_model_calibration.py).  Apply `fill_prob` at the
            # strategy layer if a realism haircut is desired.
            if bid >= limit_price:
                was_at_or_above = True
            elif was_at_or_above:
                return Fill(s.elapsed, limit_price, shares, 'bid_hit')

        if ttl_sec is not None and cancel_at <= deadline:
            return Fill(cancel_at, 0, 0, 'canceled')
        return Fill(effective_deadline, 0, 0, 'no_fill')

    # ---- settlement ----
    def settle_position(self, ticker: str, side: str, shares: int,
                        avg_cost: float) -> tuple[float, float, str]:
        """Returns (pnl_dollars, notional_at_cost, winner)."""
        w = self.windows.get(ticker)
        if w is None or w.winner not in ('yes','no'):
            return (0.0, shares*avg_cost, 'unresolved')
        payoff = shares * (1.0 if w.winner == side else 0.0)
        pnl = payoff - shares * avg_cost
        return (pnl, shares*avg_cost, w.winner)


# =====================================================================
# Calibration: replay real fill events through the simulator.
# =====================================================================
def calibrate(sim: KalshiSim):
    """For each real fill event, ask the simulator 'what would you have filled
    this order at?' and compare to the real fill price.  Separates market
    vs patient orders since they use different fill models."""
    # Build order_id -> limit_price_sent map from momentum trade intents
    MOM_LOG = Path.home() / ".kalshi_momentum_trades.jsonl"
    limit_by_oid: dict[str, float] = {}
    with open(MOM_LOG) as f:
        for line in f:
            r = json.loads(line)
            oid = r.get('order_id')
            lp  = r.get('limit_price_sent')
            if oid and lp is not None:
                limit_by_oid[oid] = float(lp)
    print(f"Intent limits indexed by order_id: {len(limit_by_oid):,}")

    events = []
    with open(FILL_LOG) as f:
        for line in f:
            r = json.loads(line)
            if r.get('ticker') not in sim.windows: continue
            events.append(r)
    print(f"Fill events covered by loaded windows: {len(events):,}\n")

    def window_start_ts(ticker):
        return sim.windows[ticker].window_start_ts

    by_role = defaultdict(list)
    for ev in events:
        w = sim.windows[ev['ticker']]
        sent_el  = ev['sent_at'] - w.window_start_ts
        fill_el  = ev['fill_at'] - w.window_start_ts
        role = ev.get('role', '?')
        side = ev['side']
        shares = ev['shares']
        actual = ev['price']

        # Classify role into market vs patient(limit) archetypes
        if role in ('market', 'dd-market'):
            sim_fill = sim.market_buy(ev['ticker'], side, sent_el, shares)
            archetype = 'market'
        elif role in ('patient', 'dd-patient', 'contra-pat', 'surface-pat',
                      'hedge-pat', 'contra', 'avgdn', 'hedge', 'surface'):
            # Look up the real limit_price_sent from the intent log
            lp = limit_by_oid.get(ev.get('order_id'))
            if lp is None: continue
            sim_fill = sim.limit_buy(ev['ticker'], side, sent_el,
                                     limit_price=lp, shares=shares)
            archetype = 'patient'
        else:
            continue

        if sim_fill.shares == 0: continue
        diff = sim_fill.price - actual
        by_role[archetype].append((diff, sim_fill, ev))

    print(f"{'archetype':<12} {'n':>6} {'mean':>9} {'p50':>8} {'±1¢':>7} "
          f"{'±2¢':>7} {'±3¢':>7}")
    print("-"*60)
    for arch in ('market','patient'):
        xs = [d for d,_,_ in by_role[arch]]
        if not xs: continue
        xs_sorted = sorted(xs)
        p50 = xs_sorted[len(xs)//2]
        mean = sum(xs)/len(xs)
        w1 = sum(abs(x)<=0.01 for x in xs)/len(xs)*100
        w2 = sum(abs(x)<=0.02 for x in xs)/len(xs)*100
        w3 = sum(abs(x)<=0.03 for x in xs)/len(xs)*100
        print(f"{arch:<12} {len(xs):>6,} {mean:>+9.4f} {p50:>+8.4f} "
              f"{w1:>6.1f}% {w2:>6.1f}% {w3:>6.1f}%")

    # ---- Cancel-rate validation ----
    # For every momentum-intent patient order, run the sim with ttl=15s and
    # compare to reality: did it fill or not?
    print("\n--- Cancel/fill prediction vs reality (patient, 15s ttl) ---")
    filled_oids = {ev['order_id'] for ev in events}
    mom_rows = []
    with open(MOM_LOG) as f:
        for line in f:
            r = json.loads(line)
            if r.get('order_id') not in limit_by_oid: continue
            if r.get('window') not in sim.windows: continue
            mom_rows.append(r)

    confusion = {('fill','fill'):0, ('fill','cancel'):0,
                 ('cancel','fill'):0, ('cancel','cancel'):0}
    for r in mom_rows:
        w = sim.windows[r['window']]
        # elapsed_actual approximates when the order hit the market
        sent_el = r.get('elapsed_actual') or r.get('T_checkpoint') or 0
        lp = limit_by_oid[r['order_id']]
        side = r['side']
        f = sim.limit_buy(r['window'], side, sent_el, lp,
                          shares=r.get('shares', 1), ttl_sec=15.0)
        real = 'fill' if r['order_id'] in filled_oids else 'cancel'
        predicted = 'fill' if f.reason in ('ask_walk','bid_hit') else 'cancel'
        confusion[(real, predicted)] += 1

    tot = sum(confusion.values())
    if tot:
        print(f"{'real\\pred':<12} {'fill':>8} {'cancel':>8}")
        print(f"{'fill':<12} {confusion[('fill','fill')]:>8} "
              f"{confusion[('fill','cancel')]:>8}")
        print(f"{'cancel':<12} {confusion[('cancel','fill')]:>8} "
              f"{confusion[('cancel','cancel')]:>8}")
        correct = confusion[('fill','fill')] + confusion[('cancel','cancel')]
        print(f"accuracy: {correct/tot*100:.1f}%  (n={tot})")


if __name__ == '__main__':
    sim = KalshiSim()
    n = sim.load()
    print(f"Loaded {n:,} windows from btc_windows.db\n")
    calibrate(sim)
