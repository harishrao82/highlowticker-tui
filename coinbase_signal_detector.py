"""coinbase_signal_detector.py — detect Twin-style upstream signals.

Subscribes to Coinbase WebSocket for BTC/ETH/SOL/XRP and watches for the
spot-price + taker-flow patterns that probably trigger Twin's first
conviction trade.  Logs every detected signal to JSONL for offline
comparison against Twin's actual trades.

Goal: see if we can detect the same signal Twin uses, before he places
his trade — bypassing the 5-35s Polymarket API lag.

Detection logic (v1):
  Each 5-min window has an open price (snapped at window start).
  Every second, compute:
    delta_pct       = (now - open) / open * 100
    taker_buy_30s   = sum of taker-buy volume over last 30s (USD)
    taker_sell_30s  = sum of taker-sell volume over last 30s (USD)
    imbalance       = (taker_buy - taker_sell) / (taker_buy + taker_sell)
  Signal fires once per 5m window per coin, when:
    abs(delta_pct) >= DELTA_THRESHOLD AND imbalance is in same direction
  Signal: side = 'Up' if delta>0 else 'Down', magnitude = |delta_pct|.
"""
import asyncio
import json
import time
from collections import deque
from pathlib import Path

import websockets

# Coinbase Advanced Trade WS
CB_WS = "wss://advanced-trade-ws.coinbase.com"
CB_PRODS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD"}

# Detection thresholds (tunable)
DELTA_THRESHOLD_PCT = 0.04      # |delta| must exceed this to consider firing
TAKER_LOOKBACK_SEC = 30         # window for taker imbalance
MIN_TAKER_VOLUME_USD = 100      # need this much volume to trust imbalance
WINDOW_LEN = 300                # 5 min
EARLY_T_LIMIT = 60              # only signal in first N seconds (matches Twin)

LOG_PATH = Path.home() / ".coinbase_signals.jsonl"


# Per-coin state
class CoinState:
    def __init__(self, coin):
        self.coin = coin
        self.last_price = 0.0
        self.window_open_price = 0.0
        self.window_start_ts = 0
        self.signal_fired = False
        # rolling 30s of (ts, side, size_usd) where side is 'buy' or 'sell' taker
        self.matches = deque()

    def open_window(self, ts: int, price: float):
        # Align to 5-min boundary
        ws = (ts // WINDOW_LEN) * WINDOW_LEN
        if ws != self.window_start_ts:
            self.window_start_ts = ws
            self.window_open_price = price
            self.signal_fired = False

    def add_match(self, ts: float, side: str, size_usd: float):
        self.matches.append((ts, side, size_usd))
        # Trim to last 30s
        cutoff = ts - TAKER_LOOKBACK_SEC
        while self.matches and self.matches[0][0] < cutoff:
            self.matches.popleft()

    def imbalance(self):
        buy = sum(s for _, side, s in self.matches if side == "buy")
        sell = sum(s for _, side, s in self.matches if side == "sell")
        total = buy + sell
        if total < MIN_TAKER_VOLUME_USD:
            return 0.0, buy, sell
        return (buy - sell) / total, buy, sell


_state: dict[str, CoinState] = {c: CoinState(c) for c in CB_PRODS}


def _log_signal(entry: dict) -> None:
    try:
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        print(f"[log err] {e}")


def _process_signal(coin: str, ts: float, price: float):
    """Check whether to fire a signal for this coin at this moment."""
    s = _state[coin]
    elapsed = ts - s.window_start_ts
    if elapsed > EARLY_T_LIMIT:
        return     # only fire in first 60s of window
    if s.signal_fired:
        return
    if not s.window_open_price or s.window_open_price <= 0:
        return

    delta_pct = (price - s.window_open_price) / s.window_open_price * 100
    if abs(delta_pct) < DELTA_THRESHOLD_PCT:
        return

    imb, buy_vol, sell_vol = s.imbalance()

    # Direction must agree: positive delta + positive imbalance, or both negative
    if delta_pct > 0 and imb < 0.05:
        return
    if delta_pct < 0 and imb > -0.05:
        return

    side = "Up" if delta_pct > 0 else "Down"
    s.signal_fired = True

    entry = {
        "ts": round(ts, 3),
        "coin": coin,
        "side": side,
        "elapsed": round(elapsed, 2),
        "window_start_ts": s.window_start_ts,
        "open_price": s.window_open_price,
        "now_price": price,
        "delta_pct": round(delta_pct, 4),
        "taker_imbalance": round(imb, 3),
        "taker_buy_30s_usd": round(buy_vol, 2),
        "taker_sell_30s_usd": round(sell_vol, 2),
    }
    _log_signal(entry)
    print(f"  ★ {coin} {side} @ T={elapsed:.0f}s  Δ={delta_pct:+.3f}%  "
          f"imb={imb:+.2f}  buy=${buy_vol:.0f}  sell=${sell_vol:.0f}")


async def cb_feed():
    """Subscribe to Coinbase market_trades + ticker for taker tape and quotes."""
    coin_list = list(CB_PRODS.keys())
    products = [CB_PRODS[c] for c in coin_list]

    while True:
        try:
            async with websockets.connect(CB_WS, ping_interval=20) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": products,
                    "channel": "market_trades",
                }))
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": products,
                    "channel": "ticker",
                }))
                print(f"Coinbase WS connected — {products}")

                async for raw in ws:
                    m = json.loads(raw)
                    ch = m.get("channel")
                    for ev in m.get("events", []):
                        if ch == "ticker":
                            for t in ev.get("tickers", []):
                                pid = t.get("product_id")
                                price = float(t.get("price", 0) or 0)
                                if not price:
                                    continue
                                coin = next((c for c, p in CB_PRODS.items() if p == pid), None)
                                if not coin:
                                    continue
                                ts = time.time()
                                state = _state[coin]
                                state.last_price = price
                                state.open_window(int(ts), price)
                                _process_signal(coin, ts, price)
                        elif ch == "market_trades":
                            for tr in ev.get("trades", []):
                                pid = tr.get("product_id")
                                coin = next((c for c, p in CB_PRODS.items() if p == pid), None)
                                if not coin:
                                    continue
                                size = float(tr.get("size", 0) or 0)
                                price = float(tr.get("price", 0) or 0)
                                side_raw = tr.get("side", "").lower()  # 'BUY' = taker buy
                                if not size or not price:
                                    continue
                                ts = time.time()
                                # Coinbase 'side' is the maker side; taker is opposite
                                # 'BUY' in market_trades means taker bought (lifted offer)
                                taker_side = "buy" if side_raw == "buy" else "sell"
                                _state[coin].add_match(ts, taker_side, size * price)
        except Exception as e:
            print(f"[cb reconnect: {e}]")
            await asyncio.sleep(2)


async def status_loop():
    """Print compact status every 15s."""
    while True:
        await asyncio.sleep(15)
        rows = []
        now = time.time()
        for coin, s in _state.items():
            if not s.window_open_price:
                continue
            elapsed = int(now - s.window_start_ts)
            delta = (s.last_price - s.window_open_price) / s.window_open_price * 100 if s.window_open_price else 0
            imb, buy, sell = s.imbalance()
            mark = "★" if s.signal_fired else " "
            rows.append(f"{coin}{mark} T={elapsed:>3}s Δ{delta:+.3f}% imb{imb:+.2f}")
        if rows:
            print(f"[{time.strftime('%H:%M:%S')}] " + " | ".join(rows))


async def main():
    print(f"Coinbase signal detector starting...")
    print(f"  Threshold: |Δ| >= {DELTA_THRESHOLD_PCT}% AND taker imbalance same direction")
    print(f"  Fire window: first {EARLY_T_LIMIT}s of each 5-min window")
    print(f"  Log: {LOG_PATH}")
    print()
    await asyncio.gather(cb_feed(), status_loop())


if __name__ == "__main__":
    asyncio.run(main())
