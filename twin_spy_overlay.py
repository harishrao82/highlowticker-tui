"""Overlay Twin's 5-min Polymarket trades on 1-min SPY bars.

Worst-price entry rules:
  - Twin Up signal  → BUY SPY at the HIGH of the 1-min candle containing the trade
  - Twin Down signal → SHORT SPY at the LOW of that candle
Exit: at the CLOSE of the candle 5 minutes after the trade timestamp.
"""
import json
import time
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import yfinance as yf
import pandas as pd

ET = ZoneInfo("America/New_York")
TWIN_LOG = Path.home() / ".twin_live_trades.jsonl"


def load_twin_signals():
    windows = defaultdict(list)
    with open(TWIN_LOG) as f:
        for line in f:
            r = json.loads(line)
            if r.get("outcome") not in ("Up", "Down"):
                continue
            if r.get("coin") not in ("BTC", "ETH", "SOL", "XRP"):
                continue
            if "5m" not in r.get("slug", ""):
                continue
            wst = r.get("window_start_ts")
            if wst is None:
                continue
            wst = int(wst)
            wst_15m = (wst // 900) * 900
            if wst != wst_15m:
                continue
            windows[(r["coin"], wst_15m)].append(r)

    signals = []
    for key, trades in windows.items():
        trades.sort(key=lambda t: t["trade_ts"])
        f = trades[0]
        price = float(f["price"])
        if price <= 0.50:
            continue
        signals.append({
            "coin": key[0], "wst": key[1],
            "side": f["outcome"], "price": price,
            "trade_ts": int(f["trade_ts"]),
        })
    return signals


def is_market_hours(dt: datetime) -> bool:
    if dt.weekday() >= 5: return False
    h = dt.hour; m = dt.minute
    if h < 9 or (h == 9 and m < 30): return False
    if h >= 16: return False
    return True


def main():
    signals = load_twin_signals()
    market_signals = [s for s in signals
                      if is_market_hours(datetime.fromtimestamp(s["trade_ts"], tz=ET))]
    print(f"Twin >0.50 signals: {len(signals)}")
    print(f"During US market hours: {len(market_signals)}")
    if not market_signals:
        return

    # Fetch SPY 1-min bars covering the full range
    ts_min = min(s["trade_ts"] for s in market_signals)
    ts_max = max(s["trade_ts"] for s in market_signals)
    start = datetime.fromtimestamp(ts_min, tz=ET).date() - timedelta(days=1)
    end = datetime.fromtimestamp(ts_max, tz=ET).date() + timedelta(days=2)
    print(f"Fetching SPY 1-min bars: {start} to {end}...")

    chunks = []
    d = start
    while d < end:
        chunk_end = min(d + timedelta(days=6), end)
        df = yf.Ticker("SPY").history(start=d, end=chunk_end, interval="1m")
        if len(df) > 0: chunks.append(df)
        d = chunk_end
    spy = pd.concat(chunks).sort_index()
    spy = spy[~spy.index.duplicated()]
    print(f"SPY bars: {len(spy):,}")
    print()

    print(f"{'time_ET':>16} {'coin':>4} {'side':>5} {'T$':>5} "
          f"{'entry':>9} {'exit_5m':>9} {'pnl/sh':>8} {'cum':>8}")
    print("-" * 75)

    cum = 0; w = 0; l = 0
    results = []

    for s in sorted(market_signals, key=lambda x: x["trade_ts"]):
        trade_dt = datetime.fromtimestamp(s["trade_ts"], tz=ET)
        # 1-min candle containing trade_ts (floor to minute)
        entry_minute = trade_dt.replace(second=0, microsecond=0)
        # Exit candle: 5 min later, take its close
        exit_minute = entry_minute + timedelta(minutes=5)

        # Find SPY bars
        try:
            entry_bar = spy.loc[pd.Timestamp(entry_minute)]
        except KeyError:
            # Try nearest minute
            mask = (spy.index >= pd.Timestamp(entry_minute)) & \
                   (spy.index < pd.Timestamp(entry_minute + timedelta(minutes=1)))
            sub = spy[mask]
            if len(sub) == 0: continue
            entry_bar = sub.iloc[0]

        # Exit bar (the one starting at +5 min)
        mask = (spy.index >= pd.Timestamp(exit_minute)) & \
               (spy.index < pd.Timestamp(exit_minute + timedelta(minutes=1)))
        exit_bars = spy[mask]
        if len(exit_bars) == 0: continue
        exit_bar = exit_bars.iloc[0]

        if s["side"] == "Up":
            # Long: pay the HIGH of the entry candle (worst entry)
            entry = entry_bar["High"]
            exit_px = exit_bar["Close"]
            pnl = exit_px - entry
            sig = "BUY"
        else:
            # Short: hit the LOW (worst short entry)
            entry = entry_bar["Low"]
            exit_px = exit_bar["Close"]
            pnl = entry - exit_px
            sig = "SHORT"

        cum += pnl
        if pnl > 0.005: w += 1
        elif pnl < -0.005: l += 1
        results.append({"signal": s, "entry": entry, "exit": exit_px, "pnl": pnl})

        print(f"{trade_dt.strftime('%m/%d %I:%M:%S%p'):>16} {s['coin']:>4} "
              f"{sig:>5} {s['price']:>5.2f} "
              f"{entry:>9.2f} {exit_px:>9.2f} {pnl:>+8.3f} {cum:>+8.3f}")

    print()
    print("=" * 70)
    n = w + l
    if n:
        print(f"Total: {len(results)} signals, {w}W/{l}L  WR={w/n*100:.1f}%")
        print(f"PnL/share: ${cum:+.3f}")
        print(f"  At 100 shares SPY: ${cum*100:+.2f}")
        print(f"  At 500 shares SPY: ${cum*500:+.2f}")

    # Per coin
    print(f"\nBy coin:")
    print(f"  {'coin':<6} {'n':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL/sh':>9}")
    for c in ["BTC", "ETH", "SOL", "XRP"]:
        cr = [r for r in results if r["signal"]["coin"] == c]
        if not cr: continue
        cw = sum(1 for r in cr if r["pnl"] > 0.005)
        cl = sum(1 for r in cr if r["pnl"] < -0.005)
        cp = sum(r["pnl"] for r in cr)
        wr = cw/(cw+cl)*100 if (cw+cl) else 0
        print(f"  {c:<6} {len(cr):>4} {cw:>3} {cl:>3} {wr:>5.1f}% {cp:>+8.3f}")


if __name__ == "__main__":
    main()
