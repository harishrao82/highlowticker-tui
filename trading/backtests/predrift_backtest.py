#!/usr/bin/env python3
"""
predrift_backtest.py — quick validation of the pre-drift strategy.

Hypothesis: at window open, if (coin_open_price − floor_strike) is strongly
positive or negative (i.e., price drifted in the 2 min before the window
started), the window is "pre-loaded" for YES or NO. Entering at the first
observed Kalshi ask captures the repricing that happens in the first 5–30s
as market makers catch up.

For each resolved window:
  pre_drift_pct = (coin_open_price − floor_strike) / floor_strike × 100
  side_bet      = yes if pre_drift_pct > +THRESHOLD, no if < −THRESHOLD
  entry_px      = first tick yes_ask (or no_ask, depending on side_bet)
  pnl           = (1 − entry) if side_bet == winner else −entry

Run across different thresholds to see which cutoff is most profitable.
"""
import json
import sqlite3
import time
from pathlib import Path
from statistics import mean, median
from collections import defaultdict
import ccxt
from rich.console import Console
from rich.table import Table

DB = Path.home() / ".btc_windows.db"
CACHE = Path.home() / ".predrift_coin_open_cache.json"
con = Console()

COIN_PRODUCT = {"BTC": "BTC-USD", "ETH": "ETH-USD",
                "SOL": "SOL-USD", "XRP": "XRP-USD"}
_cb = ccxt.coinbase()

# Load existing cache
_price_cache: dict = {}
if CACHE.exists():
    try:
        _price_cache = json.loads(CACHE.read_text())
    except Exception:
        _price_cache = {}


def _save_cache() -> None:
    try:
        CACHE.write_text(json.dumps(_price_cache))
    except Exception:
        pass


def coin_open_from_cb(coin: str, wts: int) -> float | None:
    """Fetch Coinbase 1-min candle open price at window_start_ts.
    Uses a shared cache keyed by (coin, wts) to avoid refetching."""
    key = f"{coin}:{wts}"
    if key in _price_cache:
        return _price_cache[key]
    try:
        ohlcv = _cb.fetch_ohlcv(COIN_PRODUCT[coin], "1m", wts * 1000, 2)
        if ohlcv:
            px = float(ohlcv[0][1])   # open of first candle
            _price_cache[key] = px
            return px
    except Exception:
        pass
    _price_cache[key] = None
    return None


def load_windows():
    """Load resolved windows with floor_strike. Fill in coin_open_price from
    Coinbase historical candles for windows that don't have it stored."""
    db = sqlite3.connect(DB)
    rows = db.execute("""
        SELECT
          w.ticker,
          w.window_start_ts,
          w.floor_strike,
          w.coin_open_price,
          w.winner,
          (SELECT yes_ask FROM ticks
           WHERE window_id = w.id AND yes_ask IS NOT NULL
           ORDER BY elapsed_sec LIMIT 1) AS first_yes,
          (SELECT no_ask FROM ticks
           WHERE window_id = w.id AND no_ask IS NOT NULL
           ORDER BY elapsed_sec LIMIT 1) AS first_no,
          (SELECT elapsed_sec FROM ticks
           WHERE window_id = w.id AND yes_ask IS NOT NULL
           ORDER BY elapsed_sec LIMIT 1) AS first_tick_s
        FROM windows w
        WHERE w.floor_strike IS NOT NULL
          AND w.winner IS NOT NULL
    """).fetchall()
    db.close()

    # Backfill coin_open_price from Coinbase candles where missing
    out = []
    need_fetch = 0
    for row in rows:
        ticker, wts, floor, coin_open, winner, fy, fno, fts = row
        if coin_open is None:
            coin = coin_of(ticker)
            coin_open = coin_open_from_cb(coin, wts)
            if coin_open:
                need_fetch += 1
        if coin_open is None:
            continue
        out.append((ticker, wts, floor, coin_open, winner, fy, fno, fts))
        if need_fetch and need_fetch % 30 == 0:
            time.sleep(0.12)   # rate limit pacing
    if need_fetch:
        con.print(f"[dim]Fetched {need_fetch} historical coin_open prices from Coinbase[/dim]")
        _save_cache()
    return out


def coin_of(ticker: str) -> str:
    return ticker[2:5]   # KX|BTC|15M → BTC


def backtest(rows, threshold_pct: float, stake: float = 100.0):
    """
    For each window: if |pre_drift| > threshold, bet on the drift-aligned side
    at the first observed Kalshi ask. Compute P&L at settlement.
    """
    fires = []
    for (ticker, wts, floor, coin_open, winner,
         first_yes, first_no, first_tick_s) in rows:
        if not (floor and coin_open and first_yes and first_no):
            continue
        drift = (coin_open - floor) / floor * 100.0
        if abs(drift) < threshold_pct:
            continue
        if drift > 0:
            side = "yes"
            entry = float(first_yes)
        else:
            side = "no"
            entry = float(first_no)
        if entry <= 0 or entry >= 1:
            continue
        won = (side == winner)
        shares = stake / entry
        pnl = shares * (1.0 - entry) if won else -stake
        fires.append({
            "coin": coin_of(ticker), "ticker": ticker, "wts": wts,
            "drift": drift, "side": side, "entry": entry, "winner": winner,
            "won": won, "pnl": pnl, "first_tick_s": first_tick_s,
        })
    return fires


def summarize(name, fires, stake=100.0):
    if not fires:
        return f"{name}: no fires"
    n = len(fires)
    wins = sum(1 for f in fires if f["won"])
    total_pnl = sum(f["pnl"] for f in fires)
    staked = stake * n
    ret = total_pnl / staked * 100
    avg_entry = mean(f["entry"] for f in fires)
    avg_drift = mean(abs(f["drift"]) for f in fires)
    return (n, wins, f"{wins/n*100:.0f}%",
            f"${total_pnl:+,.2f}", f"{ret:+.2f}%",
            f"{avg_entry:.3f}", f"{avg_drift:.4f}%")


def main():
    rows = load_windows()
    con.print(f"[bold cyan]{len(rows)} resolved windows with full data[/bold cyan]")
    if not rows:
        return

    # ── Scan thresholds ────────────────────────────────────────────────
    con.print("\n[bold]Threshold scan — all coins combined[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Thr","N","Wins","Win%","Total P&L","Return","Avg entry","Avg |drift|"):
        tbl.add_column(col, justify="right")

    for thr in (0.000, 0.010, 0.020, 0.030, 0.040, 0.050, 0.070, 0.100):
        fires = backtest(rows, thr)
        if not fires: continue
        n, wins, wpct, pnl, ret, avg_e, avg_d = summarize(f"{thr}", fires)
        col = "green" if "+" in pnl else "red"
        tbl.add_row(f"{thr:.3f}%", str(n), str(wins), wpct,
                    f"[{col}]{pnl}[/{col}]", ret, avg_e, avg_d)
    con.print(tbl)

    # ── Per-coin at threshold 0.02% ────────────────────────────────────
    con.print("\n[bold]Per coin at |drift| > 0.02%[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","N","Wins","Win%","Total P&L","Return","Avg entry","Avg drift"):
        tbl.add_column(col, justify="right")
    fires = backtest(rows, 0.02)
    by_coin = defaultdict(list)
    for f in fires: by_coin[f["coin"]].append(f)
    for coin in sorted(by_coin):
        cf = by_coin[coin]
        if not cf: continue
        n, wins, wpct, pnl, ret, avg_e, avg_d = summarize(coin, cf)
        col = "green" if "+" in pnl else "red"
        tbl.add_row(coin, str(n), str(wins), wpct,
                    f"[{col}]{pnl}[/{col}]", ret, avg_e, avg_d)
    con.print(tbl)

    # ── Distribution of drift values ───────────────────────────────────
    con.print("\n[bold]Drift magnitude distribution across all resolved windows[/bold]")
    drifts = [(row[3] - row[2]) / row[2] * 100 for row in rows
              if row[2] and row[3]]
    abs_d = sorted(abs(d) for d in drifts)
    n = len(abs_d)
    if n:
        def pct(q): return abs_d[int(n*q)]
        con.print(f"  N={n}  median={pct(0.5):.4f}%  "
                  f"p10={pct(0.1):.4f}%  p25={pct(0.25):.4f}%  "
                  f"p75={pct(0.75):.4f}%  p90={pct(0.9):.4f}%")

    # ── Most recent 10 fires at 0.02% threshold — detail view ─────────
    fires = backtest(rows, 0.02)
    if fires:
        con.print("\n[bold]Most recent 10 fires (threshold 0.02%)[/bold]")
        tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,1))
        for col in ("Window","Coin","Drift%","Side","Entry","Winner","Won","P&L"):
            tbl.add_column(col, justify="right")
        for f in sorted(fires, key=lambda x: -x["wts"])[:10]:
            from datetime import datetime
            t = datetime.fromtimestamp(f["wts"]).strftime("%m-%d %H:%M")
            c = "green" if f["won"] else "red"
            tbl.add_row(t, f["coin"],
                        f"{f['drift']:+.3f}%",
                        f["side"].upper(),
                        f"{f['entry']:.2f}",
                        f["winner"],
                        f"[{c}]{'✓' if f['won'] else '✗'}[/{c}]",
                        f"[{c}]${f['pnl']:+.2f}[/{c}]")
        con.print(tbl)


if __name__ == "__main__":
    main()
