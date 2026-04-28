#!/usr/bin/env python3
"""
crypto_floor_predict.py — for each Kalshi window with a stored floor_strike,
measure which exchange's spot price best predicts it in the seconds/minutes
before window open.

Best predictor = smallest mean absolute % error between exchange_price_at(T)
and the eventual floor_strike. Tested at T=-300s, T=-60s, T=-30s, T=0s,
T=+30s, T=+60s relative to window_start_ts.

Uses existing btc_windows.db for the floor_strike values, and ccxt for
historical OHLCV across multiple exchanges.
"""
import math
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from statistics import mean, median

import ccxt
from rich.console import Console
from rich.table import Table

DB = Path.home() / ".btc_windows.db"
con = Console()

# Exchanges to test. Each as (ccxt class, quote currency).
EXCHANGES = {
    "coinbase":  (ccxt.coinbase,  "USD"),
    "kraken":    (ccxt.kraken,    "USD"),
    "bitstamp":  (ccxt.bitstamp,  "USD"),
    "binanceus": (ccxt.binanceus, "USDT"),
    "okx":       (ccxt.okx,       "USDT"),
    "huobi":     (ccxt.huobi,     "USDT"),
}

COINS = ["BTC", "ETH", "SOL", "XRP"]

# Time offsets (seconds) relative to window_start_ts
T_OFFSETS = [-300, -180, -120, -60, -30, 0, +30, +60, +120]


def load_windows():
    """Return list of (coin, window_start_ts, floor_strike, coin_open)."""
    db = sqlite3.connect(DB)
    rows = db.execute("""
        SELECT substr(ticker,3,3), window_start_ts, floor_strike, coin_open_price
        FROM windows
        WHERE floor_strike IS NOT NULL AND floor_strike > 0
    """).fetchall()
    db.close()
    return [(r[0], int(r[1]), float(r[2]), float(r[3]) if r[3] else None)
            for r in rows]


def fetch_candles(ex_cls, symbol: str, start_ts: int, end_ts: int) -> dict[int, float]:
    """Return minute_ts → close price from one exchange. Empty dict on failure."""
    out: dict[int, float] = {}
    try:
        ex = ex_cls()
        if symbol not in ex.load_markets():
            return out
    except Exception:
        return out

    cur = start_ts * 1000
    end = end_ts * 1000
    while cur < end:
        try:
            ohlcv = ex.fetch_ohlcv(symbol, "1m", cur, 500)
        except Exception:
            return out
        if not ohlcv:
            return out
        for row in ohlcv:
            t = int(row[0]) // 1000
            px = float(row[4] or 0)
            if px > 0:
                out[t - (t % 60)] = px
        last = int(ohlcv[-1][0]) + 60_000
        if last <= cur:
            return out
        cur = last
        time.sleep(0.12)
    return out


def price_at(candles: dict[int, float], ts: int) -> float | None:
    """Closest prior minute close to ts."""
    m = ts - (ts % 60)
    if m in candles:
        return candles[m]
    # walk back up to 5 minutes
    for d in range(1, 6):
        if (m - d * 60) in candles:
            return candles[m - d * 60]
    return None


def main():
    windows = load_windows()
    con.print(f"[bold cyan]Loaded {len(windows)} Kalshi windows with floor_strike[/bold cyan]")

    # Time range
    ts_min = min(w[1] for w in windows) - 600
    ts_max = max(w[1] for w in windows) + 600
    from datetime import datetime
    con.print(f"[dim]Range: {datetime.fromtimestamp(ts_min)} → "
              f"{datetime.fromtimestamp(ts_max)}[/dim]\n")

    # Fetch candles per (exchange, coin)
    candles: dict[str, dict[str, dict[int, float]]] = defaultdict(dict)
    for ex_name, (ex_cls, quote) in EXCHANGES.items():
        for coin in COINS:
            symbol = f"{coin}/{quote}"
            con.print(f"  [dim]fetching {ex_name:10} {symbol}…[/dim]")
            candles[ex_name][coin] = fetch_candles(ex_cls, symbol, ts_min, ts_max)

    # ── Per-coin: best predictor of floor_strike at each T offset ────────
    for coin in COINS:
        coin_windows = [w for w in windows if w[0] == coin]
        if not coin_windows:
            continue

        con.print(f"\n[bold]══ {coin} — error vs floor_strike ({len(coin_windows)} windows) ══[/bold]")
        tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
        tbl.add_column("Exchange")
        tbl.add_column("T (s)", justify="right")
        tbl.add_column("N", justify="right")
        tbl.add_column("Mean bias %", justify="right")
        tbl.add_column("MAE %", justify="right")
        tbl.add_column("Median |err| %", justify="right")

        # Build (exchange, T) → list of % errors
        grid: dict[tuple[str, int], list[float]] = defaultdict(list)
        for (c, wts, strike, _open) in coin_windows:
            for ex_name in EXCHANGES:
                ex_candles = candles[ex_name].get(coin) or {}
                for T in T_OFFSETS:
                    px = price_at(ex_candles, wts + T)
                    if px is None or strike <= 0:
                        continue
                    err_pct = (px - strike) / strike * 100.0
                    grid[(ex_name, T)].append(err_pct)

        # Sort by (exchange, T)
        for ex_name in EXCHANGES:
            for T in T_OFFSETS:
                errs = grid.get((ex_name, T), [])
                if not errs:
                    continue
                bias = mean(errs)
                mae = mean(abs(e) for e in errs)
                med_abs = median(abs(e) for e in errs)
                col = "green" if mae < 0.03 else "yellow" if mae < 0.07 else "red"
                tbl.add_row(
                    ex_name,
                    f"{T:+d}",
                    str(len(errs)),
                    f"{bias:+.4f}%",
                    f"[{col}]{mae:.4f}%[/{col}]",
                    f"{med_abs:.4f}%",
                )
        con.print(tbl)

        # Summary: best predictor at T=0 (window open)
        con.print(f"[bold]  {coin} best predictor at T=0s:[/bold]")
        best = []
        for ex_name in EXCHANGES:
            errs = grid.get((ex_name, 0), [])
            if errs:
                best.append((ex_name, mean(abs(e) for e in errs), mean(errs)))
        best.sort(key=lambda x: x[1])
        for rank, (ex, mae, bias) in enumerate(best[:3], 1):
            con.print(f"    [green]{rank}. {ex}[/green]  MAE {mae:.4f}%  "
                      f"bias {bias:+.4f}%")


if __name__ == "__main__":
    main()
