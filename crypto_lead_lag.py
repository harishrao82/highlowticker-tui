#!/usr/bin/env python3
"""
crypto_lead_lag.py — measure whether any exchange systematically leads the
others on XRP (and other coins) using 24h of 1-min OHLCV.

For each (leader, follower) pair and each lag k ∈ [-10, +10] minutes,
compute the Pearson correlation of `leader_returns[t-k]` vs `follower_returns[t]`.
The k that maximizes correlation tells us the effective lead time:

  - k > 0  →  leader's past predicts follower's present (leader leads)
  - k = 0  →  no lead/lag, simultaneous
  - k < 0  →  follower actually leads (surprising)

We only care about positive k with corr > 0.2 for the move to be tradeable.

Coins tested: BTC, ETH, XRP, SOL.
Exchanges: Coinbase, Kraken, Bitstamp, Binance (global public API, read-only).
"""
import math
import time
from datetime import datetime

import ccxt
from rich.console import Console
from rich.table import Table

con = Console()

EXCHANGES = {
    "coinbase":  (ccxt.coinbase,  "USD"),
    "kraken":    (ccxt.kraken,    "USD"),
    "bitstamp":  (ccxt.bitstamp,  "USD"),
    "binanceus": (ccxt.binanceus, "USDT"),   # US-accessible
    "okx":       (ccxt.okx,       "USDT"),
    "huobi":     (ccxt.huobi,     "USDT"),
}

COINS = ["BTC", "ETH", "XRP", "SOL"]

BARS_BACK = 1440   # 24h of 1-min candles (or as much as each venue allows)


def fetch_1m(exchange_cls, base: str, quote: str) -> list[tuple[int, float]]:
    """Return list of (ts_sec, close) over roughly the last 24h."""
    try:
        ex = exchange_cls()
    except Exception:
        return []
    symbol = f"{base}/{quote}"
    try:
        if symbol not in ex.load_markets():
            return []
    except Exception:
        return []

    out: list[tuple[int, float]] = []
    end_ms = int(time.time()) * 1000
    start_ms = end_ms - BARS_BACK * 60_000
    cur = start_ms
    while cur < end_ms:
        try:
            ohlcv = ex.fetch_ohlcv(symbol, "1m", cur, 500)
        except Exception:
            break
        if not ohlcv:
            break
        for row in ohlcv:
            t = int(row[0]) // 1000
            if row[4]:
                out.append((t, float(row[4])))
        last = int(ohlcv[-1][0]) + 60_000
        if last <= cur:
            break
        cur = last
        time.sleep(0.12)
    return out


def log_returns(series: list[tuple[int, float]]) -> dict[int, float]:
    """Minute-over-minute log returns keyed by timestamp."""
    out = {}
    prev_ts = None
    prev_px = None
    for ts, px in series:
        if prev_px and prev_px > 0 and px > 0 and ts - prev_ts == 60:
            out[ts] = math.log(px / prev_px)
        prev_ts = ts
        prev_px = px
    return out


def corr(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 20:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0:
        return 0.0
    return num / (dx * dy)


def lag_xcorr(leader: dict[int, float], follower: dict[int, float],
              max_lag_min: int = 10) -> list[tuple[int, int, float]]:
    """Compute correlation at each integer lag k ∈ [-max, +max].

    k > 0 means leader[t-k] vs follower[t] — leader LEADS by k minutes.
    Returns list of (lag_min, n_pairs, corr).
    """
    results = []
    common_ts = sorted(set(leader.keys()) & set(follower.keys()))
    if len(common_ts) < 50:
        return results
    for k in range(-max_lag_min, max_lag_min + 1):
        xs, ys = [], []
        for t in common_ts:
            ltv = leader.get(t - k * 60)
            fvv = follower.get(t)
            if ltv is not None and fvv is not None:
                xs.append(ltv)
                ys.append(fvv)
        c = corr(xs, ys)
        results.append((k, len(xs), c))
    return results


def main():
    con.print("[bold cyan]Fetching 24h of 1-min OHLCV per (exchange, coin)…[/bold cyan]")
    data: dict[str, dict[str, dict[int, float]]] = {}  # [ex][coin] = returns
    for ex_name, (ex_cls, quote) in EXCHANGES.items():
        data[ex_name] = {}
        for coin in COINS:
            series = fetch_1m(ex_cls, coin, quote)
            ret = log_returns(series)
            data[ex_name][coin] = ret
            con.print(f"  {ex_name:9} {coin}/{quote}  {len(series):,} bars  "
                      f"{len(ret):,} returns")

    con.print("")
    for coin in COINS:
        con.print(f"\n[bold]══ {coin} lead/lag matrix ══[/bold]")
        # For each (leader, follower) pair, find the k that maximizes |corr|
        tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        tbl.add_column("Leader")
        tbl.add_column("Follower")
        tbl.add_column("Best lag (min)", justify="right")
        tbl.add_column("Corr", justify="right")
        tbl.add_column("N", justify="right")
        rows = []
        for leader_name in EXCHANGES:
            for follower_name in EXCHANGES:
                if leader_name == follower_name:
                    continue
                lser = data[leader_name].get(coin, {})
                fser = data[follower_name].get(coin, {})
                if not lser or not fser:
                    continue
                xc = lag_xcorr(lser, fser, max_lag_min=5)
                if not xc:
                    continue
                # Find k with max corr (positive k = leader leads)
                best_k, best_n, best_c = max(xc, key=lambda r: r[2])
                rows.append((leader_name, follower_name, best_k, best_c, best_n))
        # Sort by corr desc
        rows.sort(key=lambda r: -r[3])
        for ln, fn, k, c, n in rows:
            col = "green" if (k > 0 and c > 0.5) else "yellow" if (k >= 0 and c > 0.3) else "dim"
            tbl.add_row(ln, fn, f"{k:+d}", f"[{col}]{c:+.3f}[/{col}]", str(n))
        con.print(tbl)

    # Focus on XRP specifically — show full lag curve from each non-US venue
    # to Coinbase, the main US reference
    con.print("\n[bold]══ XRP detailed lag curves vs Coinbase ══[/bold]")
    coinbase_xrp = data.get("coinbase", {}).get("XRP", {})
    if coinbase_xrp:
        for leader_name in ("binanceus", "okx", "huobi", "kraken", "bitstamp"):
            lxrp = data.get(leader_name, {}).get("XRP", {})
            if not lxrp:
                continue
            xc = lag_xcorr(lxrp, coinbase_xrp, max_lag_min=5)
            if not xc:
                continue
            best_k, _, best_c = max(xc, key=lambda r: r[2])
            con.print(f"\n[cyan]{leader_name}[/cyan] → Coinbase XRP  "
                      f"[dim](positive lag = {leader_name} leads)[/dim]")
            for k, n, c in xc:
                bar_len = max(0, int(c * 40))
                bar = "█" * bar_len
                marker = " ← best" if (k, c) == (best_k, best_c) else ""
                col = "green" if k > 0 and c > 0.3 else "white"
                con.print(f"  [{col}]lag {k:+d}m  corr {c:+.3f}[/{col}]  {bar}{marker}")


if __name__ == "__main__":
    main()
