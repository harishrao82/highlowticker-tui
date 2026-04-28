#!/usr/bin/env python3
"""
kalshi_btc_momentum_sim.py — test the BTC momentum strategy on Kalshi.

For each resolved Kalshi BTC/ETH/SOL/XRP 15-min window:
  1. Fetch BTC (or ETH/SOL/XRP) price at window open and at T seconds in.
  2. delta_pct = (p_T - p_open) / p_open * 100
  3. If |delta_pct| >= threshold: side = "yes" if delta > 0 else "no"
  4. Entry = yes_ask or no_ask from Kalshi ticks at elapsed_sec ≈ T
  5. At settlement: won = side == winner; P&L = stake*(1-entry)/entry or -stake
"""
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
import httpx
from rich.console import Console
from rich.table import Table
import random

DB        = Path.home() / ".btc_windows.db"
CACHE     = Path.home() / ".kalshi_btc_1m_cache.json"
CB_URL    = "https://api.exchange.coinbase.com/products/{}/candles"
STAKE     = 100.0
con       = Console()

# Kalshi tickers: KXBTC15M, KXETH15M, KXSOL15M, KXXRP15M
SYMBOLS = {
    "BTC": ("KXBTC15M", "BTC-USD"),
    "ETH": ("KXETH15M", "ETH-USD"),
    "SOL": ("KXSOL15M", "SOL-USD"),
    "XRP": ("KXXRP15M", "XRP-USD"),
}


# ── 1. Load cache ──────────────────────────────────────────────────────────
cache: dict[str, dict[int, float]] = {}
if CACHE.exists():
    try:
        raw = json.loads(CACHE.read_text())
        for product_id, minutes in raw.items():
            cache[product_id] = {int(k): float(v) for k, v in minutes.items()}
        con.print(f"[dim]Cache loaded: "
                  f"{sum(len(v) for v in cache.values())} minutes across "
                  f"{len(cache)} products[/dim]")
    except Exception:
        cache = {}
for p in ("BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"):
    cache.setdefault(p, {})

# Also pull Polymarket BTC cache (may cover overlapping dates)
poly_cache_path = Path.home() / ".scallops_btc_1m_cache.json"
if poly_cache_path.exists():
    try:
        poly_btc = {int(k): float(v) for k, v in json.loads(poly_cache_path.read_text()).items()}
        # merge into BTC-USD bucket
        cache["BTC-USD"].update(poly_btc)
        con.print(f"[dim]Merged {len(poly_btc)} BTC minutes from Polymarket cache[/dim]")
    except Exception:
        pass


# ── 2. Load windows from btc_windows.db ─────────────────────────────────────
db = sqlite3.connect(DB)
all_windows = []
for coin, (prefix, cb_product) in SYMBOLS.items():
    rows = db.execute(
        "SELECT w.id, w.ticker, w.winner, w.window_start_ts, w.floor_strike "
        "FROM windows w WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100",
        (prefix + "%",)
    ).fetchall()
    for wid, ticker, winner, wts, strike in rows:
        all_windows.append({
            "wid": wid, "ticker": ticker, "coin": coin, "winner": winner,
            "wts": wts, "strike": strike, "product": cb_product,
        })

con.print(f"[bold cyan]{len(all_windows)} resolved Kalshi windows[/bold cyan]")


# ── 3. Figure out which minutes we need ───────────────────────────────────
def minute_start(ts: int) -> int:
    return ts - (ts % 60)

needed: dict[str, set[int]] = {p: set() for p in ("BTC-USD","ETH-USD","SOL-USD","XRP-USD")}
for w in all_windows:
    p = w["product"]
    # we want prices at 0, 30, 60, 120, 180, 300, 420, 600 s elapsed
    for off in (0, 30, 60, 120, 180, 300, 420, 600):
        needed[p].add(minute_start(w["wts"] + off))

missing = {p: sorted(set(needed[p]) - set(cache[p].keys())) for p in needed}
missing_count = sum(len(v) for v in missing.values())
con.print(f"[dim]Missing candles: {missing_count} across {len(missing)} products[/dim]")


# ── 4. Fetch missing minutes from Coinbase ────────────────────────────────
def fetch_range(product: str, start_ts: int, end_ts: int, client: httpx.Client):
    """Fetch 1-min candles from Coinbase, [start_ts, end_ts], max 300 per call."""
    params = {
        "start": datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "end":   datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "granularity": 60,
    }
    try:
        r = client.get(CB_URL.format(product), params=params, timeout=15)
        if r.status_code == 200:
            for row in r.json():
                t = int(row[0]); close = float(row[4])
                cache[product][t] = close
            return True
    except Exception as e:
        con.print(f"[yellow]  fetch {product} {e}[/yellow]")
    return False


if missing_count > 0:
    with httpx.Client() as client:
        for product, minutes in missing.items():
            if not minutes: continue
            # group into contiguous blocks
            blocks = []
            cur_block = [minutes[0]]
            for m in minutes[1:]:
                if m - cur_block[-1] <= 60 * 60:  # within 1 hour, merge
                    cur_block.append(m)
                else:
                    blocks.append(cur_block)
                    cur_block = [m]
            blocks.append(cur_block)

            # split into chunks ≤ 300 minutes
            chunks = []
            for b in blocks:
                start = min(b)
                end   = max(b) + 60
                cur = start
                while cur < end:
                    chunk_end = min(cur + 300 * 60, end)
                    chunks.append((cur, chunk_end))
                    cur = chunk_end
            con.print(f"[dim]  {product}: {len(minutes)} missing → {len(chunks)} fetch chunks[/dim]")

            for i, (s, e) in enumerate(chunks):
                fetch_range(product, s, e, client)
                if (i + 1) % 20 == 0:
                    con.print(f"[dim]    {product}: {i+1}/{len(chunks)}[/dim]")
                time.sleep(0.12)

    # Persist
    try:
        CACHE.write_text(json.dumps({
            p: {str(k): v for k, v in m.items()} for p, m in cache.items()
        }))
    except Exception:
        pass

con.print(f"[dim]Cache after fetch: "
          f"{sum(len(v) for v in cache.values())} minutes total[/dim]\n")


# ── 5. Helper: look up ask prices from ticks at elapsed T ────────────────
def kalshi_asks_at(wid: int, T: int) -> tuple[float, float] | None:
    """Return (yes_ask, no_ask) for window at elapsed_sec closest to T (≤ T)."""
    row = db.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks "
        "WHERE window_id=? AND elapsed_sec BETWEEN ? AND ? "
        "ORDER BY elapsed_sec DESC LIMIT 1",
        (wid, max(0, T - 60), T)
    ).fetchone()
    if not row: return None
    _, ya, na = row
    if ya is None or na is None: return None
    return float(ya), float(na)


def price_at(product: str, ts: int) -> float | None:
    return cache[product].get(minute_start(ts))


# ── 6. Run simulation ─────────────────────────────────────────────────────
def simulate(T: int, threshold_pct: float, coin_filter: str | None = None):
    results = []
    for w in all_windows:
        if coin_filter and w["coin"] != coin_filter: continue
        p_open = price_at(w["product"], w["wts"])
        p_T    = price_at(w["product"], w["wts"] + T)
        if not p_open or not p_T: continue
        delta = (p_T - p_open) / p_open * 100
        if abs(delta) < threshold_pct: continue

        side = "yes" if delta > 0 else "no"
        asks = kalshi_asks_at(w["wid"], T)
        if not asks: continue
        ya, na = asks
        entry = ya if side == "yes" else na
        if entry <= 0 or entry >= 1: continue

        won = (side == w["winner"])
        pnl = STAKE * (1 - entry) / entry if won else -STAKE
        results.append({
            "coin": w["coin"], "won": won, "pnl": pnl,
            "entry": entry, "delta": delta, "side": side,
        })
    if not results: return None

    pnls = [r["pnl"] for r in results]
    wins = sum(1 for r in results if r["won"])
    total = sum(pnls)
    staked = STAKE * len(results)
    return dict(
        n=len(results),
        win_rate=wins / len(results),
        total=total, staked=staked,
        agg_return=total / staked,
        mean_pnl=mean(pnls), median_pnl=median(pnls),
        results=results,
    )


def bootstrap_ci(vals, n_boot=3000):
    if not vals: return (0, 0, 0)
    rng = random.Random(42); n = len(vals); ms = []
    for _ in range(n_boot):
        samp = [vals[rng.randrange(n)] for _ in range(n)]
        ms.append(sum(samp) / n)
    ms.sort()
    return (mean(vals), ms[int(n_boot*0.025)], ms[int(n_boot*0.975)])


# ── 7. Scan T × threshold — all coins combined ─────────────────────────────
con.print("[bold]Scan (all coins combined)  —  entry time T × momentum threshold[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("T","Thresh","N","Win%","Return%","Mean/win","95% CI"):
    tbl.add_column(col, justify="right")

Ts = [30, 60, 120, 180, 300, 420, 600]
thresholds = [0.00, 0.02, 0.05, 0.10, 0.20]

best = None
for T in Ts:
    for thr in thresholds:
        r = simulate(T, thr)
        if not r or r["n"] < 30: continue
        mp, lo, hi = bootstrap_ci([x["pnl"] for x in r["results"]])
        col = "green" if r["agg_return"] > 0.03 else "yellow" if r["agg_return"] > 0 else "red"
        tbl.add_row(
            f"{T}s", f"{thr:.2f}%", str(r["n"]),
            f"{r['win_rate']:.0%}",
            f"[{col}]{r['agg_return']*100:+.2f}%[/{col}]",
            f"${mp:+.2f}",
            f"[${lo:+.2f}, ${hi:+.2f}]",
        )
        if (best is None) or (r["agg_return"] > best["agg_return"] and r["n"] >= 50):
            best = {"T": T, "thr": thr, "mp": mp, "lo": lo, "hi": hi, **r}
con.print(tbl)

if best:
    con.print(f"\n[bold green]Best cell:[/bold green]  "
              f"T={best['T']}s  thr={best['thr']}%  "
              f"return={best['agg_return']*100:+.2f}%  "
              f"N={best['n']}  win%={best['win_rate']:.1%}  "
              f"95% CI [${best['lo']:+.2f}, ${best['hi']:+.2f}]")

# ── 8. Best cell, split per coin ──────────────────────────────────────────
if best:
    con.print(f"\n[bold]Per-coin split at best cell (T={best['T']}s, thr={best['thr']}%)[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","N","Win%","Return%","Mean/win","95% CI"):
        tbl.add_column(col, justify="right")
    for coin in ("BTC","ETH","SOL","XRP"):
        r = simulate(best["T"], best["thr"], coin_filter=coin)
        if not r or r["n"] < 10: continue
        mp, lo, hi = bootstrap_ci([x["pnl"] for x in r["results"]])
        col = "green" if r["agg_return"] > 0.03 else "yellow" if r["agg_return"] > 0 else "red"
        tbl.add_row(coin, str(r["n"]), f"{r['win_rate']:.0%}",
                    f"[{col}]{r['agg_return']*100:+.2f}%[/{col}]",
                    f"${mp:+.2f}", f"[${lo:+.2f}, ${hi:+.2f}]")
    con.print(tbl)

# ── 9. Time stability of best cell ────────────────────────────────────────
if best:
    con.print(f"\n[bold]Time stability of best cell[/bold]")
    # rebuild results with window_start for sorting
    by_time = []
    for w in all_windows:
        p_open = price_at(w["product"], w["wts"])
        p_T    = price_at(w["product"], w["wts"] + best["T"])
        if not p_open or not p_T: continue
        delta = (p_T - p_open) / p_open * 100
        if abs(delta) < best["thr"]: continue
        side = "yes" if delta > 0 else "no"
        asks = kalshi_asks_at(w["wid"], best["T"])
        if not asks: continue
        ya, na = asks
        entry = ya if side == "yes" else na
        if entry <= 0 or entry >= 1: continue
        won = side == w["winner"]
        pnl = STAKE*(1-entry)/entry if won else -STAKE
        by_time.append((w["wts"], pnl, won))
    by_time.sort()
    halves = [("First half", by_time[:len(by_time)//2]),
              ("Second half", by_time[len(by_time)//2:])]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Half","N","Win%","Return%"):
        tbl.add_column(col, justify="right")
    for name, ls in halves:
        if not ls: continue
        wins = sum(1 for _,_,w in ls if w)
        total = sum(p for _,p,_ in ls)
        ret = total / (STAKE * len(ls))
        col = "green" if ret > 0 else "red"
        tbl.add_row(name, str(len(ls)), f"{wins/len(ls):.0%}",
                    f"[{col}]{ret*100:+.2f}%[/{col}]")
    con.print(tbl)

db.close()
