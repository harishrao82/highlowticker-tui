#!/usr/bin/env python3
"""
poly_scallops_4h_pattern.py — quick pattern check on Scallops' trades today
to decide whether to keep BTC enabled and whether early checkpoints (T=60s,
T=120s) are viable. Uses ~/.scallops_live_trades.jsonl + Coinbase candles
for window resolution.
"""
import json
import time
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, median

import ccxt
from rich.console import Console
from rich.table import Table

LOG    = Path.home() / ".scallops_live_trades.jsonl"
MIN_TS = int(datetime(2026, 4, 11, 8, 40, tzinfo=timezone.utc).timestamp())
ET     = timezone(timedelta(hours=-4))

_cb = ccxt.coinbase()
_px_cache: dict[str, dict[int, float]] = {
    "BTC": {}, "ETH": {}, "SOL": {}, "XRP": {},
}
COIN_PRODUCT = {"BTC":"BTC-USD","ETH":"ETH-USD","SOL":"SOL-USD","XRP":"XRP-USD"}

con = Console()


def fetch_candles(coin: str, start_ts: int, end_ts: int) -> None:
    product = COIN_PRODUCT[coin]
    cur = start_ts * 1000
    end = end_ts * 1000
    while cur < end:
        try:
            ohlcv = _cb.fetch_ohlcv(product, "1m", cur, 300)
        except Exception:
            return
        if not ohlcv:
            return
        for row in ohlcv:
            t = int(row[0]) // 1000
            _px_cache[coin][t - (t % 60)] = float(row[4])
        last = int(ohlcv[-1][0]) + 60_000
        if last <= cur: return
        cur = last
        time.sleep(0.12)


def price_at(coin: str, ts: int) -> float | None:
    return _px_cache[coin].get(ts - (ts % 60))


def load_trades() -> list[dict]:
    out = []
    for line in open(LOG):
        try:
            e = json.loads(line.strip())
        except Exception:
            continue
        if e.get("trade_ts", 0) < MIN_TS: continue
        if "updown-15m-" not in (e.get("slug") or ""): continue
        if e.get("coin") not in ("BTC","ETH","SOL","XRP"): continue
        if e.get("elapsed_in_window") is None: continue
        out.append(e)
    # Dedupe
    seen = set(); uniq = []
    for e in out:
        tx = e.get("tx","")
        if tx and tx in seen: continue
        seen.add(tx); uniq.append(e)
    return uniq


def main():
    trades = load_trades()
    con.print(f"[bold cyan]{len(trades)} Scallops 15m trades since 04:40 ET Apr 11[/bold cyan]\n")
    if not trades:
        return

    # ── Activity by coin ────────────────────────────────────────────────
    con.print("[bold]Activity by coin (today)[/bold]")
    by_coin = defaultdict(list)
    for t in trades:
        by_coin[t["coin"]].append(t)
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","Trades","Windows","$ bought","Buy %","Sell %"):
        tbl.add_column(col, justify="right")
    for coin in sorted(by_coin):
        ts = by_coin[coin]
        slugs = {t["slug"] for t in ts}
        cost = sum(t["price"]*t["size"] for t in ts)
        buys = sum(1 for t in ts if t["side"]=="BUY")
        sells = sum(1 for t in ts if t["side"]=="SELL")
        tbl.add_row(coin, str(len(ts)), str(len(slugs)),
                    f"${cost:,.0f}",
                    f"{buys/len(ts)*100:.0f}%",
                    f"{sells/len(ts)*100:.0f}%")
    con.print(tbl)

    # ── Fetch candles for all windows ───────────────────────────────────
    ranges: dict[str, tuple[int,int]] = {}
    for t in trades:
        coin = t["coin"]
        wts = t["window_start_ts"]
        end = wts + 960
        cur = ranges.get(coin)
        if cur is None:
            ranges[coin] = (wts, end)
        else:
            ranges[coin] = (min(cur[0], wts), max(cur[1], end))
    for coin, (s,e) in ranges.items():
        con.print(f"[dim]  fetching {coin} candles {datetime.fromtimestamp(s):%H:%M}-{datetime.fromtimestamp(e):%H:%M}…[/dim]")
        fetch_candles(coin, s, e)

    # Determine winner per (coin, window_start_ts)
    now_ts = int(time.time())
    winners: dict[tuple[str,int], str] = {}
    for t in trades:
        coin = t["coin"]; wts = t["window_start_ts"]
        key = (coin, wts)
        if key in winners: continue
        if now_ts < wts + 900: continue  # still open
        p_open  = price_at(coin, wts)
        p_close = price_at(coin, wts + 840)  # ~14:00 into window
        if not (p_open and p_close): continue
        if p_close > p_open:  winners[key] = "Up"
        elif p_close < p_open: winners[key] = "Down"

    # ── Per-trade classification: was his outcome correct? ─────────────
    classified = []
    for t in trades:
        if t["side"] != "BUY": continue
        key = (t["coin"], t["window_start_ts"])
        w = winners.get(key)
        if w is None: continue
        right = (t["outcome"] == w)
        classified.append({**t, "winner": w, "right": right})

    con.print(f"\n[dim]Classified {len(classified)} resolved BUY trades[/dim]\n")

    # ── By coin: hit rate (individual trade right/wrong) ───────────────
    con.print("[bold]Hit rate per coin (individual BUY trades)[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","N","Right","Hit%","Median |Δ|@trade"):
        tbl.add_column(col, justify="right")
    for coin in sorted(set(t["coin"] for t in classified)):
        ts = [t for t in classified if t["coin"]==coin]
        right = sum(1 for t in ts if t["right"])
        deltas = [abs(t["delta_pct"]) for t in ts if t.get("delta_pct") is not None]
        med_d = f"{median(deltas):.3f}%" if deltas else "-"
        pct = right/len(ts)*100 if ts else 0
        col = "green" if pct >= 55 else "yellow" if pct >= 50 else "red"
        tbl.add_row(coin, str(len(ts)), str(right),
                    f"[{col}]{pct:.0f}%[/{col}]", med_d)
    con.print(tbl)

    # ── By elapsed-time bucket ──────────────────────────────────────────
    con.print("\n[bold]Hit rate by elapsed-time bucket (all coins)[/bold]")
    buckets = [(0,30),(30,60),(60,120),(120,180),(180,300),(300,450),(450,600),(600,900)]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("T range","N","Right","Hit%","Avg Poly px","Median |Δ|"):
        tbl.add_column(col, justify="right")
    for lo, hi in buckets:
        ts = [t for t in classified if lo <= t["elapsed_in_window"] < hi]
        if not ts: continue
        right = sum(1 for t in ts if t["right"])
        pct = right/len(ts)*100
        avg_px = mean(t["price"] for t in ts)
        deltas = [abs(t["delta_pct"]) for t in ts if t.get("delta_pct") is not None]
        med_d = f"{median(deltas):.3f}%" if deltas else "-"
        col = "green" if pct >= 55 else "yellow" if pct >= 50 else "red"
        tbl.add_row(f"{lo}-{hi}s", str(len(ts)), str(right),
                    f"[{col}]{pct:.0f}%[/{col}]",
                    f"{avg_px:.2f}", med_d)
    con.print(tbl)

    # ── By (coin × bucket) for the key checkpoints ─────────────────────
    con.print("\n[bold]Per-coin hit rate at early buckets (the ones we're losing on)[/bold]")
    key_buckets = [(60,120),(120,180)]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    tbl.add_column("Coin")
    for lo,hi in key_buckets:
        tbl.add_column(f"T{lo}-{hi}s hit%", justify="right")
        tbl.add_column(f"N", justify="right")
        tbl.add_column(f"Avg px", justify="right")
    for coin in sorted(set(t["coin"] for t in classified)):
        row = [coin]
        for lo, hi in key_buckets:
            ts = [t for t in classified if t["coin"]==coin and lo <= t["elapsed_in_window"] < hi]
            if ts:
                r = sum(1 for t in ts if t["right"])
                pct = r/len(ts)*100
                col = "green" if pct >= 55 else "yellow" if pct >= 50 else "red"
                row.append(f"[{col}]{pct:.0f}%[/{col}]")
                row.append(str(len(ts)))
                row.append(f"{mean(t['price'] for t in ts):.2f}")
            else:
                row += ["-","-","-"]
        tbl.add_row(*row)
    con.print(tbl)

    # ── BTC-specific: his activity during our 300+ hours where we lost ─
    con.print("\n[bold]BTC-specific breakdown by elapsed bucket[/bold]")
    btc = [t for t in classified if t["coin"]=="BTC"]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("T range","N","Hit%","Avg px","Median |Δ|"):
        tbl.add_column(col, justify="right")
    for lo, hi in buckets:
        ts = [t for t in btc if lo <= t["elapsed_in_window"] < hi]
        if not ts: continue
        r = sum(1 for t in ts if t["right"])
        pct = r/len(ts)*100
        deltas = [abs(t["delta_pct"]) for t in ts if t.get("delta_pct") is not None]
        med_d = f"{median(deltas):.3f}%" if deltas else "-"
        col = "green" if pct >= 55 else "yellow" if pct >= 50 else "red"
        tbl.add_row(f"{lo}-{hi}s", str(len(ts)),
                    f"[{col}]{pct:.0f}%[/{col}]",
                    f"{mean(t['price'] for t in ts):.2f}", med_d)
    con.print(tbl)

    # ── Our losing patterns — sanity check head-to-head ────────────────
    con.print("\n[bold]Head-to-head on the early buckets[/bold]")
    our_early = [
        ("60-120s",  "45%", "-$13.27"),
        ("120-180s", "64%", "-$11.64"),
    ]
    for label, ours_hit, ours_pnl in our_early:
        lo, hi = [int(x) for x in label.replace("s","").split("-")]
        ts = [t for t in classified if lo <= t["elapsed_in_window"] < hi]
        if ts:
            his_hit = sum(1 for t in ts if t["right"]) / len(ts) * 100
            con.print(f"  T={label}:  ours win% {ours_hit} P&L {ours_pnl}  →  "
                      f"his hit% {his_hit:.0f}% (N={len(ts)})")
    con.print(f"\n[bold]BTC overall[/bold]")
    if btc:
        total = len(btc)
        r = sum(1 for t in btc if t["right"])
        con.print(f"  ours  BTC 89 fills, win% 66%, P&L -$36.32")
        con.print(f"  his   BTC {total} buy trades, hit% {r/total*100:.0f}%")


if __name__ == "__main__":
    main()
