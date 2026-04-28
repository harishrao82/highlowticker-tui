#!/usr/bin/env python3
"""
fill_gap_analysis.py — analyze the time gap between market (taker) and
patient (maker) fills, split by wins vs losses.

Key finding: instant patient fills (<1s) have 88% win rate.
Slow fills (2-4s) drop to 50%. Late fills (7-15s) are 54%.

Usage:
  python fill_gap_analysis.py              # since 5:45am Apr 12
  python fill_gap_analysis.py --hours 24   # last 24 hours
  python fill_gap_analysis.py --all        # all fills
"""
import argparse
import asyncio
import base64
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from statistics import mean, median

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()
console = Console()

BASE = "https://api.elections.kalshi.com/trade-api/v2"
ET = timezone(timedelta(hours=-4))
api_key = os.environ["KALSHI_API_KEY"]
pk = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)


def sign(method, path):
    ts = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = pk.sign(
        msg.encode(),
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def coin_of(t):
    return t[2:5]


def wts_of(ticker):
    try:
        _, dp, _ = ticker.split("-")
        yy = 2000 + int(dp[:2])
        m = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
            "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
            "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }[dp[2:5]]
        d = int(dp[5:7])
        h = int(dp[7:9])
        mi = int(dp[9:11])
        return int(
            (datetime(yy, m, d, h, mi, tzinfo=ET) - timedelta(minutes=15)).timestamp()
        )
    except Exception:
        return None


async def fetch_fills(since_ts):
    async with httpx.AsyncClient() as c:
        fills = []
        cursor = ""
        for _ in range(20):
            params = {"min_ts": since_ts, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            r = await c.get(
                BASE + "/portfolio/fills",
                headers=sign("GET", "/trade-api/v2/portfolio/fills"),
                params=params,
                timeout=15,
            )
            if r.status_code != 200:
                break
            j = r.json()
            batch = j.get("fills", [])
            if not batch:
                break
            fills.extend(batch)
            cursor = j.get("cursor", "")
            if not cursor:
                break

        tickers = sorted({f["ticker"] for f in fills})
        console.print(f"[dim]Resolving {len(tickers)} markets…[/dim]")
        results = {}
        for t in tickers:
            try:
                r = await c.get(
                    BASE + f"/markets/{t}",
                    headers=sign("GET", f"/trade-api/v2/markets/{t}"),
                    timeout=8,
                )
                results[t] = (
                    r.json().get("market", {}).get("result", "")
                    if r.status_code == 200
                    else ""
                )
            except Exception:
                results[t] = ""

    return fills, results


def analyze(fills, results):
    enriched = []
    for f in fills:
        t = f["ticker"]
        side = f["side"]
        shares = float(f["count_fp"])
        entry = (
            float(f["yes_price_dollars"])
            if side == "yes"
            else float(f["no_price_dollars"])
        )
        ts = int(f["ts"])
        coin = coin_of(t)
        result = results.get(t, "")
        won = None
        if result in ("yes", "no"):
            won = side == result
        enriched.append(
            dict(
                coin=coin, side=side, shares=shares, entry=entry,
                ts=ts, won=won, ticker=t, is_taker=f.get("is_taker"),
                created_time=f.get("created_time"),
            )
        )

    # Group into fires
    enriched.sort(key=lambda x: (x["ticker"], x["side"], x["ts"]))
    fires = []
    current = []
    for e in enriched:
        if current and (
            e["ticker"] != current[0]["ticker"]
            or e["side"] != current[0]["side"]
            or e["ts"] - current[-1]["ts"] > 60
        ):
            fires.append(current)
            current = []
        current.append(e)
    if current:
        fires.append(current)

    # Classify
    full_fills_win = []
    full_fills_loss = []
    market_only_win = []
    market_only_loss = []

    for fire in fires:
        if any(e["won"] is None for e in fire):
            continue
        won = fire[0]["won"]
        fire.sort(key=lambda x: x["ts"])

        if len(fire) == 1:
            if won:
                market_only_win.append(fire)
            else:
                market_only_loss.append(fire)
        else:
            mkt_ts = fire[0]["ts"]
            gaps = [f["ts"] - mkt_ts for f in fire[1:]]
            avg_gap = mean(gaps)
            max_gap = max(gaps)
            min_gap = min(gaps)

            rec = dict(
                won=won, coin=fire[0]["coin"], n_fills=len(fire),
                avg_gap=avg_gap, max_gap=max_gap, min_gap=min_gap,
                mkt_entry=fire[0]["entry"],
                pat_entries=[f["entry"] for f in fire[1:]],
                mkt_shares=fire[0]["shares"],
                pat_shares=sum(f["shares"] for f in fire[1:]),
                ticker=fire[0]["ticker"],
                side=fire[0]["side"],
                ts=fire[0]["ts"],
            )
            if won:
                full_fills_win.append(rec)
            else:
                full_fills_loss.append(rec)

    return full_fills_win, full_fills_loss, market_only_win, market_only_loss


def print_results(full_wins, full_losses, mo_wins, mo_losses):
    console.print(f"\n[bold]Fill gap analysis[/bold]")
    console.print(f"  Full fill WINS:     {len(full_wins)}")
    console.print(f"  Full fill LOSSES:   {len(full_losses)}")
    console.print(f"  Market-only WINS:   {len(mo_wins)}")
    console.print(f"  Market-only LOSSES: {len(mo_losses)}")

    if mo_wins or mo_losses:
        mo_total = len(mo_wins) + len(mo_losses)
        console.print(f"\n  Market-only win rate: "
                      f"{len(mo_wins)}/{mo_total} = "
                      f"{len(mo_wins)/mo_total*100:.0f}%")

    if full_wins:
        w_gaps = [r["avg_gap"] for r in full_wins]
        console.print(f"\n[bold]WINS — market→patient gap:[/bold]")
        console.print(f"  avg={mean(w_gaps):.1f}s  med={median(w_gaps):.1f}s  "
                      f"min={min(w_gaps):.1f}s  max={max(w_gaps):.1f}s")

    if full_losses:
        l_gaps = [r["avg_gap"] for r in full_losses]
        console.print(f"\n[bold]LOSSES — market→patient gap:[/bold]")
        console.print(f"  avg={mean(l_gaps):.1f}s  med={median(l_gaps):.1f}s  "
                      f"min={min(l_gaps):.1f}s  max={max(l_gaps):.1f}s")

    # Gap distribution table
    console.print(f"\n[bold]Gap distribution: wins vs losses[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    tbl.add_column("Gap", justify="right")
    tbl.add_column("Wins", justify="right")
    tbl.add_column("Win%", justify="right")
    tbl.add_column("Losses", justify="right")
    tbl.add_column("Loss%", justify="right")
    tbl.add_column("Win Rate", justify="right")

    tw = len(full_wins) or 1
    tl = len(full_losses) or 1
    for lo, hi, label in [
        (0, 1, "<1s"),
        (1, 2, "1-2s"),
        (2, 4, "2-4s"),
        (4, 7, "4-7s"),
        (7, 15, "7-15s"),
        (15, 60, ">15s"),
    ]:
        wn = sum(1 for r in full_wins if lo <= r["avg_gap"] < hi)
        ln = sum(1 for r in full_losses if lo <= r["avg_gap"] < hi)
        total = wn + ln
        if total == 0:
            continue
        wr = wn / total * 100
        col = "green" if wr >= 75 else "yellow" if wr >= 60 else "red"
        tbl.add_row(
            label,
            str(wn), f"{wn/tw*100:.0f}%",
            str(ln), f"{ln/tl*100:.0f}%",
            f"[{col}]{wr:.0f}%[/{col}]",
        )
    console.print(tbl)

    # Per coin
    console.print(f"\n[bold]Per coin: avg gap (wins vs losses)[/bold]")
    for coin in ["BTC", "ETH", "SOL", "XRP"]:
        cw = [r for r in full_wins if r["coin"] == coin]
        cl = [r for r in full_losses if r["coin"] == coin]
        w_str = f"avg={mean(r['avg_gap'] for r in cw):.1f}s" if cw else "n/a"
        l_str = f"avg={mean(r['avg_gap'] for r in cl):.1f}s" if cl else "n/a"
        console.print(
            f"  {coin}: wins({len(cw)}) {w_str}  |  "
            f"losses({len(cl)}) {l_str}"
        )

    # Detail: each full-fill fire
    all_full = sorted(full_wins + full_losses, key=lambda x: x["ts"])
    if all_full:
        console.print(f"\n[bold]Each full-fill fire (chronological)[/bold]")
        tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
        for col in ("Time", "Coin", "Side", "MktPx", "PatPx", "Gap",
                     "Fills", "Won", "MktSh", "PatSh"):
            tbl.add_column(col, justify="right")
        for r in all_full:
            dt = datetime.fromtimestamp(r["ts"], tz=ET)
            c = "green" if r["won"] else "red"
            avg_pat = mean(r["pat_entries"])
            tbl.add_row(
                dt.strftime("%H:%M"),
                r["coin"],
                r["side"].upper(),
                f"{r['mkt_entry']:.2f}",
                f"{avg_pat:.2f}",
                f"{r['avg_gap']:.1f}s",
                str(r["n_fills"]),
                f"[{c}]{'✓' if r['won'] else '✗'}[/{c}]",
                f"{r['mkt_shares']:.0f}",
                f"{r['pat_shares']:.0f}",
            )
        console.print(tbl)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=float, default=None,
                        help="Look back N hours (default: since 5:45am Apr 12)")
    parser.add_argument("--all", action="store_true",
                        help="All fills since Apr 11 4:40am")
    args = parser.parse_args()

    if args.all:
        since_ts = int(datetime(2026, 4, 11, 8, 40, tzinfo=timezone.utc).timestamp())
    elif args.hours:
        since_ts = int(time.time() - args.hours * 3600)
    else:
        since_ts = int(datetime(2026, 4, 12, 5, 45, tzinfo=ET).timestamp())

    since_str = datetime.fromtimestamp(since_ts, tz=ET).strftime("%Y-%m-%d %H:%M ET")
    console.print(f"[dim]Fetching fills since {since_str}…[/dim]")

    fills, results = await fetch_fills(since_ts)
    console.print(f"[bold cyan]{len(fills)} fills returned[/bold cyan]")

    if not fills:
        return

    fw, fl, mw, ml = analyze(fills, results)
    print_results(fw, fl, mw, ml)


if __name__ == "__main__":
    asyncio.run(main())
