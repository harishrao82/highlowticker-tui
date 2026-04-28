#!/usr/bin/env python3
"""
kalshi_session_report.py — pull all real Kalshi fills since a given timestamp,
resolve each market, compute realized P&L, and summarize by coin and checkpoint.
"""
import asyncio
import base64
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()
con = Console()

BASE = "https://api.elections.kalshi.com/trade-api/v2"
api_key = os.environ["KALSHI_API_KEY"]
pk = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

# Report all fills since this moment (April 11, 2026 4:40 AM ET == 08:40 UTC)
MIN_TS = int(datetime(2026, 4, 11, 8, 40, tzinfo=timezone.utc).timestamp())


def sign(method: str, path: str) -> dict:
    ts = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = pk.sign(
        msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def coin_of(ticker: str) -> str:
    # KXBTC15M-... → BTC
    return ticker[2:5]


def window_start_from_ticker(ticker: str) -> int | None:
    """Parse KXBTC15M-26APR110815-15 → unix timestamp of 2026-04-11 08:15 ET."""
    try:
        _, date_part, _ = ticker.split("-")
        yy = 2000 + int(date_part[:2])
        month = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
                 "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}[date_part[2:5]]
        day    = int(date_part[5:7])
        hour   = int(date_part[7:9])
        minute = int(date_part[9:11])
        # Close time ET; window starts 15m before
        from datetime import timedelta
        et = timezone(timedelta(hours=-4))
        close_dt = datetime(yy, month, day, hour, minute, tzinfo=et)
        start_dt = close_dt - timedelta(minutes=15)
        return int(start_dt.timestamp())
    except Exception:
        return None


async def fetch_all_fills(client: httpx.AsyncClient) -> list[dict]:
    fills = []
    cursor = ""
    for _ in range(20):
        params = {"min_ts": MIN_TS, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        r = await client.get(
            BASE + "/portfolio/fills",
            headers=sign("GET", "/trade-api/v2/portfolio/fills"),
            params=params, timeout=15,
        )
        if r.status_code != 200:
            con.print(f"[red]fills {r.status_code}: {r.text[:200]}[/red]")
            break
        j = r.json()
        batch = j.get("fills", [])
        if not batch:
            break
        fills.extend(batch)
        cursor = j.get("cursor", "")
        if not cursor:
            break
    return fills


async def fetch_result(client: httpx.AsyncClient, ticker: str) -> str:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(
            BASE + f"/markets/{ticker}",
            headers=sign("GET", path), timeout=8,
        )
        if r.status_code == 200:
            return r.json().get("market", {}).get("result", "") or ""
    except Exception:
        pass
    return ""


async def main():
    async with httpx.AsyncClient() as client:
        con.print(f"[dim]Fetching fills since "
                  f"{datetime.fromtimestamp(MIN_TS, tz=timezone.utc):%Y-%m-%d %H:%M UTC}"
                  f"  ({datetime.fromtimestamp(MIN_TS):%H:%M local})[/dim]")
        fills = await fetch_all_fills(client)
        con.print(f"[bold cyan]{len(fills)} fills returned[/bold cyan]\n")
        if not fills:
            return

        # Unique markets for result lookup
        unique_tickers = sorted({f["ticker"] for f in fills})
        con.print(f"[dim]Resolving {len(unique_tickers)} unique markets…[/dim]")
        results: dict[str, str] = {}
        for t in unique_tickers:
            results[t] = await fetch_result(client, t)

    # Enrich fills with resolution + P&L
    enriched = []
    for f in fills:
        ticker = f["ticker"]
        side = f["side"]            # "yes" or "no"
        action = f["action"]        # "buy"/"sell"
        shares = float(f["count_fp"])
        yes_px = float(f["yes_price_dollars"])
        no_px  = float(f["no_price_dollars"])
        fee    = float(f.get("fee_cost", 0) or 0)
        ts     = int(f["ts"])
        coin   = coin_of(ticker)
        wts    = window_start_from_ticker(ticker)
        elapsed = (ts - wts) if wts else None
        result = results.get(ticker, "")

        # Our entry cost per share (what we paid for this side)
        entry_px = yes_px if side == "yes" else no_px
        won = None
        pnl = None
        if result in ("yes", "no"):
            won = (side == result)
            if action == "buy":
                pnl = (1.0 - entry_px) * shares - fee if won else -entry_px * shares - fee
            else:
                pnl = -((1.0 - entry_px) * shares + fee) if won else (entry_px * shares - fee)

        enriched.append({
            "ticker": ticker, "coin": coin, "side": side, "action": action,
            "shares": shares, "entry_px": entry_px, "fee": fee,
            "ts": ts, "elapsed": elapsed, "result": result,
            "won": won, "pnl": pnl,
        })

    # ── Overall ────────────────────────────────────────────────────────────
    resolved = [e for e in enriched if e["pnl"] is not None]
    pending  = [e for e in enriched if e["pnl"] is None]
    total_pnl = sum(e["pnl"] for e in resolved) if resolved else 0.0
    wins = sum(1 for e in resolved if e["won"])
    staked = sum(e["entry_px"] * e["shares"] for e in resolved)
    fees_total = sum(e["fee"] for e in resolved)

    con.print(f"[bold]Overall[/bold]")
    con.print(f"  fills:        {len(enriched)}")
    con.print(f"  resolved:     {len(resolved)}  (pending: {len(pending)})")
    if resolved:
        pct = wins / len(resolved) * 100
        win_col = "green" if pct >= 50 else "red"
        pnl_col = "green" if total_pnl >= 0 else "red"
        con.print(f"  win rate:     [{win_col}]{wins}/{len(resolved)} = {pct:.0f}%[/{win_col}]")
        con.print(f"  total staked: ${staked:.2f}")
        con.print(f"  fees:         ${fees_total:.2f}")
        con.print(f"  [bold]total P&L:    [{pnl_col}]${total_pnl:+.2f}[/{pnl_col}][/bold]")
        if staked > 0:
            con.print(f"  return:       {total_pnl/staked*100:+.1f}% of staked")

    # ── Per coin ───────────────────────────────────────────────────────────
    con.print(f"\n[bold]Per coin[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","Fills","Resolved","Wins","Win%","Staked","P&L","Return%"):
        tbl.add_column(col, justify="right")
    by_coin = defaultdict(list)
    for e in enriched:
        by_coin[e["coin"]].append(e)
    for coin in sorted(by_coin):
        es = by_coin[coin]
        res = [e for e in es if e["pnl"] is not None]
        wins_c = sum(1 for e in res if e["won"])
        pnl_c = sum(e["pnl"] for e in res)
        staked_c = sum(e["entry_px"]*e["shares"] for e in res)
        ret = (pnl_c/staked_c*100) if staked_c else 0
        pct = (wins_c/len(res)*100) if res else 0
        col = "green" if pnl_c > 0 else "red" if pnl_c < 0 else "white"
        tbl.add_row(
            coin, str(len(es)), str(len(res)), str(wins_c),
            f"{pct:.0f}%",
            f"${staked_c:.2f}",
            f"[{col}]${pnl_c:+.2f}[/{col}]",
            f"{ret:+.1f}%",
        )
    con.print(tbl)

    # ── By checkpoint / elapsed bucket ────────────────────────────────────
    con.print(f"\n[bold]Entries by elapsed-time bucket[/bold]")
    buckets = [(0,60),(60,120),(120,180),(180,300),(300,450),(450,600),(600,900)]
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("T range","Fills","Resolved","Wins","Win%","Avg entry","P&L"):
        tbl.add_column(col, justify="right")
    for lo, hi in buckets:
        es = [e for e in enriched if e["elapsed"] is not None and lo <= e["elapsed"] < hi]
        if not es: continue
        res = [e for e in es if e["pnl"] is not None]
        wins_b = sum(1 for e in res if e["won"])
        pnl_b = sum(e["pnl"] for e in res)
        pct = (wins_b/len(res)*100) if res else 0
        avg_px = mean(e["entry_px"] for e in es)
        col = "green" if pnl_b > 0 else "red" if pnl_b < 0 else "white"
        tbl.add_row(
            f"{lo}-{hi}s", str(len(es)), str(len(res)), str(wins_b),
            f"{pct:.0f}%",
            f"{avg_px:.2f}",
            f"[{col}]${pnl_b:+.2f}[/{col}]",
        )
    con.print(tbl)

    # ── Each fill (chronological) ─────────────────────────────────────────
    con.print(f"\n[bold]All fills (chronological)[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,1))
    for col in ("Time ET","Coin","T","Side","Px","Shares","Result","P&L"):
        tbl.add_column(col, justify="right")
    from datetime import timedelta
    ET = timezone(timedelta(hours=-4))
    for e in sorted(enriched, key=lambda x: x["ts"]):
        dt_et = datetime.fromtimestamp(e["ts"], tz=ET)
        t_str = dt_et.strftime("%H:%M:%S")
        elapsed_str = f"+{e['elapsed']:>4}s" if e["elapsed"] is not None else "   —"
        pnl_str = "—"
        pnl_col = "white"
        if e["pnl"] is not None:
            pnl_str = f"${e['pnl']:+.2f}"
            pnl_col = "green" if e["pnl"] >= 0 else "red"
        result_str = e["result"].upper() if e["result"] else "pending"
        res_col = "green" if (e["won"] is True) else "red" if (e["won"] is False) else "yellow"
        side_col = "cyan" if e["side"] == "yes" else "magenta"
        tbl.add_row(
            t_str, e["coin"], elapsed_str,
            f"[{side_col}]{e['side'].upper()}[/{side_col}]",
            f"{e['entry_px']:.2f}",
            f"{e['shares']:.0f}",
            f"[{res_col}]{result_str}[/{res_col}]",
            f"[{pnl_col}]{pnl_str}[/{pnl_col}]",
        )
    con.print(tbl)


if __name__ == "__main__":
    asyncio.run(main())
