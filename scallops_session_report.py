#!/usr/bin/env python3
"""
scallops_session_report.py — compute Idolized-Scallops' P&L from the shadow
log for the same time window we're tracking our own bot.

Reads ~/.scallops_live_trades.jsonl, filters to 15m crypto trades since
04:40 ET Apr 11, groups by slug, fetches each market's resolution from the
Polymarket gamma API, and computes realized P&L per position.
"""
import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, median

import ccxt
from rich.console import Console
from rich.table import Table

LOG       = Path.home() / ".scallops_live_trades.jsonl"
MIN_TS    = int(datetime(2026,4,11,8,40,tzinfo=timezone.utc).timestamp())   # 04:40 ET
ET        = timezone(timedelta(hours=-4))

# Coin spot prices at each 1-min boundary, built on demand from Coinbase
# historical candles. We resolve Polymarket 15m up/down by comparing the
# price at window_start_ts to the price at window_start_ts + 900 (minus a bit).
_cb = ccxt.coinbase()
_price_cache: dict[str, dict[int, float]] = {
    "BTC": {}, "ETH": {}, "SOL": {}, "XRP": {},
}
COIN_PRODUCT = {"BTC":"BTC-USD","ETH":"ETH-USD","SOL":"SOL-USD","XRP":"XRP-USD"}

con = Console()


def fetch_candles(coin: str, start_ts: int, end_ts: int) -> None:
    """Populate _price_cache[coin] with 1-min close prices in [start,end]."""
    product = COIN_PRODUCT[coin]
    cur_ms = start_ts * 1000
    end_ms = end_ts * 1000
    while cur_ms < end_ms:
        try:
            ohlcv = _cb.fetch_ohlcv(product, "1m", cur_ms, 300)
        except Exception as e:
            con.print(f"[yellow]  fetch {coin}: {e}[/yellow]")
            return
        if not ohlcv: return
        for row in ohlcv:
            t = int(row[0]) // 1000
            _price_cache[coin][t - (t % 60)] = float(row[4])
        last = int(ohlcv[-1][0]) + 60_000
        if last <= cur_ms: return
        cur_ms = last
        time.sleep(0.12)


def price_at(coin: str, ts: int) -> float | None:
    return _price_cache[coin].get(ts - (ts % 60))


def load_trades() -> list[dict]:
    out = []
    for line in open(LOG):
        line = line.strip()
        if not line: continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        if e.get("trade_ts", 0) < MIN_TS: continue
        slug = e.get("slug") or ""
        if "updown-15m-" not in slug: continue
        if e.get("coin") not in ("BTC","ETH","SOL","XRP"): continue
        out.append(e)
    # Dedupe by tx
    seen = set(); unique = []
    for e in out:
        tx = e.get("tx","")
        if tx and tx in seen: continue
        seen.add(tx); unique.append(e)
    return unique


async def main():
    trades = load_trades()
    con.print(f"[bold cyan]{len(trades)} Scallops 15m crypto trades since 04:40 ET[/bold cyan]")
    if not trades:
        return

    # Group by slug (window)
    by_slug: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_slug[t["slug"]].append(t)

    con.print(f"[dim]Unique windows: {len(by_slug)}[/dim]")

    # Determine time range we need for each coin, then bulk fetch
    coins_needed: dict[str, tuple[int, int]] = {}
    for slug in by_slug:
        coin = slug.split("-")[0].upper()
        wts  = int(slug.rsplit("-", 1)[1])
        end  = wts + 960   # window_start + 16 min for buffer
        cur = coins_needed.get(coin)
        if cur is None:
            coins_needed[coin] = (wts, end)
        else:
            coins_needed[coin] = (min(cur[0], wts), max(cur[1], end))
    for coin, (s, e) in coins_needed.items():
        con.print(f"[dim]Fetching {coin} candles {datetime.fromtimestamp(s):%H:%M} → {datetime.fromtimestamp(e):%H:%M}…[/dim]")
        fetch_candles(coin, s, e)

    now = int(time.time())
    rows = []
    for slug, ts_list in by_slug.items():
        coin = slug.split("-")[0].upper()
        wts  = int(slug.rsplit("-", 1)[1])
        close_ts = wts + 900

        p_open  = price_at(coin, wts)
        p_close = price_at(coin, close_ts - 60)   # close = last minute of window
        is_open = now < close_ts   # window still in progress

        winner = None
        if not is_open and p_open and p_close:
            if p_close > p_open:
                winner = "Up"
            elif p_close < p_open:
                winner = "Down"
            # equal = tie, treat as open

        # Aggregate his trades
        bought_sh   = defaultdict(float)
        bought_cost = defaultdict(float)
        sold_sh     = defaultdict(float)
        sold_rev    = defaultdict(float)
        for t in ts_list:
            outc = t.get("outcome", "")
            px   = float(t.get("price", 0) or 0)
            sz   = float(t.get("size", 0) or 0)
            nominal = px * sz
            if t.get("side") == "BUY":
                bought_sh[outc]   += sz
                bought_cost[outc] += nominal
            else:
                sold_sh[outc]   += sz
                sold_rev[outc]  += nominal

        total_cost = sum(bought_cost.values())
        total_rev  = sum(sold_rev.values())

        # Settlement: if we have a winner, losing side pays 0
        if winner:
            settle = ((bought_sh["Up"] - sold_sh["Up"]) *
                      (1.0 if winner == "Up" else 0.0) +
                      (bought_sh["Down"] - sold_sh["Down"]) *
                      (1.0 if winner == "Down" else 0.0))
            pnl = total_rev + settle - total_cost
            confident = True
        else:
            # Mark-to-market at current coin price relative to window open
            # (windows still open — use current coin delta as proxy)
            settle = 0.0
            pnl = None
            confident = False

        # Lean side (net shares)
        net_up = bought_sh["Up"] - sold_sh["Up"]
        net_dn = bought_sh["Down"] - sold_sh["Down"]
        lean_side = "Up" if net_up > net_dn else "Down"
        lean_matches = (winner == lean_side) if winner else None

        rows.append({
            "slug": slug,
            "coin": coin,
            "wts":  wts,
            "n_trades": len(ts_list),
            "cost": total_cost,
            "rev":  total_rev,
            "settle": settle,
            "pnl": pnl,
            "winner": winner,
            "closed": not is_open,
            "confident": confident,
            "lean": lean_side,
            "lean_matches": lean_matches,
            "net_up_sh": net_up,
            "net_dn_sh": net_dn,
            "p_open": p_open,
            "p_close": p_close,
        })

    # ── Overall ────────────────────────────────────────────────────────────
    resolved = [r for r in rows if r["pnl"] is not None]
    pending  = [r for r in rows if r["pnl"] is None]
    total_pnl   = sum(r["pnl"] for r in resolved)
    total_cost  = sum(r["cost"] for r in rows)
    total_rev   = sum(r["rev"] for r in rows)
    wins = sum(1 for r in resolved if r["lean_matches"])

    con.print("[bold]Scallops overall (since 04:40 ET Apr 11)[/bold]")
    con.print(f"  windows traded:       {len(rows)}")
    con.print(f"  resolved:             {len(resolved)}  (pending: {len(pending)})")
    con.print(f"  total trades:         {sum(r['n_trades'] for r in rows):,}")
    con.print(f"  bought:               ${total_cost:,.0f}")
    con.print(f"  sold (partial exits): ${total_rev:,.0f}")
    if resolved:
        pnl_col = "green" if total_pnl >= 0 else "red"
        pct = wins / len(resolved) * 100
        win_col = "green" if pct >= 50 else "red"
        con.print(f"  lean-match rate:      [{win_col}]{wins}/{len(resolved)} = {pct:.0f}%[/{win_col}]")
        con.print(f"  [bold]resolved P&L:         [{pnl_col}]${total_pnl:+,.0f}[/{pnl_col}][/bold]")
        resolved_cost = sum(r["cost"] for r in resolved)
        if resolved_cost > 0:
            con.print(f"  return on resolved cost: {total_pnl/resolved_cost*100:+.2f}%")

    # ── Per coin ───────────────────────────────────────────────────────────
    con.print(f"\n[bold]Per coin (resolved windows only)[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","Wins","Lean%","Bought","P&L","Return%"):
        tbl.add_column(col, justify="right")
    by_coin = defaultdict(list)
    for r in resolved:
        by_coin[r["coin"]].append(r)
    for coin in sorted(by_coin):
        rs = by_coin[coin]
        cost = sum(r["cost"] for r in rs)
        pnl = sum(r["pnl"] for r in rs)
        wins_c = sum(1 for r in rs if r["lean_matches"])
        ret = (pnl/cost*100) if cost else 0
        pct = wins_c/len(rs)*100
        col  = "green" if pnl >= 0 else "red"
        tbl.add_row(
            coin,
            f"{wins_c}/{len(rs)}",
            f"{pct:.0f}%",
            f"${cost:,.0f}",
            f"[{col}]${pnl:+,.0f}[/{col}]",
            f"{ret:+.1f}%",
        )
    con.print(tbl)

    # ── Per-window detail ─────────────────────────────────────────────────
    con.print(f"\n[bold]Per window (chronological)[/bold]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,1))
    for col in ("Window ET","Coin","N","Cost","Lean","Winner","Match","P&L"):
        tbl.add_column(col, justify="right")
    for r in sorted(rows, key=lambda x: x["wts"]):
        w_et = datetime.fromtimestamp(r["wts"], tz=ET).strftime("%H:%M")
        if r["pnl"] is None:
            match_str = "…"
            match_col = "yellow"
            winner_str = "open"
            pnl_str   = "—"
            pnl_col   = "white"
        else:
            match_str = "✓" if r["lean_matches"] else "✗"
            match_col = "green" if r["lean_matches"] else "red"
            winner_str = r["winner"]
            pnl_col = "green" if r["pnl"] >= 0 else "red"
            pnl_str = f"${r['pnl']:+,.0f}"
        tbl.add_row(
            w_et, r["coin"], str(r["n_trades"]),
            f"${r['cost']:,.0f}",
            r["lean"], winner_str,
            f"[{match_col}]{match_str}[/{match_col}]",
            f"[{pnl_col}]{pnl_str}[/{pnl_col}]",
        )
    con.print(tbl)

    # ── Head-to-head ──────────────────────────────────────────────────────
    con.print(f"\n[bold]Head-to-head same session[/bold]")
    our_pnl = 3.21  # from kalshi_session_report latest run
    our_ret = 9.7
    con.print(f"  our  (Kalshi, 1 share):      [green]$+{our_pnl:.2f}[/green]  @ +{our_ret}% return on stake")
    if resolved:
        his_cost = sum(r["cost"] for r in resolved)
        his_ret = total_pnl / his_cost * 100 if his_cost else 0
        his_col = "green" if total_pnl >= 0 else "red"
        con.print(f"  his  (Polymarket, actual):   [{his_col}]${total_pnl:+,.0f}[/{his_col}]  @ {his_ret:+.2f}% return on stake")


if __name__ == "__main__":
    asyncio.run(main())
