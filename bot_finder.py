#!/usr/bin/env python3
"""
bot_finder.py — Scan Polymarket 15m crypto up/down markets for active bots.

Fetches recent trades from all 15m windows, profiles each wallet by trade count,
notional, side preference, and estimated win rate (vs settlement outcome).

Usage: python3 bot_finder.py [--hours 4]
"""
import asyncio
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

ET = ZoneInfo("America/New_York")
TRADES_API = "https://data-api.polymarket.com/trades"

KNOWN_WALLETS = {
    "0xe1d6b51521bd4365769199f392f9818661bd907c": "Idolized-Scallops",
    "0x3a847382ad6fff9be1db4e073fd9b869f6884d44": "Twin-Driving-Shower",
    "0xfe787d2da716d60e8acff57fb87eb13cd4d10319": "ferrarichampions2026",
    "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30": "Bonereaper",
}

COINS = ["btc", "eth", "sol", "xrp"]
HOURS = float(sys.argv[sys.argv.index("--hours") + 1]) if "--hours" in sys.argv else 4


def window_slugs(hours: float):
    """Generate all 15m window slugs for the past N hours."""
    now = int(time.time())
    n_windows = int(hours * 4)
    slugs = []
    for coin in COINS:
        for i in range(n_windows):
            wst = (now // 900) * 900 - i * 900
            slugs.append((coin, wst, f"{coin}-updown-15m-{wst}"))
    return slugs


async def fetch_trades(client, slug, coin, wst):
    """Fetch all trades for a given market slug."""
    all_trades = []
    for offset in range(0, 1000, 100):
        try:
            r = await client.get(TRADES_API, params={
                "slug": slug, "limit": 100, "offset": offset
            })
            if r.status_code != 200:
                break
            trades = r.json()
            if not isinstance(trades, list) or not trades:
                break
            for t in trades:
                t["_coin"] = coin.upper()
                t["_wst"] = wst
            all_trades.extend(trades)
            if len(trades) < 100:
                break
        except Exception:
            break
        await asyncio.sleep(0.3)  # rate limit
    return all_trades


async def crawl():
    slugs = window_slugs(HOURS)
    print(f"Crawling {len(slugs)} market slugs ({HOURS}h × 4 coins)...", flush=True)

    all_trades = []
    async with httpx.AsyncClient(timeout=15) as client:
        # Process in batches of 8 concurrent
        for batch_start in range(0, len(slugs), 8):
            batch = slugs[batch_start:batch_start + 8]
            tasks = [fetch_trades(client, slug, coin, wst)
                     for coin, wst, slug in batch]
            results = await asyncio.gather(*tasks)
            for r in results:
                all_trades.extend(r)
            done = min(batch_start + 8, len(slugs))
            print(f"  {done}/{len(slugs)} slugs scanned, {len(all_trades):,} trades so far",
                  flush=True)

    return all_trades


def profile_wallets(trades):
    """Build per-wallet profiles from raw trades."""
    wallets = defaultdict(lambda: {
        "name": "???", "trades": 0, "notional": 0.0,
        "windows": set(), "coins": set(), "sides": Counter(),
        "prices": [], "first_trades": {},  # (coin, wst) -> first trade
    })

    for t in trades:
        wallet = (t.get("proxyWallet") or "").lower()
        if not wallet:
            continue
        side = t.get("side", "")
        if side != "BUY":
            continue
        outcome = t.get("outcome", "")
        if outcome not in ("Up", "Down"):
            continue

        w = wallets[wallet]
        if t.get("name"):
            w["name"] = t["name"]
        w["trades"] += 1
        price = float(t.get("price", 0) or 0)
        size = float(t.get("size", 0) or 0)
        w["notional"] += price * size
        coin = t["_coin"]
        wst = t["_wst"]
        w["windows"].add((coin, wst))
        w["coins"].add(coin)
        w["sides"][outcome] += 1
        w["prices"].append(price)

        key = (coin, wst)
        ts = int(t.get("timestamp", 0) or 0)
        if key not in w["first_trades"] or ts < w["first_trades"][key]["ts"]:
            w["first_trades"][key] = {
                "ts": ts, "side": outcome, "price": price
            }

    return wallets


def compute_win_rates(wallets):
    """Estimate WR by checking if first-trade side matches settlement.
    Use btc_windows.db for Kalshi settlement as proxy."""
    import sqlite3
    con = sqlite3.connect(Path.home() / ".btc_windows.db")
    kalshi_winners = {}
    for tk, wst, winner in con.execute(
            "SELECT ticker, window_start_ts, winner FROM windows WHERE winner IS NOT NULL"):
        coin = tk.split("15M")[0].replace("KX", "")
        kalshi_winners[(coin, wst)] = "Up" if winner == "yes" else "Down"
    con.close()

    for wallet, w in wallets.items():
        wins = 0
        resolved = 0
        for (coin, wst), ft in w["first_trades"].items():
            winner = kalshi_winners.get((coin, wst))
            if winner is None:
                continue
            resolved += 1
            if ft["side"] == winner:
                wins += 1
        w["resolved"] = resolved
        w["wins"] = wins
        w["wr"] = wins / resolved if resolved > 0 else None


def main():
    trades = asyncio.run(crawl())
    print(f"\nTotal trades fetched: {len(trades):,}")

    wallets = profile_wallets(trades)
    print(f"Unique wallets: {len(wallets):,}")

    compute_win_rates(wallets)

    # Rank by trade count, minimum 10 trades
    ranked = [(w_addr, w) for w_addr, w in wallets.items() if w["trades"] >= 10]
    ranked.sort(key=lambda x: -x[1]["trades"])

    print(f"\n{'#':>3} {'name':>22} {'trades':>7} {'$not':>8} {'wins':>5} "
          f"{'coins':>5} {'up%':>5} {'avg$':>6} {'WR':>6} {'tag':>12}")
    print("-" * 95)

    for i, (wallet, w) in enumerate(ranked[:30], 1):
        n_coins = len(w["coins"])
        up_pct = w["sides"]["Up"] / w["trades"] * 100 if w["trades"] else 0
        avg_price = sum(w["prices"]) / len(w["prices"]) if w["prices"] else 0
        wr_str = f"{w['wr']*100:.0f}%" if w["wr"] is not None else "—"
        tag = KNOWN_WALLETS.get(wallet, "")
        n_windows = len(w["windows"])

        print(f"{i:>3} {w['name']:>22} {w['trades']:>7,} ${w['notional']:>7,.0f} "
              f"{n_windows:>5} {n_coins:>5} {up_pct:>4.0f}% {avg_price:>6.3f} "
              f"{wr_str:>6} {tag:>12}")

    print(f"\n=== TOP 5 NEW (not in known list) ===\n")
    new_bots = [(w_addr, w) for w_addr, w in ranked if w_addr not in KNOWN_WALLETS]
    for i, (wallet, w) in enumerate(new_bots[:5], 1):
        wr_str = f"{w['wr']*100:.0f}%" if w["wr"] is not None else "—"
        print(f"{i}. {w['name']}  (WR={wr_str}, {w['trades']} trades, "
              f"${w['notional']:,.0f}, {len(w['coins'])} coins)")
        print(f"   https://polymarket.com/profile/{wallet}")
        print()


if __name__ == "__main__":
    main()
