#!/usr/bin/env python3
"""
poly_scientific_impala.py — characterize trader 0x04283f2fef49d70d8c55ab240450d17a65bf85b
(proxy wallet 0x07d126ea6f8542a58ee2c4c7c31535dd9b3e8ffa, pseudonym Scientific-Impala)
"""
import httpx, json, time
from collections import Counter, defaultdict
from datetime import datetime
from statistics import mean, median
from rich.console import Console
from rich.table import Table

PROFILE_WALLET = "0x04283f2fef49d70d8c55ab240450d17a65bf85b"
PROXY_WALLET   = "0x07d126ea6f8542a58ee2c4c7c31535dd9b3e8ffa"
con = Console()

# ── Pull as many trades as we can ───────────────────────────────────────────
trades: list[dict] = []
offset = 0
LIMIT  = 500
with httpx.Client(timeout=15) as client:
    while True:
        # try profile wallet first, then proxy
        for w in (PROFILE_WALLET, PROXY_WALLET):
            r = client.get(
                "https://data-api.polymarket.com/trades",
                params={"user": w, "limit": LIMIT, "offset": offset},
            )
            if r.status_code == 200 and r.json():
                trades.extend(r.json())
                break
        else:
            break
        if len(r.json()) < LIMIT:
            break
        offset += LIMIT
        if offset > 10_000:   # safety cap
            break

con.print(f"[bold cyan]Pulled {len(trades)} trades[/bold cyan]")
if not trades:
    raise SystemExit("no trades")

# dedupe by tx hash
seen = set()
uniq = []
for t in trades:
    h = t.get("transactionHash")
    if h and h not in seen:
        seen.add(h); uniq.append(t)
trades = uniq
con.print(f"[dim]unique by tx: {len(trades)}[/dim]")

# ── 1. Side distribution (BUY vs SELL) ─────────────────────────────────────
sides = Counter(t.get("side") for t in trades)
con.print(f"\n[bold]BUY/SELL[/bold]  {dict(sides)}")

# ── 2. Market families ─────────────────────────────────────────────────────
def market_family(slug: str) -> str:
    if "updown-5m"  in slug: return "updown-5m"
    if "updown-15m" in slug: return "updown-15m"
    if "updown-1h"  in slug: return "updown-1h"
    return slug.split("-")[0] + "-" + (slug.split("-")[1] if "-" in slug else "?")

fams = Counter(market_family(t.get("slug", "")) for t in trades)
con.print(f"\n[bold]Market families[/bold]")
for f, n in fams.most_common(10):
    con.print(f"  {f:20s}  {n}")

# ── 3. Coins traded ────────────────────────────────────────────────────────
def coin_of(slug: str) -> str:
    return slug.split("-")[0].upper() if slug else "?"
coins = Counter(coin_of(t.get("slug","")) for t in trades)
con.print(f"\n[bold]Coins[/bold]  {dict(coins.most_common(10))}")

# ── 4. Trades per window (slug) ────────────────────────────────────────────
by_slug = defaultdict(list)
for t in trades:
    by_slug[t.get("slug","")].append(t)
counts = sorted([len(v) for v in by_slug.values()], reverse=True)
con.print(f"\n[bold]Windows traded[/bold]  {len(by_slug)}  "
          f"trades/window median={median(counts)}  mean={mean(counts):.1f}  "
          f"max={counts[0]}")

# ── 5. Notional volume + side split per coin ───────────────────────────────
con.print(f"\n[bold]Notional by coin & side[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Coin","Buys $","Sells $","Net buy $","BuyN","SellN"):
    tbl.add_column(col, justify="right")
vol = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "buy_n": 0, "sell_n": 0})
for t in trades:
    coin = coin_of(t.get("slug",""))
    notional = float(t.get("price",0)) * float(t.get("size",0))
    if t.get("side") == "BUY":
        vol[coin]["buy"]  += notional; vol[coin]["buy_n"]  += 1
    else:
        vol[coin]["sell"] += notional; vol[coin]["sell_n"] += 1
for coin in sorted(vol, key=lambda c: -vol[c]["buy"]-vol[c]["sell"]):
    v = vol[coin]
    tbl.add_row(coin, f"${v['buy']:,.0f}", f"${v['sell']:,.0f}",
                f"${v['buy']-v['sell']:+,.0f}",
                str(v['buy_n']), str(v['sell_n']))
con.print(tbl)

# ── 6. Time range & trade frequency ────────────────────────────────────────
times = sorted(int(t.get("timestamp",0)) for t in trades)
if len(times) >= 2:
    span = times[-1] - times[0]
    con.print(f"\n[bold]Time span[/bold]  "
              f"{datetime.fromtimestamp(times[0]):%Y-%m-%d %H:%M} → "
              f"{datetime.fromtimestamp(times[-1]):%Y-%m-%d %H:%M}  "
              f"({span/3600:.1f} hours)  rate {len(trades)/(span/3600):.0f} trades/hr")

# ── 7. Round-trip analysis: does each window have both BUY and SELL? ──────
with_both = 0; only_buy = 0; only_sell = 0
for slug, ts in by_slug.items():
    buys  = [t for t in ts if t.get("side") == "BUY"]
    sells = [t for t in ts if t.get("side") == "SELL"]
    if buys and sells:
        with_both += 1
    elif buys:
        only_buy += 1
    else:
        only_sell += 1
con.print(f"\n[bold]Per-window composition[/bold]  "
          f"both buy+sell: {with_both}  only buy: {only_buy}  only sell: {only_sell}")

# ── 8. Buy price distribution by outcome (Up/Down) ─────────────────────────
con.print(f"\n[bold]Buy prices by outcome[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Outcome","N","Mean px","Median px","Min","Max"):
    tbl.add_column(col, justify="right")
for outc in ("Up","Down"):
    prices = [float(t.get("price",0)) for t in trades
              if t.get("side")=="BUY" and t.get("outcome")==outc]
    if not prices: continue
    tbl.add_row(outc, str(len(prices)), f"{mean(prices):.3f}",
                f"{median(prices):.3f}", f"{min(prices):.3f}", f"{max(prices):.3f}")
con.print(tbl)

# sell prices
con.print(f"\n[bold]Sell prices by outcome[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Outcome","N","Mean px","Median px","Min","Max"):
    tbl.add_column(col, justify="right")
for outc in ("Up","Down"):
    prices = [float(t.get("price",0)) for t in trades
              if t.get("side")=="SELL" and t.get("outcome")==outc]
    if not prices: continue
    tbl.add_row(outc, str(len(prices)), f"{mean(prices):.3f}",
                f"{median(prices):.3f}", f"{min(prices):.3f}", f"{max(prices):.3f}")
con.print(tbl)

# ── 9. A few recent windows in detail ──────────────────────────────────────
con.print(f"\n[bold]5 most recent windows in detail[/bold]")
recent_slugs = sorted(
    by_slug.keys(),
    key=lambda s: max(int(t.get("timestamp",0)) for t in by_slug[s]),
    reverse=True,
)[:5]
for slug in recent_slugs:
    ts_list = sorted(by_slug[slug], key=lambda t: int(t.get("timestamp",0)))
    first_ts = int(ts_list[0]["timestamp"])
    # parse window start from slug (last number chunk)
    try:
        w_start = int(slug.rsplit("-",1)[1])
    except Exception:
        w_start = first_ts
    title = ts_list[0].get("title","")[:60]
    con.print(f"\n[cyan]{slug}[/cyan]  [dim]{title}[/dim]")
    con.print(f"  w_start={datetime.fromtimestamp(w_start):%H:%M:%S}  "
              f"{len(ts_list)} trades")
    for t in ts_list[:12]:
        elapsed = int(t["timestamp"]) - w_start
        side = t.get("side","")
        outc = t.get("outcome","")
        px   = float(t.get("price",0))
        sz   = float(t.get("size",0))
        notional = px * sz
        col  = "green" if side=="BUY" else "red"
        con.print(f"    +{elapsed:4d}s  [{col}]{side:<4}[/{col}]  "
                  f"{outc:<4}  @{px:.3f}  sz={sz:.1f}  ${notional:.0f}")
    if len(ts_list) > 12:
        con.print(f"    ... {len(ts_list)-12} more")
