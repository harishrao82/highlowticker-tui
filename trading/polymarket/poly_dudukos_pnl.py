#!/usr/bin/env python3
"""
poly_dudukos_pnl.py — clean P&L + pattern analysis for dudukos
(wallet 0x77c812f7735ad07e3ce58fd466a0e1b4346bbc8c)

Uses Polymarket's /positions and /value endpoints for authoritative P&L,
then cross-references with the trades API for behavioural patterns.
"""
import httpx, json
from collections import defaultdict, Counter
from datetime import datetime
from statistics import mean, median
from rich.console import Console
from rich.table import Table

WALLET = "0x77c812f7735ad07e3ce58fd466a0e1b4346bbc8c"
con = Console()

# ── 1. Account value + all positions ────────────────────────────────────────
with httpx.Client(timeout=20) as client:
    r = client.get("https://data-api.polymarket.com/value",
                   params={"user": WALLET})
    value_usd = float(r.json()[0]["value"]) if r.status_code == 200 else 0

    positions = []
    offset = 0
    while True:
        r = client.get("https://data-api.polymarket.com/positions",
                       params={"user": WALLET, "sizeThreshold": 0,
                               "limit": 500, "offset": offset})
        batch = r.json() or []
        if not batch: break
        positions.extend(batch)
        if len(batch) < 500: break
        offset += 500

    # trades for behaviour (time range + trade count per market)
    trades = []
    offset = 0
    while offset < 10_000:
        r = client.get("https://data-api.polymarket.com/trades",
                       params={"user": WALLET, "limit": 500, "offset": offset})
        batch = r.json() or []
        if not batch: break
        trades.extend(batch)
        if len(batch) < 500: break
        offset += 500

con.print(f"[bold cyan]dudukos  ({WALLET})[/bold cyan]")
con.print(f"[bold]Portfolio value:[/bold] ${value_usd:,.0f}")
con.print(f"[dim]{len(positions)} positions  •  {len(trades)} trades[/dim]\n")

# ── 2. Overall P&L ─────────────────────────────────────────────────────────
realized     = sum(float(p.get("realizedPnl")   or 0) for p in positions)
cash_pnl     = sum(float(p.get("cashPnl")       or 0) for p in positions)
unrealized   = cash_pnl - realized
total_cost   = sum(float(p.get("totalBought")   or 0) for p in positions)
cur_value    = sum(float(p.get("currentValue")  or 0) for p in positions)

con.print("[bold]P&L breakdown[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Metric","$"):
    tbl.add_column(col, justify="right")
tbl.add_row("Total bought (all positions)", f"${total_cost:,.0f}")
tbl.add_row("Current market value",         f"${cur_value:,.0f}")
tbl.add_row("[green]Realized P&L[/green]",  f"${realized:+,.0f}")
tbl.add_row("Unrealized P&L",               f"${unrealized:+,.0f}")
tbl.add_row("[bold]Total cash P&L[/bold]",  f"[{'green' if cash_pnl >= 0 else 'red'}]${cash_pnl:+,.0f}[/]")
con.print(tbl)

# ── 3. Win/loss count and size distribution ───────────────────────────────
winners = [p for p in positions if float(p["cashPnl"] or 0) > 0]
losers  = [p for p in positions if float(p["cashPnl"] or 0) < 0]
even    = [p for p in positions if float(p["cashPnl"] or 0) == 0]
con.print(f"\n[bold]Positions[/bold]  winners: {len(winners)}  losers: {len(losers)}  "
          f"flat: {len(even)}  win% {len(winners)/len(positions):.0%}")

open_pos = [p for p in positions if float(p.get("currentValue") or 0) > 1]
settled  = [p for p in positions if float(p.get("currentValue") or 0) <= 0.01]
con.print(f"  settled/at-zero: {len(settled)}  still open: {len(open_pos)}")

# ── 4. Entry price distribution ────────────────────────────────────────────
avg_prices = sorted(float(p.get("avgPrice") or 0) for p in positions)
def pct(xs, q): return xs[int(len(xs)*q)]
con.print(f"\n[bold]Avg entry price per position[/bold]  "
          f"p10 {pct(avg_prices,0.1):.3f}  "
          f"p50 {pct(avg_prices,0.5):.3f}  "
          f"p90 {pct(avg_prices,0.9):.3f}  "
          f"max {avg_prices[-1]:.3f}")
cheap = sum(1 for p in avg_prices if p < 0.1)
con.print(f"  positions entered below $0.10: {cheap}/{len(avg_prices)} "
          f"= {cheap/len(avg_prices):.0%}")

# ── 5. Top winners / losers ────────────────────────────────────────────────
def fmt_row(p):
    title = (p.get("title") or p.get("slug") or "")[:60]
    cost  = float(p.get("totalBought") or 0)
    avg   = float(p.get("avgPrice") or 0)
    cur   = float(p.get("curPrice") or 0)
    size  = float(p.get("size") or 0)
    outc  = p.get("outcome","")
    pnl   = float(p.get("cashPnl") or 0)
    real  = float(p.get("realizedPnl") or 0)
    return (title, outc, cost, avg, cur, size, real, pnl)

con.print("\n[bold]Top 10 winning positions[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,1))
for col in ("Title","Out","Cost","Avg","Cur","Size","Realized","Total P&L"):
    tbl.add_column(col, justify="right" if col != "Title" else "left")
for p in sorted(positions, key=lambda x: -float(x.get("cashPnl") or 0))[:10]:
    t, o, c, a, cp, sz, rl, pl = fmt_row(p)
    tbl.add_row(t[:48], o, f"${c:,.0f}", f"{a:.3f}", f"{cp:.3f}",
                f"{sz:,.0f}", f"${rl:+,.0f}", f"${pl:+,.0f}")
con.print(tbl)

con.print("\n[bold]Top 10 losing positions[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,1))
for col in ("Title","Out","Cost","Avg","Cur","Size","Realized","Total P&L"):
    tbl.add_column(col, justify="right" if col != "Title" else "left")
for p in sorted(positions, key=lambda x: float(x.get("cashPnl") or 0))[:10]:
    t, o, c, a, cp, sz, rl, pl = fmt_row(p)
    tbl.add_row(t[:48], o, f"${c:,.0f}", f"{a:.3f}", f"{cp:.3f}",
                f"{sz:,.0f}", f"${rl:+,.0f}", f"${pl:+,.0f}")
con.print(tbl)

# ── 6. Theme clustering ──────────────────────────────────────────────────
def theme(slug: str, title: str = "") -> str:
    s = (slug + " " + title).lower()
    if any(k in s for k in ("israel","iran","gaza","hezbollah","yemen","kharg","khamenei")):
        return "MidEast geopolitics"
    if "elon"   in s or "musk tweet" in s or "tweets" in s: return "Elon tweets"
    if "spacex" in s: return "SpaceX"
    if "cuba"   in s: return "Cuba"
    if "putin"  in s or "russia" in s or "ukraine" in s: return "Russia/Ukraine"
    if "updown" in s: return "Crypto updown"
    if "ai-"    in s or "grok" in s or "claude" in s or "gpt" in s: return "AI"
    if "trump"  in s: return "Trump"
    return "Other"

by_theme = defaultdict(list)
for p in positions:
    by_theme[theme(p.get("slug",""), p.get("title",""))].append(p)

con.print("\n[bold]P&L by theme[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Theme","N","Cost","Realized","Cash P&L","Win%","Median PnL"):
    tbl.add_column(col, justify="right")
for th in sorted(by_theme, key=lambda t: -sum(float(p.get("cashPnl") or 0) for p in by_theme[t])):
    ps = by_theme[th]
    cost = sum(float(p.get("totalBought") or 0) for p in ps)
    real = sum(float(p.get("realizedPnl") or 0) for p in ps)
    cp   = sum(float(p.get("cashPnl") or 0) for p in ps)
    wr   = sum(1 for p in ps if float(p.get("cashPnl") or 0) > 0) / len(ps)
    mp   = median(float(p.get("cashPnl") or 0) for p in ps)
    tbl.add_row(th, str(len(ps)),
                f"${cost:,.0f}", f"${real:+,.0f}", f"${cp:+,.0f}",
                f"{wr:.0%}", f"${mp:+.0f}")
con.print(tbl)

# ── 7. Time span + trade cadence ───────────────────────────────────────────
if trades:
    ts = sorted(int(t["timestamp"]) for t in trades)
    span = ts[-1] - ts[0]
    con.print(f"\n[bold]Trading window[/bold]  "
              f"{datetime.fromtimestamp(ts[0]):%Y-%m-%d %H:%M} → "
              f"{datetime.fromtimestamp(ts[-1]):%Y-%m-%d %H:%M}  "
              f"({span/86400:.1f} days)")
    sides = Counter(t.get("side") for t in trades)
    con.print(f"  BUYs: {sides.get('BUY',0)}  SELLs: {sides.get('SELL',0)}")
    counts = sorted(Counter(t.get("slug","") for t in trades).values(), reverse=True)
    if counts:
        con.print(f"  Trades per market:  median {median(counts)}  "
                  f"mean {mean(counts):.1f}  max {counts[0]}")
