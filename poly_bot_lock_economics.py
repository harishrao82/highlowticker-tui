#!/usr/bin/env python3
"""
poly_bot_lock_economics.py — decompose bot P&L into (lock portion, overhang portion).

For each resolved window:
  avg_up   = spent_up / shares_up
  avg_dn   = spent_dn / shares_down
  pair_px  = avg_up + avg_dn
  lock_n   = min(shares_up, shares_down)
  lock_pnl = lock_n * (1 - pair_px)      # guaranteed portion if paired

  overhang_n    = max - min
  overhang_side = side with more shares
  overhang_cost = avg_of_that_side * overhang_n
  overhang_pnl  = overhang_n - overhang_cost (if side == winner) else -overhang_cost

  Check: lock_pnl + overhang_pnl ≈ reported pnl
"""
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from rich.console import Console
from rich.table import Table

STATE = Path.home() / ".btc_strategy_state.json"
con   = Console()

data   = json.loads(STATE.read_text())
shadow = data["shadow"]


def coin_from_key(k: str) -> str:
    return k.split("-")[0].upper()


rows = []
for k, sh in shadow.items():
    if not sh.get("resolved"):
        continue
    su = float(sh.get("shares_up")   or 0)
    sd = float(sh.get("shares_down") or 0)
    xu = float(sh.get("spent_up")    or 0)
    xd = float(sh.get("spent_down")  or 0)
    winner = sh.get("winner")
    pnl    = float(sh.get("pnl") or 0)
    if su == 0 and sd == 0:
        continue
    avg_up = xu / su if su > 0 else None
    avg_dn = xd / sd if sd > 0 else None
    if avg_up is None or avg_dn is None:
        # one-sided window — pure directional, skip lock decomposition
        continue
    pair_px = avg_up + avg_dn
    lock_n  = min(su, sd)
    lock_pnl = lock_n * (1 - pair_px)

    if su > sd:
        oh_side = "Up"
        oh_n    = su - sd
        oh_cost = avg_up * oh_n
    else:
        oh_side = "Down"
        oh_n    = sd - su
        oh_cost = avg_dn * oh_n
    if oh_n > 0:
        oh_pnl = (oh_n - oh_cost) if oh_side == winner else -oh_cost
    else:
        oh_pnl = 0.0

    rows.append({
        "key": k, "coin": coin_from_key(k), "winner": winner,
        "shares_up": su, "shares_down": sd, "spent_up": xu, "spent_down": xd,
        "avg_up": avg_up, "avg_dn": avg_dn, "pair_px": pair_px,
        "lock_n": lock_n, "lock_pnl": lock_pnl,
        "oh_side": oh_side, "oh_n": oh_n, "oh_cost": oh_cost, "oh_pnl": oh_pnl,
        "calc_pnl": lock_pnl + oh_pnl,
        "reported_pnl": pnl,
    })

con.print(f"[bold cyan]Loaded {len(rows)} two-sided bot windows[/bold cyan]")

# ── sanity: does calc ≈ reported? ───────────────────────────────────────────
diffs = [r["calc_pnl"] - r["reported_pnl"] for r in rows]
con.print(f"[dim]calc − reported  mean {mean(diffs):+.2f}  "
          f"median {median(diffs):+.2f}  "
          f"max abs {max(abs(d) for d in diffs):.2f}[/dim]\n")


# ── 1. Aggregate split: lock vs overhang ────────────────────────────────────
total_lock = sum(r["lock_pnl"] for r in rows)
total_oh   = sum(r["oh_pnl"]   for r in rows)
total_rep  = sum(r["reported_pnl"] for r in rows)

con.print("[bold]Total P&L decomposition[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Source", "Total P&L", "% of total"):
    tbl.add_column(col, justify="right")
tbl.add_row("Lock (paired)",       f"${total_lock:+,.0f}", f"{total_lock/total_rep*100:+.0f}%")
tbl.add_row("Directional overhang",f"${total_oh:+,.0f}",   f"{total_oh/total_rep*100:+.0f}%")
tbl.add_row("[bold]Reported[/bold]",f"${total_rep:+,.0f}", "100%")
con.print(tbl)


# ── 2. Per coin ─────────────────────────────────────────────────────────────
con.print("\n[bold]Per coin decomposition[/bold]")
by_coin = defaultdict(list)
for r in rows:
    by_coin[r["coin"]].append(r)

tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Coin","N","Lock P&L","Overhang P&L","Total","Lock %","Median pair_px"):
    tbl.add_column(col, justify="right")
for coin in sorted(by_coin):
    rs = by_coin[coin]
    lp = sum(r["lock_pnl"] for r in rs)
    op = sum(r["oh_pnl"]   for r in rs)
    tp = lp + op
    mpx = median(r["pair_px"] for r in rs)
    lock_pct = lp/tp*100 if tp else 0
    tbl.add_row(coin, str(len(rs)),
                f"${lp:+,.0f}", f"${op:+,.0f}", f"${tp:+,.0f}",
                f"{lock_pct:+.0f}%", f"{mpx:.3f}")
con.print(tbl)


# ── 3. Distribution of combined pair price ─────────────────────────────────
con.print("\n[bold]Combined pair price (avg_up + avg_dn) distribution[/bold]")
prices = sorted(r["pair_px"] for r in rows)
def pct(xs, p):
    return xs[int(len(xs)*p)]
con.print(f"  p10 {pct(prices,0.10):.3f}  "
          f"p25 {pct(prices,0.25):.3f}  "
          f"p50 {pct(prices,0.50):.3f}  "
          f"p75 {pct(prices,0.75):.3f}  "
          f"p90 {pct(prices,0.90):.3f}")
arb_count = sum(1 for p in prices if p < 1.0)
con.print(f"  Windows with pair_px < $1.00 (true arb): "
          f"{arb_count}/{len(prices)} = {arb_count/len(prices):.0%}")

# histogram
con.print("\n  Histogram:")
buckets = defaultdict(int)
for p in prices:
    b = round(p * 20) / 20  # 0.05 buckets
    buckets[b] += 1
for b in sorted(buckets):
    bar = "█" * (buckets[b] * 50 // len(prices))
    marker = " [green]ARB[/green]" if b < 1.0 else " [red]loss[/red]"
    con.print(f"    {b:.2f}  {buckets[b]:4d}  {bar}{marker}")


# ── 4. Lock P&L by pair_px bucket ──────────────────────────────────────────
con.print("\n[bold]Lock P&L by pair_px bucket[/bold]  "
          "(shows expected: cheap pairs = positive, rich pairs = negative)")
pg = defaultdict(list)
for r in rows:
    b = round(r["pair_px"] * 20) / 20
    pg[b].append(r)
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("pair_px","N","Sum lock P&L","Median lock_n","Median lock $"):
    tbl.add_column(col, justify="right")
for b in sorted(pg):
    rs = pg[b]
    total_lock_pnl = sum(x["lock_pnl"] for x in rs)
    med_n   = median(x["lock_n"]   for x in rs)
    med_lp  = median(x["lock_pnl"] for x in rs)
    tbl.add_row(f"{b:.2f}", str(len(rs)),
                f"${total_lock_pnl:+,.0f}",
                f"{med_n:.0f}",
                f"${med_lp:+.1f}")
con.print(tbl)


# ── 5. Overhang contribution ────────────────────────────────────────────────
con.print("\n[bold]Overhang P&L stats[/bold]")
oh_right = sum(1 for r in rows if r["oh_n"] > 0 and r["oh_side"] == r["winner"])
oh_wrong = sum(1 for r in rows if r["oh_n"] > 0 and r["oh_side"] != r["winner"])
con.print(f"  Overhang side = winner: {oh_right}  |  "
          f"overhang side = loser: {oh_wrong}  "
          f"({oh_right/(oh_right+oh_wrong):.0%} right)")
oh_pnls = [r["oh_pnl"] for r in rows if r["oh_n"] > 0]
con.print(f"  Overhang P&L   mean ${mean(oh_pnls):+.1f}  "
          f"median ${median(oh_pnls):+.1f}  "
          f"total ${sum(oh_pnls):+,.0f}")


# ── 6. Top-10 most profitable lock windows ─────────────────────────────────
con.print("\n[bold]Top 10 windows by lock P&L[/bold]")
top = sorted(rows, key=lambda r: r["lock_pnl"], reverse=True)[:10]
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Window","Coin","Winner","pair_px","lock_n","lock P&L","oh P&L","Total"):
    tbl.add_column(col, justify="right")
for r in top:
    tbl.add_row(r["key"][-14:], r["coin"], r["winner"],
                f"{r['pair_px']:.3f}", f"{r['lock_n']:.0f}",
                f"${r['lock_pnl']:+,.0f}", f"${r['oh_pnl']:+,.0f}",
                f"${r['reported_pnl']:+,.0f}")
con.print(tbl)
