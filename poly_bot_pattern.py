#!/usr/bin/env python3
"""
poly_bot_pattern.py — mine Idolized-Scallops' trading pattern.

Data: ~/.btc_strategy_state.json -> shadow{} (per-window bot trades)
"""
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from rich.console import Console
from rich.table import Table

STATE = Path.home() / ".btc_strategy_state.json"
con   = Console()

data = json.loads(STATE.read_text())
positions = data["positions"]   # our trades per window
shadow    = data["shadow"]      # bot trades per window


def coin_from_key(k: str) -> str:
    return k.split("-")[0].upper()


def window_start_ts(k: str) -> int:
    return int(k.rsplit("-", 1)[1])


def elapsed_sec(trade_time: str, w_start_ts: int) -> int | None:
    """Convert HH:MM:SS + window_start_ts into seconds since window open."""
    try:
        h, m, s = map(int, trade_time.split(":"))
    except Exception:
        return None
    start_dt = datetime.fromtimestamp(w_start_ts)
    day      = start_dt.replace(hour=h, minute=m, second=s)
    delta    = (day - start_dt).total_seconds()
    if delta < -3600:   # wrapped past midnight
        delta += 86400
    if delta < -60 or delta > 1800:
        return None
    return int(delta)


# ── Join positions ↔ shadow on window key ──────────────────────────────────
joined = []
for k, sh in shadow.items():
    if not sh.get("resolved"): continue
    trades = sh.get("trades") or []
    if not trades: continue
    winner = sh.get("winner")
    pnl    = float(sh.get("pnl") or 0)
    spent  = float(sh.get("spent_up") or 0) + float(sh.get("spent_down") or 0)
    ret    = (pnl / spent) if spent > 0 else 0
    wts    = window_start_ts(k)
    # normalize sides to 'Up'/'Down'
    trades_s = sorted(
        [
            {**t, "elapsed": elapsed_sec(t["time"], wts)}
            for t in trades
            if "time" in t
        ],
        key=lambda t: t["elapsed"] if t["elapsed"] is not None else 0,
    )
    trades_s = [t for t in trades_s if t["elapsed"] is not None]
    if not trades_s: continue
    joined.append({
        "key":    k,
        "coin":   coin_from_key(k),
        "winner": winner,
        "pnl":    pnl,
        "spent":  spent,
        "ret":    ret,
        "shares_up":   float(sh.get("shares_up") or 0),
        "shares_down": float(sh.get("shares_down") or 0),
        "trades": trades_s,
    })

con.print(f"[bold cyan]Loaded {len(joined)} resolved bot windows[/bold cyan]\n")


# ── 1. Overall P&L ──────────────────────────────────────────────────────────
total_pnl   = sum(w["pnl"]   for w in joined)
total_spent = sum(w["spent"] for w in joined)
win_count   = sum(1 for w in joined if w["pnl"] > 0)
con.print(f"[bold]Overall[/bold]  total P&L ${total_pnl:+,.0f}  "
          f"total spent ${total_spent:,.0f}  "
          f"return {total_pnl/total_spent*100:+.2f}%  "
          f"win rate {win_count/len(joined):.0%}")


# ── 2. Per coin ─────────────────────────────────────────────────────────────
by_coin = defaultdict(list)
for w in joined:
    by_coin[w["coin"]].append(w)

con.print("\n[bold]Per coin[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Coin", "Windows", "Win%", "Total P&L", "Spent", "Return%", "Median PnL/win"):
    tbl.add_column(col, justify="right")
for coin in sorted(by_coin):
    ws = by_coin[coin]
    pnl = sum(w["pnl"] for w in ws)
    sp  = sum(w["spent"] for w in ws)
    wr  = sum(1 for w in ws if w["pnl"] > 0) / len(ws)
    mp  = median(w["pnl"] for w in ws)
    tbl.add_row(coin, str(len(ws)), f"{wr:.0%}", f"${pnl:+,.0f}",
                f"${sp:,.0f}", f"{(pnl/sp*100) if sp else 0:+.2f}%",
                f"${mp:+.1f}")
con.print(tbl)


# ── 3. First trade timing (seconds into window) ─────────────────────────────
first_elapsed = [w["trades"][0]["elapsed"] for w in joined]
con.print(f"\n[bold]First trade elapsed[/bold]  "
          f"median {median(first_elapsed)}s  "
          f"mean {mean(first_elapsed):.0f}s  "
          f"min {min(first_elapsed)}s  max {max(first_elapsed)}s")

# histogram
buckets = Counter()
for e in first_elapsed:
    b = (e // 30) * 30
    buckets[b] += 1
con.print("  Histogram (30s buckets):")
for b in sorted(buckets):
    bar = "█" * (buckets[b] * 40 // len(first_elapsed))
    con.print(f"    {b:4d}–{b+29:<4d}s  {buckets[b]:4d}  {bar}")


# ── 4. Trades per window ────────────────────────────────────────────────────
counts = [len(w["trades"]) for w in joined]
con.print(f"\n[bold]Trades per window[/bold]  "
          f"median {median(counts)}  mean {mean(counts):.1f}  "
          f"min {min(counts)}  max {max(counts)}")


# ── 5. First-trade side vs winner ───────────────────────────────────────────
first_right = sum(1 for w in joined if w["trades"][0]["side"] == w["winner"])
con.print(f"\n[bold]First trade = winner side[/bold]  "
          f"{first_right}/{len(joined)} = {first_right/len(joined):.1%}")


# ── 6. Does bot hold both sides or one side at expiry? ─────────────────────
both_sides = sum(1 for w in joined if w["shares_up"] > 0 and w["shares_down"] > 0)
only_up    = sum(1 for w in joined if w["shares_up"] > 0 and w["shares_down"] == 0)
only_dn    = sum(1 for w in joined if w["shares_up"] == 0 and w["shares_down"] > 0)
con.print(f"\n[bold]Exit composition[/bold]  "
          f"both sides: {both_sides}  only Up: {only_up}  only Down: {only_dn}")


# ── 7. First-trade cost distribution ────────────────────────────────────────
first_costs = sorted(w["trades"][0]["cost"] for w in joined)
def pct(xs, p):
    return xs[int(len(xs) * p)]
con.print(f"\n[bold]First trade cost $[/bold]  "
          f"p10 {pct(first_costs,0.1):.0f}  "
          f"p50 {pct(first_costs,0.5):.0f}  "
          f"p90 {pct(first_costs,0.9):.0f}  "
          f"max {first_costs[-1]:.0f}")


# ── 8. PnL by first-trade entry price bucket ────────────────────────────────
con.print(f"\n[bold]P&L grouped by first-trade price[/bold]")
pg = defaultdict(list)
for w in joined:
    p = w["trades"][0]["price"]
    bucket = round(p * 10) / 10  # 0.1 bucket
    pg[bucket].append(w["pnl"])
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("First price", "N", "Mean PnL", "Median PnL", "Win%"):
    tbl.add_column(col, justify="right")
for b in sorted(pg):
    pnls = pg[b]
    wins = sum(1 for x in pnls if x > 0)
    tbl.add_row(f"{b:.1f}", str(len(pnls)),
                f"${mean(pnls):+.1f}", f"${median(pnls):+.1f}",
                f"{wins/len(pnls):.0%}")
con.print(tbl)


# ── 9. Does trade count predict outcome? ────────────────────────────────────
con.print(f"\n[bold]P&L by trade count bucket[/bold]")
cg = defaultdict(list)
for w in joined:
    n = len(w["trades"])
    b = "1" if n == 1 else "2-3" if n <= 3 else "4-6" if n <= 6 else "7-10" if n <= 10 else "10+"
    cg[b].append(w["pnl"])
order = ["1", "2-3", "4-6", "7-10", "10+"]
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("# trades", "N", "Mean PnL", "Median PnL", "Win%"):
    tbl.add_column(col, justify="right")
for b in order:
    pnls = cg.get(b, [])
    if not pnls: continue
    wins = sum(1 for x in pnls if x > 0)
    tbl.add_row(b, str(len(pnls)),
                f"${mean(pnls):+.1f}", f"${median(pnls):+.1f}",
                f"{wins/len(pnls):.0%}")
con.print(tbl)
