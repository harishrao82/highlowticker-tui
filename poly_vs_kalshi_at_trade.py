#!/usr/bin/env python3
"""
poly_vs_kalshi_at_trade.py — for each Scallops trade we logged in
~/.scallops_live_trades.jsonl, look up what Kalshi was quoting at the
same elapsed-second within the same 15-min window, and see whether our
EV-capped strategy would have fired.
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from rich.console import Console
from rich.table import Table

DB    = Path.home() / ".btc_windows.db"
LOG   = Path.home() / ".scallops_live_trades.jsonl"
con   = Console()

# Same EV-capped checkpoints as the live trader
CHECKPOINTS = [
    ( 30, 0.03, 0.53),
    ( 60, 0.05, 0.59),
    (120, 0.10, 0.68),
    (180, 0.10, 0.71),
    (300, 0.10, 0.79),
    (420, 0.10, 0.85),
    (600, 0.05, 0.88),
]

# Load Scallops trades
all_trades = [json.loads(l) for l in open(LOG) if l.strip()]
# Keep only 15m crypto trades that have elapsed_in_window
trades_15m = [t for t in all_trades
              if t.get("elapsed_in_window") is not None
              and t.get("coin") in ("BTC", "ETH", "SOL", "XRP")
              and t.get("market_type") == "15m"]
# Some old log entries have elapsed but no market_type — fall back on slug
if not trades_15m:
    trades_15m = [t for t in all_trades
                  if t.get("elapsed_in_window") is not None
                  and t.get("coin") in ("BTC", "ETH", "SOL", "XRP")
                  and "updown-15m" in (t.get("slug") or "")]

con.print(f"[bold cyan]{len(trades_15m)} Scallops 15m crypto trades in log[/bold cyan]")
if not trades_15m:
    raise SystemExit

# Build a map of Kalshi window_id by (window_start_ts, coin)
db = sqlite3.connect(DB)
kw_by_key = {}
for wid, ticker, winner, wts in db.execute(
    "SELECT id, ticker, winner, window_start_ts FROM windows"
):
    coin = ticker[2:5]
    kw_by_key[(wts, coin)] = (wid, ticker, winner)

# Pre-cache ticks per relevant window
needed_wids = set()
for t in trades_15m:
    wts  = t["window_start_ts"]
    coin = t["coin"]
    key  = (wts, coin)
    if key in kw_by_key:
        needed_wids.add(kw_by_key[key][0])

ticks_by_wid: dict[int, dict[int, tuple[float,float]]] = {}
for wid in needed_wids:
    rows = db.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks WHERE window_id=?",
        (wid,)
    ).fetchall()
    ticks_by_wid[wid] = {r[0]: (r[1], r[2]) for r in rows
                         if r[1] is not None and r[2] is not None}

con.print(f"[dim]Loaded ticks for {len(ticks_by_wid)} Kalshi windows[/dim]\n")


def kalshi_ask_at(wid: int, elapsed: int, lookback: int = 10) -> tuple[float, float] | None:
    ticks = ticks_by_wid.get(wid, {})
    for d in range(lookback + 1):
        if (elapsed - d) in ticks:
            return ticks[elapsed - d]
        if (elapsed + d) in ticks:
            return ticks[elapsed + d]
    return None


# ── Per-trade comparison ──────────────────────────────────────────────────
def closest_checkpoint(elapsed: int) -> tuple[int, float, float] | None:
    """Pick the checkpoint whose T is closest to (and ≤) elapsed."""
    best = None
    for T, thr, max_px in CHECKPOINTS:
        if T <= elapsed:
            best = (T, thr, max_px)
    return best


con.print("[bold]Per-trade Kalshi cross-reference (most recent 30)[/bold]")
con.print("[dim]Polymarket scallops trade  →  what Kalshi was quoting at same elapsed[/dim]\n")

tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,1))
for col in ("Coin","T (s)","Side","Poly $","Δ%","K-yes","K-no","K-ask side","Cap @T","Verdict"):
    tbl.add_column(col, justify="right")

# Sort trades by trade_ts (newest first), take 30
recent = sorted(trades_15m, key=lambda x: -x["trade_ts"])[:30]
recent.sort(key=lambda x: (x["window_start_ts"], x["elapsed_in_window"]))

would_fire = 0
would_skip_price = 0
no_kalshi = 0

for t in recent:
    coin    = t["coin"]
    wts     = t["window_start_ts"]
    elapsed = t["elapsed_in_window"]
    side    = "yes" if t["outcome"] == "Up" else "no"
    poly_px = t["price"]
    delta   = t.get("delta_pct")

    key = (wts, coin)
    if key not in kw_by_key:
        tbl.add_row(coin, str(elapsed), side, f"{poly_px:.3f}",
                    f"{delta:+.3f}" if delta is not None else "?",
                    "—","—","—","—","[dim]no Kalshi[/dim]")
        no_kalshi += 1
        continue
    wid, ticker, winner = kw_by_key[key]
    asks = kalshi_ask_at(wid, elapsed)
    if not asks:
        tbl.add_row(coin, str(elapsed), side, f"{poly_px:.3f}",
                    f"{delta:+.3f}" if delta is not None else "?",
                    "—","—","—","—","[dim]no tick[/dim]")
        no_kalshi += 1
        continue

    ya, na = asks
    k_ask = ya if side == "yes" else na

    cp = closest_checkpoint(elapsed)
    if not cp:
        cp_str = "—"
        verdict = "[dim]too early[/dim]"
    else:
        T, thr, cap = cp
        cp_str = f"{cap:.2f}@T{T}"
        # Would we fire?
        delta_ok  = (delta is not None and abs(delta) >= thr)
        side_ok   = (side == ("yes" if (delta or 0) > 0 else "no"))
        price_ok  = (k_ask <= cap and k_ask > 0.05)

        if not delta_ok:
            verdict = "[yellow]Δ too small[/yellow]"
        elif not side_ok:
            verdict = "[red]wrong side vs Δ[/red]"
        elif not price_ok:
            verdict = "[red]ask too rich[/red]"
            would_skip_price += 1
        else:
            verdict = "[green]WOULD FIRE[/green]"
            would_fire += 1

    tbl.add_row(
        coin, str(elapsed), side.upper(),
        f"{poly_px:.3f}",
        f"{delta:+.3f}" if delta is not None else "?",
        f"{ya:.2f}" if ya else "?",
        f"{na:.2f}" if na else "?",
        f"{k_ask:.2f}",
        cp_str,
        verdict,
    )
con.print(tbl)

con.print(f"\n[bold]Summary[/bold]  WOULD FIRE: {would_fire}  "
          f"skipped (ask too rich): {would_skip_price}  "
          f"no Kalshi data: {no_kalshi}")


# ── Side-by-side: Poly fill price vs Kalshi ask at same instant ───────────
con.print("\n[bold]Price gap (Kalshi ask − Polymarket fill) per coin[/bold]")
gaps = defaultdict(list)
for t in trades_15m:
    coin = t["coin"]; wts = t["window_start_ts"]
    elapsed = t["elapsed_in_window"]
    side = "yes" if t["outcome"] == "Up" else "no"
    poly_px = t["price"]
    if (wts, coin) not in kw_by_key: continue
    wid = kw_by_key[(wts, coin)][0]
    asks = kalshi_ask_at(wid, elapsed)
    if not asks: continue
    ya, na = asks
    k_ask = ya if side == "yes" else na
    gaps[coin].append({"poly": poly_px, "kalshi": k_ask,
                       "gap": k_ask - poly_px,
                       "elapsed": elapsed, "side": side})

if any(gaps.values()):
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("Coin","N","Mean Poly","Mean Kalshi","Mean gap","Worst gap","Best gap"):
        tbl.add_column(col, justify="right")
    for coin, lst in sorted(gaps.items()):
        if not lst: continue
        mp = sum(x["poly"] for x in lst) / len(lst)
        mk = sum(x["kalshi"] for x in lst) / len(lst)
        mg = sum(x["gap"] for x in lst) / len(lst)
        worst = max(lst, key=lambda x: x["gap"])
        best  = min(lst, key=lambda x: x["gap"])
        col = "red" if mg > 0 else "green"
        tbl.add_row(coin, str(len(lst)),
                    f"{mp:.3f}", f"{mk:.3f}",
                    f"[{col}]{mg:+.3f}[/{col}]",
                    f"{worst['gap']:+.3f}",
                    f"{best['gap']:+.3f}")
    con.print(tbl)

con.print("\n[dim]gap > 0 means Kalshi was MORE expensive than Polymarket "
          "at the same elapsed time (Kalshi reprices faster — bad for us)[/dim]")
con.print("[dim]gap < 0 means Kalshi was CHEAPER (good for us — could enter "
          "before Kalshi caught up)[/dim]")

db.close()
