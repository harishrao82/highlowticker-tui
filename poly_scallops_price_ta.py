#!/usr/bin/env python3
"""
poly_scallops_price_ta.py — does Scallops' trade side correlate with BTC price
movement at the moment of each trade?

Steps:
 1. Load BTC windows with bot trades.
 2. Fetch 1-min BTC OHLC from Coinbase Exchange REST for each window range.
 3. For each trade, compute:
      - price at trade time (1-min candle close)
      - window open price (first minute of the window)
      - delta_pct = (price_now - price_open) / price_open
 4. Group trades by side (Up/Down) and analyze delta_pct distribution,
    at different elapsed-time buckets.

Hypothesis to test:
  (A) Momentum: bot buys Up when BTC is up, Down when BTC is down
  (B) Contrarian: bot buys opposite side of recent move
  (C) Noise: no correlation between price and side
"""
import json
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median, stdev
from rich.console import Console
from rich.table import Table
import httpx

STATE = Path.home() / ".btc_strategy_state.json"
con   = Console()

CB_CANDLES = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
CACHE      = Path.home() / ".scallops_btc_1m_cache.json"


def window_start_ts(k: str) -> int:
    return int(k.rsplit("-", 1)[1])


def elapsed_sec(trade_time: str, w_start_ts: int) -> int | None:
    try:
        h, m, s = map(int, trade_time.split(":"))
    except Exception:
        return None
    start_dt = datetime.fromtimestamp(w_start_ts)
    day = start_dt.replace(hour=h, minute=m, second=s)
    delta = (day - start_dt).total_seconds()
    if delta < -3600: delta += 86400
    if delta < -60 or delta > 1800: return None
    return int(delta)


# ── 1. Load BTC Scallops windows ────────────────────────────────────────────
data = json.loads(STATE.read_text())
shadow = data["shadow"]

windows = []
for k, sh in shadow.items():
    if not k.startswith("btc-updown-15m-"):
        continue
    if not sh.get("resolved"):
        continue
    trades = sh.get("trades") or []
    if not trades:
        continue
    wts = window_start_ts(k)
    ts_trades = []
    for t in trades:
        e = elapsed_sec(t.get("time", ""), wts)
        if e is None:
            continue
        ts_trades.append({
            "elapsed": e, "side": t["side"],
            "price":   float(t.get("price", 0)),
            "shares":  float(t.get("shares", 0)),
            "trade_ts": wts + e,
        })
    if not ts_trades:
        continue
    windows.append({"key": k, "window_start": wts,
                    "winner": sh.get("winner"),
                    "trades": sorted(ts_trades, key=lambda t: t["elapsed"])})

con.print(f"[bold cyan]{len(windows)} BTC Scallops windows with trades[/bold cyan]")

if not windows:
    raise SystemExit("no data")

# Determine the minute ranges we need (one 30-min block per window start)
ranges: list[tuple[int, int]] = []
for w in windows:
    wts = w["window_start"]
    start = wts - 120            # 2 min before window open
    end   = wts + 900 + 60       # 1 min after window close
    ranges.append((start, end))


# ── 2. Fetch Coinbase 1-min candles ─────────────────────────────────────────
price_by_min: dict[int, float] = {}    # minute_ts (start-of-minute) → close

if CACHE.exists():
    try:
        price_by_min = {int(k): float(v) for k, v in json.loads(CACHE.read_text()).items()}
        con.print(f"[dim]Loaded {len(price_by_min)} cached minute candles[/dim]")
    except Exception:
        price_by_min = {}


def need_fetch_for(start: int, end: int) -> bool:
    m = start - (start % 60)
    while m <= end:
        if m not in price_by_min:
            return True
        m += 60
    return False


needed_windows = [r for r in ranges if need_fetch_for(*r)]
con.print(f"[dim]{len(needed_windows)} window ranges need fetching[/dim]")

# Merge overlapping ranges to batch fetch (Coinbase max 300 candles per call)
ranges_sorted = sorted(needed_windows)
merged: list[tuple[int, int]] = []
for s, e in ranges_sorted:
    if merged and s <= merged[-1][1] + 60:
        merged[-1] = (merged[-1][0], max(merged[-1][1], e))
    else:
        merged.append((s, e))
con.print(f"[dim]Merged into {len(merged)} fetch blocks[/dim]")

# Split merged blocks into ≤300-minute chunks
chunks = []
for s, e in merged:
    cur = s
    while cur < e:
        chunk_end = min(cur + 300 * 60, e)
        chunks.append((cur, chunk_end))
        cur = chunk_end
con.print(f"[dim]Split into {len(chunks)} chunks ≤300 min each[/dim]")

if chunks:
    with httpx.Client(timeout=15) as client:
        for i, (s, e) in enumerate(chunks):
            params = {
                "start": datetime.utcfromtimestamp(s).strftime("%Y-%m-%dT%H:%M:%S"),
                "end":   datetime.utcfromtimestamp(e).strftime("%Y-%m-%dT%H:%M:%S"),
                "granularity": 60,
            }
            try:
                r = client.get(CB_CANDLES, params=params)
                if r.status_code == 200:
                    for row in r.json():
                        # [time, low, high, open, close, volume]
                        t = int(row[0])
                        close = float(row[4])
                        price_by_min[t] = close
                else:
                    con.print(f"[yellow]chunk {i}: {r.status_code} {r.text[:100]}[/yellow]")
            except Exception as ex:
                con.print(f"[yellow]chunk {i}: {ex}[/yellow]")
            if i % 20 == 0 and i > 0:
                con.print(f"[dim]  fetched {i}/{len(chunks)} chunks, "
                          f"{len(price_by_min)} minutes cached[/dim]")
            time.sleep(0.12)   # ~8 req/sec to stay under rate limit

    # Save cache
    try:
        CACHE.write_text(json.dumps({str(k): v for k, v in price_by_min.items()}))
    except Exception:
        pass

con.print(f"[bold]Total minute candles available: {len(price_by_min)}[/bold]\n")


def price_at(ts: int) -> float | None:
    m = ts - (ts % 60)
    return price_by_min.get(m)


# ── 3. Enrich each trade with BTC delta from window open ────────────────────
enriched_trades = []
matched_windows = 0
for w in windows:
    wts = w["window_start"]
    open_px = price_at(wts)
    if not open_px:
        continue
    matched_windows += 1
    for t in w["trades"]:
        p_now = price_at(t["trade_ts"])
        if not p_now:
            continue
        delta_pct = (p_now - open_px) / open_px * 100
        enriched_trades.append({
            "key":       w["key"],
            "winner":    w["winner"],
            "side":      t["side"],
            "elapsed":   t["elapsed"],
            "price_bot": t["price"],     # bot's paid price
            "shares":    t["shares"],
            "btc_open":  open_px,
            "btc_now":   p_now,
            "delta_pct": delta_pct,
        })

con.print(f"[bold]Enriched {len(enriched_trades)} trades from "
          f"{matched_windows}/{len(windows)} windows[/bold]\n")

if not enriched_trades:
    raise SystemExit("no enriched trades")


# ── 4. Overall: is bot's side momentum-aligned with BTC delta? ─────────────
con.print("[bold]Does the bot's trade side follow BTC price movement?[/bold]")

up_deltas   = [t["delta_pct"] for t in enriched_trades if t["side"] == "Up"]
down_deltas = [t["delta_pct"] for t in enriched_trades if t["side"] == "Down"]

con.print(f"  Up trades:   N={len(up_deltas)}  "
          f"mean Δ={mean(up_deltas):+.3f}%  "
          f"median Δ={median(up_deltas):+.3f}%")
con.print(f"  Down trades: N={len(down_deltas)}  "
          f"mean Δ={mean(down_deltas):+.3f}%  "
          f"median Δ={median(down_deltas):+.3f}%")
gap = mean(up_deltas) - mean(down_deltas)
con.print(f"  [bold]Gap (up_mean − down_mean): {gap:+.3f}%[/bold]  "
          f"[dim]positive = momentum follower[/dim]")


# ── 5. Split by elapsed-time bucket ────────────────────────────────────────
con.print("\n[bold]By elapsed-time bucket[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
for col in ("Elapsed", "Up N", "Up mean Δ%", "Down N", "Down mean Δ%", "Gap"):
    tbl.add_column(col, justify="right")
buckets = [(0,60),(60,120),(120,300),(300,600),(600,900)]
for lo, hi in buckets:
    ups   = [t["delta_pct"] for t in enriched_trades
             if t["side"] == "Up"   and lo <= t["elapsed"] < hi]
    downs = [t["delta_pct"] for t in enriched_trades
             if t["side"] == "Down" and lo <= t["elapsed"] < hi]
    if not ups or not downs: continue
    gap = mean(ups) - mean(downs)
    tbl.add_row(f"{lo}-{hi}s",
                str(len(ups)), f"{mean(ups):+.3f}",
                str(len(downs)), f"{mean(downs):+.3f}",
                f"[{'green' if gap > 0 else 'red'}]{gap:+.3f}%[/{'green' if gap > 0 else 'red'}]")
con.print(tbl)


# ── 6. Delta-at-trade histogram: side vs bucketed delta ───────────────────
con.print("\n[bold]Trade side frequency by BTC delta bucket[/bold]  "
          "[dim](if delta > X%, what % of trades are Up?)[/dim]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
for col in ("Δ bucket", "N", "Up%", "Down%", "Bot's pref"):
    tbl.add_column(col, justify="right")
delta_buckets = [
    ("Δ < -0.20%", lambda d: d < -0.20),
    ("-0.20..-0.05", lambda d: -0.20 <= d < -0.05),
    ("-0.05..0.05",  lambda d: -0.05 <= d <= 0.05),
    ("0.05..0.20",   lambda d: 0.05 < d <= 0.20),
    ("Δ > 0.20%",    lambda d: d > 0.20),
]
for name, pred in delta_buckets:
    ts = [t for t in enriched_trades if pred(t["delta_pct"])]
    if not ts: continue
    up_n = sum(1 for t in ts if t["side"] == "Up")
    dn_n = len(ts) - up_n
    up_pct = up_n / len(ts)
    pref = "Up"  if up_pct > 0.55 else "Down" if up_pct < 0.45 else "—"
    col = "green" if pref == "Up" else "red" if pref == "Down" else "white"
    tbl.add_row(name, str(len(ts)),
                f"{up_pct*100:.0f}%", f"{(1-up_pct)*100:.0f}%",
                f"[{col}]{pref}[/{col}]")
con.print(tbl)


# ── 7. Does entry lean match the winner BY delta at time of trade? ────────
con.print("\n[bold]When BTC delta matches trade side, is that side usually the winner?[/bold]")
momentum_match = 0; momentum_correct = 0
contra_match = 0; contra_correct = 0
for t in enriched_trades:
    d = t["delta_pct"]
    if abs(d) < 0.05: continue  # filter tiny noise
    momentum_side = "Up" if d > 0 else "Down"
    if t["side"] == momentum_side:
        momentum_match += 1
        if t["side"] == t["winner"]: momentum_correct += 1
    else:
        contra_match += 1
        if t["side"] == t["winner"]: contra_correct += 1
con.print(f"  Momentum trades (bot's side = direction of BTC move):")
con.print(f"    N={momentum_match}  winner match {momentum_correct}/{momentum_match} "
          f"= {momentum_correct/max(momentum_match,1):.1%}")
con.print(f"  Contrarian trades (bot's side = opposite of BTC move):")
con.print(f"    N={contra_match}  winner match {contra_correct}/{contra_match} "
          f"= {contra_correct/max(contra_match,1):.1%}")


# ── 8. Per-window: compute avg delta when bot committed ───────────────────
con.print("\n[bold]Per-window commit delta[/bold]  "
          "[dim](at the elapsed time where the bot's lean-sign matched its final lean and stayed, "
          "what was BTC's delta?)[/dim]")

# First compute final lean per window
window_leans = {}
for w in windows:
    cum_up = 0.0; cum_dn = 0.0
    for t in w["trades"]:
        if t["side"] == "Up": cum_up += t["shares"]
        else:                 cum_dn += t["shares"]
    window_leans[w["key"]] = "Up" if cum_up > cum_dn else "Down"

commit_deltas_right = []
commit_deltas_wrong = []
for w in windows:
    if w["key"] not in window_leans: continue
    final_lean = window_leans[w["key"]]
    # find first trade where lean_sign matches and stays
    cum_up = 0.0; cum_dn = 0.0
    last_wrong_idx = -1
    for i, t in enumerate(w["trades"]):
        if t["side"] == "Up": cum_up += t["shares"]
        else:                 cum_dn += t["shares"]
        correct = (cum_up > cum_dn and final_lean == "Up") or (cum_dn > cum_up and final_lean == "Down")
        if not correct:
            last_wrong_idx = i
    if last_wrong_idx == -1:
        commit_idx = 0
    elif last_wrong_idx == len(w["trades"]) - 1:
        commit_idx = len(w["trades"]) - 1
    else:
        commit_idx = last_wrong_idx + 1

    commit_trade = w["trades"][commit_idx]
    open_px = price_at(w["window_start"])
    commit_px = price_at(commit_trade["trade_ts"])
    if not open_px or not commit_px: continue
    delta = (commit_px - open_px) / open_px * 100
    if final_lean == w["winner"]:
        commit_deltas_right.append((delta, final_lean, commit_trade["elapsed"]))
    else:
        commit_deltas_wrong.append((delta, final_lean, commit_trade["elapsed"]))

con.print(f"  Windows where final lean = winner: N={len(commit_deltas_right)}")
if commit_deltas_right:
    # split by final lean
    ups = [d for d, l, _ in commit_deltas_right if l == "Up"]
    dns = [d for d, l, _ in commit_deltas_right if l == "Down"]
    if ups: con.print(f"    Winning 'Up' leans: BTC Δ at commit — mean {mean(ups):+.3f}%  median {median(ups):+.3f}%")
    if dns: con.print(f"    Winning 'Down' leans: BTC Δ at commit — mean {mean(dns):+.3f}%  median {median(dns):+.3f}%")

con.print(f"\n  Windows where final lean ≠ winner: N={len(commit_deltas_wrong)}")
if commit_deltas_wrong:
    ups = [d for d, l, _ in commit_deltas_wrong if l == "Up"]
    dns = [d for d, l, _ in commit_deltas_wrong if l == "Down"]
    if ups: con.print(f"    Losing 'Up' leans: BTC Δ at commit — mean {mean(ups):+.3f}%  median {median(ups):+.3f}%")
    if dns: con.print(f"    Losing 'Down' leans: BTC Δ at commit — mean {mean(dns):+.3f}%  median {median(dns):+.3f}%")
