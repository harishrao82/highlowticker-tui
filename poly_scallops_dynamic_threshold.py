#!/usr/bin/env python3
"""
poly_scallops_dynamic_threshold.py — does Scallops' entry threshold adapt
to market conditions (night vs day, quiet vs vol)?

Uses:
  - ~/.btc_strategy_state.json         shadow trades (hours of historical data)
  - ~/.scallops_btc_1m_cache.json      BTC minute closes
  - ~/.scallops_live_trades.jsonl      recent live shadow (optional)

For each BTC-15m-updown trade, computes:
  - delta_pct   = (btc_at_trade - btc_at_window_open) / btc_at_window_open * 100
  - realized_vol = stddev of BTC 1-min log returns over the prior hour
  - hour_et      = hour of day in ET (0-23)

Then asks:
  - Does the distribution of |delta_pct| at which he fires shift by hour?
  - Does it shift with realized vol?
  - Is "today's night session" (last 6 hours) different from the historical norm?
"""
import json
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, stdev
from rich.console import Console
from rich.table import Table

STATE       = Path.home() / ".btc_strategy_state.json"
CACHE       = Path.home() / ".scallops_btc_1m_cache.json"
LIVE_LOG    = Path.home() / ".scallops_live_trades.jsonl"
con         = Console()

ET_OFFSET   = -4 * 3600   # seconds


# ── Load BTC 1-min prices ───────────────────────────────────────────────────
price_by_min: dict[int, float] = {}
if CACHE.exists():
    for k, v in json.loads(CACHE.read_text()).items():
        price_by_min[int(k)] = float(v)
con.print(f"[dim]Loaded {len(price_by_min):,} BTC minute candles[/dim]")

def price_at(ts: int) -> float | None:
    return price_by_min.get(ts - (ts % 60))


# ── Realized volatility: stddev of 1-min log returns over prior 60 min ─────
def realized_vol_pct(end_ts: int, window_min: int = 60) -> float | None:
    start_ts = end_ts - window_min * 60
    prices = []
    m = start_ts - (start_ts % 60)
    while m <= end_ts:
        p = price_by_min.get(m)
        if p:
            prices.append(p)
        m += 60
    if len(prices) < 10:
        return None
    rets = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            rets.append(math.log(prices[i] / prices[i-1]))
    if len(rets) < 5:
        return None
    if len(rets) < 2:
        return None
    return stdev(rets) * 100.0   # as a pct of price


# ── Parse shadow trades from state.json ────────────────────────────────────
data   = json.loads(STATE.read_text())
shadow = data.get("shadow", {})

def window_start_ts(key: str) -> int:
    return int(key.rsplit("-", 1)[1])

def elapsed_sec(trade_time: str, wts: int) -> int | None:
    try:
        h, m, s = map(int, trade_time.split(":"))
    except Exception:
        return None
    sd = datetime.fromtimestamp(wts)
    day = sd.replace(hour=h, minute=m, second=s)
    d = (day - sd).total_seconds()
    if d < -3600: d += 86400
    if d < -60 or d > 1800: return None
    return int(d)


enriched = []
for k, sh in shadow.items():
    if not k.startswith("btc-updown-15m-"):
        continue
    wts = window_start_ts(k)
    open_p = price_at(wts)
    if not open_p:
        continue
    for t in (sh.get("trades") or []):
        e = elapsed_sec(t.get("time", ""), wts)
        if e is None: continue
        trade_ts = wts + e
        p_at = price_at(trade_ts)
        if not p_at: continue
        delta = (p_at - open_p) / open_p * 100.0
        enriched.append({
            "trade_ts":  trade_ts,
            "delta":     delta,
            "side":      t.get("side"),
            "price":     float(t.get("price", 0) or 0),
            "size":      float(t.get("shares", 0) or 0),
            "hour_et":   ((trade_ts + ET_OFFSET) // 3600) % 24,
        })

con.print(f"[bold cyan]{len(enriched):,} enriched BTC trades[/bold cyan]\n")
if not enriched:
    raise SystemExit

# Compute volatility for each trade (cached per minute for speed)
vol_cache: dict[int, float] = {}
for t in enriched:
    m = t["trade_ts"] - (t["trade_ts"] % 60)
    if m not in vol_cache:
        v = realized_vol_pct(t["trade_ts"])
        if v is not None:
            vol_cache[m] = v
    t["vol"] = vol_cache.get(m)

with_vol = [t for t in enriched if t["vol"] is not None]
con.print(f"[dim]{len(with_vol):,} trades with realized-vol data[/dim]\n")


# ── 1. |delta| distribution by hour of day (ET) ───────────────────────────
con.print("[bold]|delta| at his trade time — by hour of day (ET)[/bold]")
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Hour ET","N trades","p10","p50","p90","mean |Δ|","mean vol%"):
    tbl.add_column(col, justify="right")
by_hour = defaultdict(list)
for t in enriched:
    by_hour[int(t["hour_et"])].append(t)

def pct(xs, q):
    return sorted(xs)[int(len(xs)*q)] if xs else 0

for h in range(24):
    ts = by_hour.get(h, [])
    if not ts: continue
    abs_d = [abs(x["delta"]) for x in ts]
    vols = [x["vol"] for x in ts if x["vol"] is not None]
    mean_vol = mean(vols) if vols else 0
    tbl.add_row(
        f"{h:02d}:00",
        str(len(ts)),
        f"{pct(abs_d, 0.10):.3f}%",
        f"{pct(abs_d, 0.50):.3f}%",
        f"{pct(abs_d, 0.90):.3f}%",
        f"{mean(abs_d):.3f}%",
        f"{mean_vol:.3f}%" if mean_vol else "-",
    )
con.print(tbl)


# ── 2. |delta| distribution by realized vol bucket ────────────────────────
con.print("\n[bold]|delta| at his trade time — by realized BTC vol (prior 1h)[/bold]")
vol_bands = [
    ("<0.03%",  lambda v: v < 0.03),
    ("0.03-0.05%", lambda v: 0.03 <= v < 0.05),
    ("0.05-0.08%", lambda v: 0.05 <= v < 0.08),
    ("0.08-0.12%", lambda v: 0.08 <= v < 0.12),
    (">=0.12%", lambda v: v >= 0.12),
]
tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
for col in ("Vol band","N","p10","p50 (median)","p90","mean |Δ|"):
    tbl.add_column(col, justify="right")
for name, pred in vol_bands:
    ts = [x for x in with_vol if pred(x["vol"])]
    if not ts: continue
    abs_d = [abs(x["delta"]) for x in ts]
    tbl.add_row(name, str(len(ts)),
                f"{pct(abs_d, 0.10):.3f}%",
                f"{pct(abs_d, 0.50):.3f}%",
                f"{pct(abs_d, 0.90):.3f}%",
                f"{mean(abs_d):.3f}%")
con.print(tbl)


# ── 3. Correlation: his |delta| vs current vol ───────────────────────────
n = len(with_vol)
if n >= 30:
    ds = [abs(x["delta"]) for x in with_vol]
    vs = [x["vol"] for x in with_vol]
    d_mean = mean(ds); v_mean = mean(vs)
    num = sum((ds[i]-d_mean)*(vs[i]-v_mean) for i in range(n))
    d_var = sum((d - d_mean)**2 for d in ds)
    v_var = sum((v - v_mean)**2 for v in vs)
    if d_var > 0 and v_var > 0:
        corr = num / (math.sqrt(d_var) * math.sqrt(v_var))
        con.print(f"\n[bold]Pearson correlation  |Δ at trade| vs vol(prior 1h): {corr:+.3f}[/bold]  "
                  f"[dim]({n:,} trades)[/dim]")
        if corr > 0.2:
            con.print("  [green]→ positive: when vol is high, he fires on bigger deltas. DYNAMIC.[/green]")
        elif corr < -0.2:
            con.print("  [red]→ negative: when vol is high, he fires on smaller deltas (unusual).[/red]")
        else:
            con.print("  [yellow]→ ~zero: his threshold looks stable across vol regimes.[/yellow]")


# ── 4. Today's "night session" — last 6 hours of live trades ─────────────
con.print("\n[bold]Today's recent session  (live shadow log, last 6h)[/bold]")
if LIVE_LOG.exists():
    cutoff = int(time.time()) - 6*3600
    live_trades = []
    for line in open(LIVE_LOG):
        try:
            e = json.loads(line)
        except Exception:
            continue
        if e.get("trade_ts", 0) < cutoff:
            continue
        if e.get("market_type") != "15m" and "updown-15m" not in (e.get("slug") or ""):
            continue
        if e.get("coin") != "BTC":
            continue
        if e.get("delta_pct") is None:
            continue
        live_trades.append(e)

    if live_trades:
        abs_ds = [abs(e["delta_pct"]) for e in live_trades]
        con.print(f"  {len(live_trades)} BTC 15m trades in last 6h")
        con.print(f"  median |Δ| at trade = {median(abs_ds):.3f}%")
        con.print(f"  p10 = {pct(abs_ds, 0.10):.3f}%  "
                  f"p50 = {pct(abs_ds, 0.50):.3f}%  "
                  f"p90 = {pct(abs_ds, 0.90):.3f}%")
        # Compare to historical median
        hist_abs = [abs(t["delta"]) for t in enriched]
        hist_med = median(hist_abs)
        diff = median(abs_ds) - hist_med
        col = "green" if abs(diff) > 0.01 else "yellow"
        con.print(f"  historical median |Δ| (all hours): {hist_med:.3f}%")
        con.print(f"  [{col}]delta shift: {diff:+.3f}%[/{col}]")
    else:
        con.print("  [dim]no live BTC 15m trades in the last 6h[/dim]")
else:
    con.print("  [dim]no live shadow log yet[/dim]")
