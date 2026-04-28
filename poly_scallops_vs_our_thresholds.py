#!/usr/bin/env python3
"""
poly_scallops_vs_our_thresholds.py — validate our dynamic threshold against
Scallops' actual historical BTC trades.

For each of his BTC-15m trades:
  1. Determine coin, window_start_ts, elapsed_in_window, |delta| at trade time
  2. Compute what our effective threshold would have been:
       - find effective checkpoint at his elapsed time
       - vol_eff = max(profile[dow_utc][hour_utc], realized_1h_at_window_start)
       - thr = clip(factor * vol_eff, VOL_THR_FLOOR, VOL_THR_CEILING)
  3. Did |delta| >= thr? (match = we would have fired too)

Aggregate the match rate overall and by hour of day to see if our thresholds
are too tight or too loose.
"""
import asyncio
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, stdev
from rich.console import Console
from rich.table import Table

import btc_vol_profile

STATE = Path.home() / ".btc_strategy_state.json"
CACHE = Path.home() / ".scallops_btc_1m_cache.json"

# These must mirror kalshi_momentum_live.py
CHECKPOINTS = [
    ( 30, 0.5, 0.53),
    ( 60, 0.7, 0.59),
    (120, 1.0, 0.68),
    (180, 1.0, 0.71),
    (300, 1.2, 0.79),
    (420, 1.5, 0.85),
    (600, 1.5, 0.88),
]
VOL_THR_FLOOR   = 0.015
VOL_THR_CEILING = 0.25
LOOKBACK_MIN    = 60

con = Console()

# ── Load price cache ───────────────────────────────────────────────────────
price_by_min: dict[int, float] = {
    int(k): float(v) for k, v in json.loads(CACHE.read_text()).items()
}
con.print(f"[dim]Loaded {len(price_by_min):,} BTC minute candles[/dim]")


def price_at(ts: int) -> float | None:
    return price_by_min.get(ts - (ts % 60))


def realized_vol_at(ts: int, window_min: int = LOOKBACK_MIN) -> float:
    closes = []
    for m in range(window_min + 1):
        p = price_by_min.get(ts - (ts % 60) - m * 60)
        if p is None:
            break
        closes.append(p)
    if len(closes) < 10:
        return 0.0
    closes.reverse()
    rets = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0 and closes[i] > 0:
            rets.append(math.log(closes[i] / closes[i-1]))
    if len(rets) < 5:
        return 0.0
    return stdev(rets) * 100.0


def effective_checkpoint(elapsed: int):
    best = None
    for T, factor, cap in CHECKPOINTS:
        if elapsed >= T:
            best = (T, factor, cap)
        else:
            break
    return best


# ── Load Scallops trades ────────────────────────────────────────────────────
data   = json.loads(STATE.read_text())
shadow = data.get("shadow", {})


def window_start_ts(key: str) -> int:
    return int(key.rsplit("-", 1)[1])


def elapsed_sec(tt: str, wts: int):
    try:
        h, m, s = map(int, tt.split(":"))
    except Exception:
        return None
    sd = datetime.fromtimestamp(wts)
    day = sd.replace(hour=h, minute=m, second=s)
    d = (day - sd).total_seconds()
    if d < -3600: d += 86400
    if d < -60 or d > 1800: return None
    return int(d)


async def main():
    profile = await btc_vol_profile.load_or_build(
        {"BTC": "BTC-USD"},
        log=lambda m: con.print(f"[dim]{m}[/dim]"),
    )

    trades = []
    for k, sh in shadow.items():
        if not k.startswith("btc-updown-15m-"):
            continue
        wts = window_start_ts(k)
        open_p = price_at(wts)
        if not open_p: continue
        for t in (sh.get("trades") or []):
            e = elapsed_sec(t.get("time", ""), wts)
            if e is None or e < 1 or e >= 900: continue
            trade_ts = wts + e
            p_at = price_at(trade_ts)
            if not p_at: continue
            delta = abs((p_at - open_p) / open_p * 100.0)
            trades.append({
                "wts":     wts,
                "elapsed": e,
                "delta":   delta,
                "dt_utc":  datetime.fromtimestamp(wts, tz=timezone.utc),
            })

    con.print(f"[bold cyan]Evaluating {len(trades):,} Scallops BTC trades "
              f"against our dynamic thresholds[/bold cyan]\n")

    # Evaluate each trade
    match     = 0
    below_thr = 0
    no_checkpoint = 0
    match_by_hour: dict[int, list[int]] = defaultdict(list)
    below_by_hour: dict[int, list[float]] = defaultdict(list)
    vol_used_hist: list[float] = []
    thr_hist: list[float] = []

    for t in trades:
        cp = effective_checkpoint(t["elapsed"])
        if not cp:
            no_checkpoint += 1
            continue
        T_eff, factor, _cap = cp

        slot_vol = btc_vol_profile.expected_vol(profile, "BTC", t["dt_utc"])
        real_vol = realized_vol_at(t["wts"])
        eff_vol  = max(slot_vol, real_vol)
        thr      = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * eff_vol))

        thr_hist.append(thr)
        vol_used_hist.append(eff_vol)

        hour_utc = t["dt_utc"].hour
        fires = (t["delta"] >= thr)
        match_by_hour[hour_utc].append(1 if fires else 0)
        if fires:
            match += 1
        else:
            below_thr += 1
            below_by_hour[hour_utc].append(t["delta"])

    total = match + below_thr
    con.print(f"[bold]Overall[/bold]  matches {match:,}/{total:,} = "
              f"{match/total*100:.1f}%   below threshold {below_thr:,}")
    con.print(f"  (excluded {no_checkpoint:,} trades before T=30s checkpoint)")
    con.print(f"  median threshold used: {median(thr_hist):.3f}%  "
              f"mean eff_vol used: {mean(vol_used_hist):.4f}%")

    # By hour of day (UTC)
    con.print("\n[bold]Match rate by UTC hour[/bold]  "
              "[dim](lower = our thresholds too strict vs his trades)[/dim]")
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,2))
    for col in ("UTC hr","N","matches","match%","median |Δ| below thr","profile_vol"):
        tbl.add_column(col, justify="right")
    for h in sorted(match_by_hour):
        flags = match_by_hour[h]
        belows = below_by_hour.get(h, [])
        n = len(flags)
        matches = sum(flags)
        pct = matches / n * 100 if n else 0
        med_below = f"{median(belows):.4f}%" if belows else "—"
        # profile vol at this hour (average across dows)
        pvs = [
            profile["BTC"].get(dow, {}).get(h)
            for dow in range(7)
            if profile["BTC"].get(dow, {}).get(h) is not None
        ]
        pv_mean = mean(pvs) if pvs else 0
        col = "green" if pct >= 60 else "yellow" if pct >= 40 else "red"
        tbl.add_row(
            f"{h:02d}:00",
            str(n),
            str(matches),
            f"[{col}]{pct:.0f}%[/{col}]",
            med_below,
            f"{pv_mean:.4f}%",
        )
    con.print(tbl)

    # Distribution of "how far below the threshold he fired" — is he just
    # barely under (minor calibration gap) or way under (signal is different)?
    if below_thr:
        con.print("\n[bold]For trades we would have skipped — how much is his |Δ| "
                  "below our threshold?[/bold]")
        gaps = []
        for t in trades:
            cp = effective_checkpoint(t["elapsed"])
            if not cp: continue
            T, factor, _ = cp
            slot = btc_vol_profile.expected_vol(profile, "BTC", t["dt_utc"])
            real = realized_vol_at(t["wts"])
            eff  = max(slot, real)
            thr  = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * eff))
            if t["delta"] < thr:
                gaps.append((thr - t["delta"]) / thr * 100)
        gaps.sort()
        def pct(xs, q): return xs[int(len(xs)*q)] if xs else 0
        con.print(f"  N={len(gaps)}  p10={pct(gaps,0.1):.0f}%  "
                  f"p50={pct(gaps,0.5):.0f}%  p90={pct(gaps,0.9):.0f}%")
        con.print(f"  [dim]p50 = median 'miss fraction' — his delta was "
                  f"this much below our threshold[/dim]")


if __name__ == "__main__":
    asyncio.run(main())
