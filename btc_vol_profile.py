#!/usr/bin/env python3
"""
btc_vol_profile.py — time-of-week realized volatility profile.

Builds a lookup table: `profile[coin][day_of_week][hour] = expected_1h_vol_pct`
from Coinbase historical 1-min candles. Caches to disk so subsequent runs
skip the fetch (rebuilds daily).

Used to scale momentum thresholds in advance of known-volatile hours
(e.g. 9:30 ET market open) instead of relying on a backward-looking
realized-vol computation that lags regime changes.

Usage:
    import btc_vol_profile
    profile = await btc_vol_profile.load_or_build({"BTC": "BTC-USD", ...})
    vol_pct = btc_vol_profile.expected_vol(profile, "BTC", datetime.now())
"""
import asyncio
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import stdev, mean
from typing import Awaitable, Callable

import ccxt

CACHE_FILE     = Path.home() / ".btc_vol_profile.json"
CACHE_TTL_SEC  = 24 * 3600      # rebuild daily
DAYS_BACK      = 21             # 3 weeks → ~3 samples per (dow, hour) bin
LOOKBACK_MIN   = 60             # realized-vol window length

_cb = ccxt.coinbase()


async def _fetch_history(product: str, days: int,
                         log: Callable[[str], None] = print) -> list[tuple[int, float]]:
    """Paginated Coinbase 1-min OHLCV fetch for the past `days` days.
    Returns list of (unix_sec, close) sorted ascending by time."""
    loop = asyncio.get_event_loop()
    now_ms   = int(time.time()) * 1000
    start_ms = now_ms - days * 86400 * 1000
    cursor   = start_ms

    all_rows: list[tuple[int, float]] = []
    MAX_ITERS = 500

    for i in range(MAX_ITERS):
        if cursor >= now_ms:
            break
        try:
            ohlcv = await loop.run_in_executor(
                None,
                lambda c=cursor: _cb.fetch_ohlcv(product, "1m", c, 300),
            )
        except Exception as e:
            log(f"  {product} fetch error at iter {i}: {e}")
            break
        if not ohlcv:
            break
        for row in ohlcv:
            ts = int(row[0]) // 1000
            close = float(row[4])
            if close > 0:
                all_rows.append((ts, close))
        last_ts_ms = int(ohlcv[-1][0]) + 60_000
        if last_ts_ms <= cursor:
            break    # stuck — paginator returning same range
        cursor = last_ts_ms
        await asyncio.sleep(0.12)   # stay under 10 req/s

    # Dedupe + sort
    seen = {}
    for ts, px in all_rows:
        seen[ts] = px
    return sorted(seen.items())


def _compute_profile(rows: list[tuple[int, float]]) -> dict[int, dict[int, float]]:
    """Given sorted [(ts, close), ...], compute (dow, hour) → mean 1h realized
    vol in percent. Returns {dow: {hour: mean_vol_pct}}."""
    if len(rows) < LOOKBACK_MIN + 2:
        return {}

    by_ts = {ts: px for ts, px in rows}

    bins: dict[int, dict[int, list[float]]] = {}
    for ts, _ in rows:
        # Gather contiguous prior-60-min closes
        closes: list[float] = []
        for m in range(LOOKBACK_MIN + 1):
            p = by_ts.get(ts - m * 60)
            if p is None:
                break
            closes.append(p)
        if len(closes) < LOOKBACK_MIN // 2:
            continue
        closes.reverse()

        rets: list[float] = []
        for i in range(1, len(closes)):
            if closes[i-1] > 0 and closes[i] > 0:
                rets.append(math.log(closes[i] / closes[i-1]))
        if len(rets) < 5:
            continue

        vol_pct = stdev(rets) * 100.0

        # Always bin by UTC so the profile is timezone-stable
        dt   = datetime.fromtimestamp(ts, tz=timezone.utc)
        dow  = dt.weekday()    # 0=Mon
        hour = dt.hour
        bins.setdefault(dow, {}).setdefault(hour, []).append(vol_pct)

    profile: dict[int, dict[int, float]] = {}
    for dow, hours in bins.items():
        profile[dow] = {h: mean(vols) for h, vols in hours.items() if vols}
    return profile


async def build(coins: dict[str, str],
                log: Callable[[str], None] = print) -> dict[str, dict[int, dict[int, float]]]:
    """Fetch history + compute profile for each coin. `coins = {"BTC": "BTC-USD", ...}`"""
    profile: dict[str, dict[int, dict[int, float]]] = {}
    for coin, product in coins.items():
        log(f"  fetching {coin} ({product}) last {DAYS_BACK}d …")
        rows = await _fetch_history(product, DAYS_BACK, log=log)
        log(f"    {len(rows):,} candles")
        profile[coin] = _compute_profile(rows)
        n_bins = sum(len(h) for h in profile[coin].values())
        log(f"    {n_bins} (dow,hour) bins populated")
    return profile


def _save(profile: dict, path: Path = CACHE_FILE) -> None:
    payload = {
        "built_ts": int(time.time()),
        "profile": {
            coin: {str(dow): {str(h): round(v, 6) for h, v in hrs.items()}
                   for dow, hrs in dw.items()}
            for coin, dw in profile.items()
        },
    }
    try:
        path.write_text(json.dumps(payload, indent=2))
    except Exception as e:
        print(f"[vol_profile] cache write failed: {e}")


def _load(path: Path = CACHE_FILE) -> tuple[dict | None, float]:
    """Returns (profile, age_seconds) or (None, 0) if missing / malformed."""
    if not path.exists():
        return None, 0.0
    try:
        raw = json.loads(path.read_text())
        built_ts = float(raw.get("built_ts", 0))
        age = time.time() - built_ts
        profile: dict[str, dict[int, dict[int, float]]] = {}
        for coin, dw in (raw.get("profile") or {}).items():
            profile[coin] = {
                int(dow): {int(h): float(v) for h, v in hrs.items()}
                for dow, hrs in dw.items()
            }
        return profile, age
    except Exception as e:
        print(f"[vol_profile] cache load failed: {e}")
        return None, 0.0


async def load_or_build(coins: dict[str, str], force: bool = False,
                        log: Callable[[str], None] = print) -> dict:
    """Return profile from cache, or rebuild if stale/missing/forced."""
    if not force:
        cached, age = _load()
        if cached and age < CACHE_TTL_SEC:
            log(f"[vol_profile] loaded from cache (age {age/3600:.1f}h)")
            return cached

    log(f"[vol_profile] building — takes ~{len(coins) * 12}s …")
    profile = await build(coins, log=log)
    _save(profile)
    log(f"[vol_profile] cached to {CACHE_FILE}")
    return profile


def expected_vol(profile: dict, coin: str, dt: datetime) -> float:
    """Lookup expected 1h realized vol pct for (coin, dow_utc, hour_utc).
    Caller can pass any tz-aware datetime; we convert to UTC internally.
    Returns 0.0 if the profile has no data for that slot."""
    if not profile or coin not in profile:
        return 0.0
    if dt.tzinfo is None:
        # Assume naive == UTC (shouldn't happen in practice)
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    return profile[coin].get(dt_utc.weekday(), {}).get(dt_utc.hour, 0.0)


# ── CLI: build and print a summary when run directly ───────────────────────

if __name__ == "__main__":
    async def _cli() -> None:
        p = await load_or_build(
            {"BTC": "BTC-USD", "ETH": "ETH-USD", "XRP": "XRP-USD"},
            force=False,
        )
        from rich.console import Console
        from rich.table import Table
        con = Console()
        days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        for coin in p:
            con.print(f"\n[bold cyan]{coin}[/bold cyan]  expected 1h vol % by hour (rows=day)")
            tbl = Table(show_header=True, box=None, header_style="bold", padding=(0,1))
            tbl.add_column("Day")
            for h in range(24):
                tbl.add_column(f"{h:02d}", justify="right")
            for dow in range(7):
                row = [days[dow]]
                for h in range(24):
                    v = p[coin].get(dow, {}).get(h)
                    if v is None:
                        row.append("-")
                    else:
                        col = "red" if v > 0.08 else "yellow" if v > 0.04 else "green"
                        row.append(f"[{col}]{v*100:.1f}[/{col}]")
                tbl.add_row(*row)
            con.print(tbl)

    asyncio.run(_cli())
