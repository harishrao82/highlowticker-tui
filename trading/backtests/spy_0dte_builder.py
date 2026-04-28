#!/usr/bin/env python3
"""
SPY 0DTE probability surface builder.

For any time T (seconds since 9:30 AM ET) and delta D (% change from day open),
computes P(day closes above open) from historical 1-min SPY data via Polygon.io.

This is the empirical "option delta" — the historical probability that SPY ends
the day above its opening price given where it is right now.

Output: ~/.spy_0dte_surface.json  (same format as ~/.btc_model_surface.json)
Viewer: python3 btc_surface_viewer.py --surface ~/.spy_0dte_surface.json --port 7333

Usage:
    python3 spy_0dte_builder.py --apikey YOUR_KEY
    python3 spy_0dte_builder.py --apikey YOUR_KEY --days 180
"""
import argparse
import asyncio
import json
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV

# ── Paths & config ──────────────────────────────────────────────────────────────
CACHE_FILE   = Path.home() / ".spy_0dte_cache.json"
SURFACE_FILE = Path.home() / ".spy_0dte_surface.json"
POLY_BASE    = "https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute"

ET = ZoneInfo("America/New_York")
MARKET_OPEN_SEC  = 9 * 3600 + 30 * 60   # 9:30 AM in seconds since midnight
MARKET_CLOSE_SEC = 16 * 3600             # 4:00 PM
DAY_SEC          = MARKET_CLOSE_SEC - MARKET_OPEN_SEC   # 23,400s = 390 min

# Rate limit: free plan = 5 req/min → 13s between requests
RATE_LIMIT_SLEEP = 13.0

# Surface grid
T_STEP = 60      # 1-minute resolution (seconds)
D_MIN  = -3.00   # % delta
D_MAX  = +3.00
D_STEP = 0.02    # 0.02% steps → 301 delta values

# Kernel smoother bandwidths
KS_SIGMA_T = 300.0   # 5 minutes
KS_SIGMA_D = 0.05    # 0.05% delta


# ── Polygon fetch ───────────────────────────────────────────────────────────────

async def fetch_spy_bars(api_key: str, days: int) -> list[dict]:
    """
    Download 1-min SPY bars for the last N calendar days via Polygon.io.
    Handles cursor-based pagination. Sleeps 13s between requests (free plan limit).
    Returns list of {ts_ms, open, close} dicts sorted ascending.
    """
    end_date   = date.today()
    start_date = end_date - timedelta(days=days)
    print(f"Downloading SPY 1-min bars {start_date} → {end_date}  (rate-limited, ~13s/request)")

    all_bars = []
    url = (f"{POLY_BASE}/{start_date}/{end_date}"
           f"?apiKey={api_key}&limit=50000&sort=asc&adjusted=true")
    request_count = 0

    async with httpx.AsyncClient(timeout=30) as client:
        while url:
            try:
                r = await client.get(url)
                data = r.json()
            except Exception as e:
                print(f"  Fetch error: {e} — retrying in 30s")
                await asyncio.sleep(30)
                continue

            status = data.get("status", "")
            if "exceeded" in data.get("error", "").lower():
                print("  Rate limited — waiting 60s")
                await asyncio.sleep(60)
                continue
            if status not in ("OK", "DELAYED"):
                print(f"  API error: {data.get('error', status)}")
                break

            results = data.get("results", [])
            all_bars.extend(results)
            request_count += 1

            next_url = data.get("next_url")
            if next_url:
                url = f"{next_url}&apiKey={api_key}"
                print(f"  {len(all_bars):,} bars fetched so far… (page {request_count})")
                await asyncio.sleep(RATE_LIMIT_SLEEP)
            else:
                url = None

    # Deduplicate, sort
    seen, unique = set(), []
    for b in all_bars:
        ts = b["t"]
        if ts not in seen:
            seen.add(ts)
            unique.append({"ts_ms": ts, "open": b["o"], "close": b["c"]})
    unique.sort(key=lambda x: x["ts_ms"])
    print(f"  Done — {len(unique):,} unique 1-min bars across ~{request_count} requests")
    return unique


# ── Build trading day windows ────────────────────────────────────────────────────

def bars_to_days(bars: list[dict]) -> list[dict]:
    """
    Group bars into trading days. Each day:
      - open_price: first bar's open (9:30 AM)
      - close_price: last bar's close (4:00 PM)
      - winner: "Up" if close_price >= open_price else "Down"
      - minutes: list of {t_sec (since open), delta_pct}

    Only days with >= 380 of 390 minutes are kept.
    """
    # Group by calendar date in ET
    by_date: dict[date, list] = {}
    for b in bars:
        dt_et = datetime.fromtimestamp(b["ts_ms"] / 1000, tz=ET)
        d = dt_et.date()
        by_date.setdefault(d, []).append((dt_et, b))

    days = []
    for d, day_bars in sorted(by_date.items()):
        # Filter to regular session only: 9:30–16:00 ET
        session = [
            (dt, b) for dt, b in day_bars
            if dt.hour * 3600 + dt.minute * 60 >= MARKET_OPEN_SEC
            and dt.hour * 3600 + dt.minute * 60 < MARKET_CLOSE_SEC
        ]
        if len(session) < 380:
            continue   # incomplete day

        session.sort(key=lambda x: x[0])
        open_price  = session[0][1]["open"]
        close_price = session[-1][1]["close"]
        winner      = "Up" if close_price >= open_price else "Down"

        minutes = []
        for dt, b in session:
            t_sec = (dt.hour * 3600 + dt.minute * 60) - MARKET_OPEN_SEC
            delta = (b["close"] - open_price) / open_price * 100
            minutes.append({"t_sec": t_sec, "delta": round(delta, 4)})

        days.append({
            "date":        d.isoformat(),
            "open_price":  open_price,
            "close_price": close_price,
            "winner":      winner,
            "minutes":     minutes,
        })

    return days


# ── Model ────────────────────────────────────────────────────────────────────────

def _features(t_sec: np.ndarray, delta: np.ndarray) -> np.ndarray:
    t = t_sec / DAY_SEC   # normalised 0→1
    return np.column_stack([t, delta, t**2, delta**2, t * delta])


def fit_model(days: list[dict]):
    t_list, d_list, y_list = [], [], []
    for day in days:
        y = 1 if day["winner"] == "Up" else 0
        for m in day["minutes"]:
            if m["t_sec"] == 0:
                continue
            t_list.append(m["t_sec"])
            d_list.append(m["delta"])
            y_list.append(y)

    train_t = np.array(t_list, dtype=float)
    train_d = np.array(d_list, dtype=float)
    train_y = np.array(y_list, dtype=float)

    print(f"  Training on {len(train_y):,} observations from {len(days)} trading days")
    X     = _features(train_t, train_d)
    base  = LogisticRegression(max_iter=1000, C=1.0)
    model = CalibratedClassifierCV(base, cv=5, method="isotonic")
    model.fit(X, train_y)

    # Calibration check
    p_pred = model.predict_proba(X)[:, 1]
    bins   = np.linspace(0, 1, 11)
    print("  Calibration (pred → actual):")
    for i in range(10):
        mask = (p_pred >= bins[i]) & (p_pred < bins[i+1])
        if mask.sum() > 50:
            print(f"    {bins[i]:.1f}–{bins[i+1]:.1f}: actual={train_y[mask].mean():.3f}  n={mask.sum():,}")

    return model, train_t, train_d, train_y


# ── Surface precomputation ────────────────────────────────────────────────────────

def build_surface(model, train_t, train_d, train_y, days) -> dict:
    t_vals = np.arange(0, DAY_SEC + T_STEP, T_STEP, dtype=float)   # 0, 60, …, 23400
    d_vals = np.round(np.arange(D_MIN, D_MAX + D_STEP / 2, D_STEP), 4)

    n_t, n_d = len(t_vals), len(d_vals)
    print(f"  Grid: {n_t} time points × {n_d} delta values = {n_t * n_d:,} cells")

    T_grid, D_grid = np.meshgrid(t_vals, d_vals, indexing="ij")
    T_flat = T_grid.ravel()
    D_flat = D_grid.ravel()

    X_flat = _features(T_flat, D_flat)
    P_flat = model.predict_proba(X_flat)[:, 1]

    print("  Computing kernel n_eff…")
    chunk     = 1000
    neff_flat = np.zeros(len(T_flat))
    for i in range(0, len(T_flat), chunk):
        t_c = T_flat[i:i+chunk][:, None]
        d_c = D_flat[i:i+chunk][:, None]
        wt  = np.exp(
            -(((train_t[None, :] - t_c) / KS_SIGMA_T) ** 2)
            -(((train_d[None, :] - d_c) / KS_SIGMA_D) ** 2)
        )
        w_sum  = wt.sum(axis=1)
        w_sum2 = (wt ** 2).sum(axis=1)
        neff_flat[i:i+chunk] = np.where(w_sum2 > 0, w_sum ** 2 / w_sum2, 0)
        if i % 50000 == 0 and i > 0:
            print(f"    {i/len(T_flat)*100:.0f}%…")

    P_grid    = P_flat.reshape(n_t, n_d)
    Neff_grid = neff_flat.reshape(n_t, n_d)

    surface_rows = []
    for ti in range(n_t):
        row = []
        for di in range(n_d):
            row.append({
                "p_up":  round(float(P_grid[ti, di]), 4),
                "n_eff": int(Neff_grid[ti, di]),
            })
        surface_rows.append(row)

    return {
        "t_vals":  [float(v) for v in t_vals],
        "d_vals":  [float(v) for v in d_vals],
        "surface": surface_rows,
        "meta": {
            "ticker":     "SPY",
            "n_days":     len(days),
            "n_obs":      len(train_t),
            "built_at":   datetime.now().isoformat(),
            "t_step":     T_STEP,
            "d_step":     D_STEP,
            "d_min":      D_MIN,
            "d_max":      D_MAX,
            "day_sec":    DAY_SEC,
            "market_open_sec": MARKET_OPEN_SEC,
        },
    }


# ── Main ─────────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apikey",  required=True, help="Polygon.io API key")
    parser.add_argument("--days",    type=int, default=180, help="Calendar days to fetch (default 180)")
    parser.add_argument("--refresh", action="store_true", help="Re-download even if cache exists")
    args = parser.parse_args()

    # Load or download bars
    bars = None
    if CACHE_FILE.exists() and not args.refresh:
        print(f"Loading cached bars from {CACHE_FILE}…")
        cached = json.loads(CACHE_FILE.read_text())
        bars = cached.get("bars", [])
        print(f"  {len(bars):,} bars cached ({cached.get('days', '?')} day window)")

    if not bars:
        bars = await fetch_spy_bars(args.apikey, args.days)
        CACHE_FILE.write_text(json.dumps({"days": args.days, "bars": bars}))
        print(f"  Cached to {CACHE_FILE}")

    print("Grouping into trading days…")
    days = bars_to_days(bars)
    print(f"  {len(days)} complete trading days")
    if not days:
        print("ERROR: no complete trading days found. Check date range / API access.")
        return

    # Print win rate
    up_days = sum(1 for d in days if d["winner"] == "Up")
    print(f"  Up days: {up_days}/{len(days)} ({up_days/len(days)*100:.1f}%)")

    print("Fitting logistic regression model…")
    model, train_t, train_d, train_y = fit_model(days)

    print("Precomputing probability surface…")
    surface = build_surface(model, train_t, train_d, train_y, days)

    SURFACE_FILE.write_text(json.dumps(surface))
    size_kb = SURFACE_FILE.stat().st_size // 1024
    print(f"\nSurface saved → {SURFACE_FILE}  ({size_kb} KB)")
    print(f"  {len(surface['t_vals'])} time steps × {len(surface['d_vals'])} delta steps")
    print(f"  Meta: {surface['meta']}")
    print(f"\nView it: python3 btc_surface_viewer.py --surface ~/.spy_0dte_surface.json --port 7333")


if __name__ == "__main__":
    asyncio.run(main())
