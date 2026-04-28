#!/usr/bin/env python3
"""
BTC probability surface builder.

Loads cached 1-min BTC candles, fits the logistic regression model,
then precomputes P(Up) and n_eff at every (time_seconds, delta_pct)
combination on a fine grid and saves to ~/.btc_model_surface.json.

Run this once after a fresh data download, or on a schedule.

Usage:
    python3 btc_model_builder.py
    python3 btc_model_builder.py --days 365 --refresh
"""
import argparse
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

import httpx
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV

# ── Paths ───────────────────────────────────────────────────────────────────────
CACHE_FILE   = Path.home() / ".btc_odds_cache.json"
SURFACE_FILE = Path.home() / ".btc_model_surface.json"
CB_CANDLES   = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
WINDOW_SEC   = 900
CANDLE_SEC   = 60

# Surface grid resolution
T_STEP    = 15      # seconds — every 15 seconds, 0..900
D_MIN     = -1.50   # % delta
D_MAX     = +1.50
D_STEP    = 0.01    # 0.01% steps → 301 delta values

# Kernel smoother bandwidths
KS_SIGMA_T = 60.0
KS_SIGMA_D = 0.05


# ── Data fetching ───────────────────────────────────────────────────────────────

async def fetch_candles(client: httpx.AsyncClient, days: int) -> list[dict]:
    print(f"Downloading {days} days of 1-min BTC candles...")
    all_candles = []
    end   = int(time.time())
    start = end - days * 24 * 3600
    batch = 300 * CANDLE_SEC

    t = start
    total = (end - start) // batch + 1
    done  = 0
    while t < end:
        batch_end = min(t + batch, end)
        try:
            r = await client.get(CB_CANDLES,
                params={"granularity": CANDLE_SEC, "start": t, "end": batch_end},
                timeout=10)
            data = r.json()
            if isinstance(data, list):
                all_candles.extend(data)
            await asyncio.sleep(0.25)
        except Exception as e:
            print(f"  fetch error at {t}: {e}")
            await asyncio.sleep(1)
        t = batch_end
        done += 1
        if done % 20 == 0:
            print(f"  {done/total*100:.0f}% — {len(all_candles):,} candles")

    seen, unique = set(), []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0])
            unique.append({"ts": c[0], "open": c[3], "high": c[2],
                           "low": c[1], "close": c[4]})
    unique.sort(key=lambda x: x["ts"])
    print(f"  Done — {len(unique):,} unique 1-min candles")
    return unique


# ── Windows ─────────────────────────────────────────────────────────────────────

def candles_to_windows(candles: list[dict]) -> list[dict]:
    by_ts = {c["ts"]: c for c in candles}
    windows = []
    if not candles:
        return windows
    first_ts, last_ts = candles[0]["ts"], candles[-1]["ts"]
    t = (first_ts // WINDOW_SEC) * WINDOW_SEC
    while t + WINDOW_SEC <= last_ts:
        wc = [by_ts[t + m * CANDLE_SEC] for m in range(15)
              if (t + m * CANDLE_SEC) in by_ts]
        if len(wc) >= 14:
            op = wc[0]["open"]
            cp = wc[-1]["close"]
            winner = "Up" if cp >= op else "Down"
            minutes = []
            for c in wc:
                idx = (c["ts"] - t) // CANDLE_SEC
                minutes.append({
                    "minute": int(idx),
                    "delta":  round((c["close"] - op) / op * 100, 4),
                })
            windows.append({"winner": winner, "minutes": minutes})
        t += WINDOW_SEC
    return windows


# ── Model ────────────────────────────────────────────────────────────────────────

def _features(t_sec: np.ndarray, delta: np.ndarray) -> np.ndarray:
    t = t_sec / 900.0
    return np.column_stack([t, delta, t**2, delta**2, t * delta])


def fit_model(windows: list[dict]):
    t_list, d_list, y_list = [], [], []
    for w in windows:
        y = 1 if w["winner"] == "Up" else 0
        for m in w["minutes"]:
            if m["minute"] == 0:
                continue
            t_list.append(m["minute"] * 60 + 30)
            d_list.append(m["delta"])
            y_list.append(y)

    train_t = np.array(t_list, dtype=float)
    train_d = np.array(d_list, dtype=float)
    train_y = np.array(y_list, dtype=float)

    X = _features(train_t, train_d)
    base = LogisticRegression(max_iter=1000, C=1.0)
    model = CalibratedClassifierCV(base, cv=5, method="isotonic")
    model.fit(X, train_y)
    print(f"  Trained on {len(train_y):,} observations from {len(windows):,} windows")
    return model, train_t, train_d, train_y


# ── Surface precomputation ────────────────────────────────────────────────────────

def build_surface(model, train_t, train_d, train_y) -> dict:
    """
    Compute P(Up) and n_eff at every grid point and return as a dict
    ready for JSON serialisation.
    """
    t_vals = np.arange(0, WINDOW_SEC + T_STEP, T_STEP, dtype=float)  # 0,15,...,900
    d_vals = np.round(np.arange(D_MIN, D_MAX + D_STEP / 2, D_STEP), 4)  # -1.50,...,+1.50

    n_t, n_d = len(t_vals), len(d_vals)
    print(f"  Grid: {n_t} time points × {n_d} delta values = {n_t * n_d:,} cells")

    # Vectorised prediction over the full grid
    T_grid, D_grid = np.meshgrid(t_vals, d_vals, indexing="ij")   # (n_t, n_d)
    T_flat = T_grid.ravel()
    D_flat = D_grid.ravel()
    X_flat = _features(T_flat, D_flat)
    P_flat = model.predict_proba(X_flat)[:, 1]

    # Kernel n_eff with prefiltering — only consider training points within
    # 3σ of each grid point (exp(-9) ≈ 0 contribution beyond this).
    print("  Computing kernel n_eff for all grid points...")
    T_CUT = 3 * KS_SIGMA_T   # 180s window around each grid t
    D_CUT = 3 * KS_SIGMA_D   # 0.15% window around each grid d
    neff_flat = np.zeros(len(T_flat))
    for i, (tg, dg) in enumerate(zip(T_flat, D_flat)):
        mask = (np.abs(train_t - tg) < T_CUT) & (np.abs(train_d - dg) < D_CUT)
        if not mask.any():
            continue
        wt     = np.exp(-(((train_t[mask] - tg) / KS_SIGMA_T) ** 2)
                        -(((train_d[mask] - dg) / KS_SIGMA_D) ** 2))
        w_sum  = wt.sum()
        w_sum2 = (wt ** 2).sum()
        neff_flat[i] = w_sum ** 2 / w_sum2 if w_sum2 > 0 else 0
        if i % 2000 == 0 and i > 0:
            print(f"    {i/len(T_flat)*100:.0f}%...")

    P_grid    = P_flat.reshape(n_t, n_d)
    Neff_grid = neff_flat.reshape(n_t, n_d)

    # Serialise to lists
    surface_rows = []
    for ti, t in enumerate(t_vals):
        row = []
        for di, d in enumerate(d_vals):
            p    = float(P_grid[ti, di])
            neff = int(Neff_grid[ti, di])
            row.append({"p_up": round(p, 4), "n_eff": neff})
        surface_rows.append(row)

    return {
        "t_vals":    [float(v) for v in t_vals],
        "d_vals":    [float(v) for v in d_vals],
        "surface":   surface_rows,   # surface[ti][di] → {p_up, n_eff}
        "meta": {
            "n_windows":  len(windows_global),
            "n_obs":      len(train_t),
            "built_at":   datetime.utcnow().isoformat() + "Z",
            "t_step":     T_STEP,
            "d_step":     D_STEP,
            "d_min":      D_MIN,
            "d_max":      D_MAX,
        },
    }


# ── Main ─────────────────────────────────────────────────────────────────────────

windows_global = []   # needed by build_surface for meta


async def main():
    global windows_global
    parser = argparse.ArgumentParser()
    parser.add_argument("--days",    type=int,  default=30)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    # Load or download candles
    candles = None
    if CACHE_FILE.exists() and not args.refresh:
        print(f"Loading cached candles from {CACHE_FILE}...")
        cached = json.loads(CACHE_FILE.read_text())
        candles = cached.get("candles", [])
        cached_days = cached.get("days", 0)
        print(f"  {len(candles):,} candles ({cached_days} days)")
        if cached_days < args.days:
            print(f"  Cache too short — refreshing to {args.days} days...")
            candles = None

    if candles is None:
        async with httpx.AsyncClient() as client:
            candles = await fetch_candles(client, args.days)
        CACHE_FILE.write_text(json.dumps({"days": args.days, "candles": candles}))
        print(f"  Saved to {CACHE_FILE}")

    print("Building 15-min windows...")
    windows_global = candles_to_windows(candles)
    print(f"  {len(windows_global):,} windows")

    print("Fitting model...")
    model, train_t, train_d, train_y = fit_model(windows_global)

    print("Precomputing probability surface...")
    surface = build_surface(model, train_t, train_d, train_y)

    SURFACE_FILE.write_text(json.dumps(surface))
    size_kb = SURFACE_FILE.stat().st_size // 1024
    print(f"\nSurface saved to {SURFACE_FILE}  ({size_kb} KB)")
    print(f"  {len(surface['t_vals'])} time steps × {len(surface['d_vals'])} delta steps")
    print(f"  Meta: {surface['meta']}")
    print("\nRun  python3 btc_surface_viewer.py  to launch the web viewer.")


if __name__ == "__main__":
    asyncio.run(main())
