"""Training dataset builder.

Replays Binance daily tick zips, slices each 15-minute candle, and samples
features at predefined seconds within each window. Each sample inherits the
label of the window it belongs to (1 = window closed green).

Pipeline:
  1. compute_baselines(zip_paths)   — first pass: avg_15min_volume,
                                       avg_trade_rate_per_sec
  2. build_training_dataset(...)    — second pass: per-window sampling
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from src.collector.historical import load_trade_zip
from src.features.extractor import (
    FEATURE_NAMES,
    TWAP_WINDOW_SECS,
    WIN_SECS,
    compute_features,
    compute_twap_per_sec,
)

# Sample at every minute boundary within the 15-min window. Skips sec 0 (not
# enough data yet) and the close itself (label is trivial there).
DEFAULT_SAMPLE_SECONDS: tuple[int, ...] = tuple(range(60, 900, 60))  # 60..840

# Windows with fewer than this many ticks are dropped (sparse / data outages)
MIN_TICKS_PER_WINDOW = 50


def compute_baselines(
    zip_paths: list[Path],
    train_ratio: float = 0.7,
) -> dict[str, float]:
    """Compute baseline scalars from the TRAINING portion of zip_paths.

    Returns:
        avg_15min_volume       — mean BTC volume per 15-min window
        avg_trade_rate_per_sec — mean trades per second across the period
    """
    n_train = max(1, int(len(zip_paths) * train_ratio))
    train_paths = zip_paths[:n_train]

    total_qty = 0.0
    total_trades = 0
    total_seconds = 0.0
    for zp in train_paths:
        df = load_trade_zip(zp)
        if len(df) == 0:
            continue
        total_qty += float(df["qty"].sum())
        total_trades += len(df)
        span = (df["ts"].iloc[-1] - df["ts"].iloc[0]).total_seconds()
        total_seconds += span if span > 0 else 86400.0

    avg_qty_per_sec = total_qty / total_seconds
    return {
        "avg_15min_volume": avg_qty_per_sec * 900.0,
        "avg_trade_rate_per_sec": total_trades / total_seconds,
        "_train_zips": len(train_paths),
        "_total_trades_train": total_trades,
        "_total_seconds_train": total_seconds,
    }


def _build_sub_minute_candles(
    ts_i8: np.ndarray,
    prices: np.ndarray,
    window_open_ns: int,
) -> list[dict]:
    """Return [{minute_idx, open, close}, ...] for each 1-min sub-candle that
    has at least one tick. minute_idx 0 = first minute of the window."""
    minute_starts = window_open_ns + np.arange(16) * 60 * 1_000_000_000
    bounds = np.searchsorted(ts_i8, minute_starts, side="left")
    candles = []
    for i in range(15):
        start, end = int(bounds[i]), int(bounds[i + 1])
        if end > start:
            candles.append({
                "minute_idx": i,
                "open": float(prices[start]),
                "close": float(prices[end - 1]),
            })
    return candles


def _rows_for_window(
    w: pd.DataFrame,
    window_open_ns: int,
    prior_candle: dict | None,
    baselines: dict,
    sample_seconds: Iterable[int],
    twap_per_sec: np.ndarray,
) -> tuple[list[dict], dict]:
    """Produce one row per sample point + the window's TWAP-OHLC.

    All price-derived features (and the label) use the TWAP-60 smoothed
    series in cfb / Kalshi convention:
      strike      = twap_per_sec[0]   = TWAP of [ws − 60, ws]
      close-twap  = twap_per_sec[900] = TWAP of [ws + 840, ws + 900]
      label       = 1 if close-twap > strike
    Volume / flow / sub-minute candle features remain tick-based.
    """
    ts_i8 = w["ts"].astype("int64").to_numpy()
    prices = w["price"].to_numpy()
    qtys = w["qty"].to_numpy()
    signed = w["signed_qty"].to_numpy()

    twap_open  = float(twap_per_sec[0])         # = strike (cfb convention)
    twap_close = float(twap_per_sec[WIN_SECS])
    twap_high  = float(np.nanmax(twap_per_sec[: WIN_SECS + 1]))
    twap_low   = float(np.nanmin(twap_per_sec[: WIN_SECS + 1]))
    label = int(twap_close > twap_open)

    sub_candles = _build_sub_minute_candles(ts_i8, prices, window_open_ns)
    window_open_ts = pd.Timestamp(window_open_ns, tz="UTC")

    rows: list[dict] = []
    for sample_sec in sample_seconds:
        if sample_sec < TWAP_WINDOW_SECS:
            continue  # current-tick TWAP still needs 60s of within-window data
        sample_ns = window_open_ns + sample_sec * 1_000_000_000
        upto = int(np.searchsorted(ts_i8, sample_ns, side="right"))
        if upto == 0:
            continue
        feats = compute_features(
            ts_i8=ts_i8, prices=prices, qtys=qtys, signed_qtys=signed,
            upto=upto,
            window_open_ns=window_open_ns,
            sample_sec=sample_sec,
            completed_1min_candles=sub_candles,
            prior_candle=prior_candle,
            baselines=baselines,
            twap_per_sec=twap_per_sec,
        )
        feats["window_open"] = window_open_ts
        feats["sample_sec"] = sample_sec
        feats["label"] = label
        rows.append(feats)

    return rows, {
        "open": twap_open, "high": twap_high, "low": twap_low, "close": twap_close,
    }


def build_training_dataset(
    zip_paths: list[Path],
    baselines: dict,
    output_path: Path,
    sample_seconds: Iterable[int] = DEFAULT_SAMPLE_SECONDS,
) -> pd.DataFrame:
    """Replay all zips → write per-(window, sample) feature rows to parquet.

    Maintains a 60s tick carryover buffer between windows so the TWAP-60
    strike (= TWAP of [ws-60, ws]) can be computed for every window
    including the first window of each new day.
    """
    all_rows: list[dict] = []
    prior_candle: dict | None = None
    skipped_sparse = 0
    total_windows = 0
    # Carryover: last 60s of ticks from the previous window, for TWAP strike.
    carry_ts = np.empty(0, dtype=np.int64)
    carry_px = np.empty(0, dtype=np.float64)

    for i, zp in enumerate(zip_paths, 1):
        df = load_trade_zip(zp)
        if len(df) == 0:
            print(f"[{i:>3}/{len(zip_paths)}] {zp.name}: EMPTY")
            continue

        df["window_open"] = df["ts"].dt.floor("15min")
        for window_open, w in df.groupby("window_open", sort=True):
            total_windows += 1
            if len(w) < MIN_TICKS_PER_WINDOW:
                skipped_sparse += 1
                continue
            w = w.reset_index(drop=True)
            window_open_ns = int(pd.Timestamp(window_open).timestamp() * 1_000_000_000)

            # Build the FULL tick arrays (prior 60s + current window) for TWAP.
            ts_now = w["ts"].astype("int64").to_numpy()
            px_now = w["price"].to_numpy()
            ts_full = np.concatenate([carry_ts, ts_now])
            px_full = np.concatenate([carry_px, px_now])
            twap_per_sec = compute_twap_per_sec(ts_full, px_full, window_open_ns)

            rows, ohlc = _rows_for_window(
                w=w,
                window_open_ns=window_open_ns,
                prior_candle=prior_candle,
                baselines=baselines,
                sample_seconds=sample_seconds,
                twap_per_sec=twap_per_sec,
            )
            all_rows.extend(rows)
            prior_candle = ohlc

            # Save last 60s of THIS window's ticks for the next window's strike.
            cutoff_ns = window_open_ns + (WIN_SECS - TWAP_WINDOW_SECS) * 1_000_000_000
            keep = ts_now >= cutoff_ns
            carry_ts = ts_now[keep]
            carry_px = px_now[keep]

        print(
            f"[{i:>3}/{len(zip_paths)}] {zp.name}: "
            f"rows so far {len(all_rows):,}  (windows {total_windows}, "
            f"sparse skipped {skipped_sparse})",
            flush=True,
        )

    out = pd.DataFrame(all_rows)
    # Stable column order: meta first, then features, then label
    meta_cols = ["window_open", "sample_sec"]
    out = out[meta_cols + list(FEATURE_NAMES) + ["label"]]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)
    return out


def print_dataset_summary(df: pd.DataFrame, baselines: dict) -> None:
    print(f"\nTraining dataset: {len(df):,} rows × {df.shape[1]} cols")
    print(f"  windows:        {df['window_open'].nunique():,}")
    print(f"  date range:     {df['window_open'].min()} → {df['window_open'].max()}")
    print(f"  sample seconds: {sorted(df['sample_sec'].unique().tolist())}")
    pos = int(df['label'].sum())
    neg = len(df) - pos
    print(f"  label balance:  {pos:,} green / {neg:,} red "
          f"(green ratio {pos/len(df):.3f})")
    print(f"  baselines:")
    print(f"    avg_15min_volume       = {baselines['avg_15min_volume']:.3f} BTC")
    print(f"    avg_trade_rate_per_sec = {baselines['avg_trade_rate_per_sec']:.2f}")
