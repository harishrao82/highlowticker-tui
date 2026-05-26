"""Binance daily tick zip loader.

Reads Binance Vision daily trade zips and returns a normalized tick DataFrame
with venue-agnostic aggressor labels. This is the offline counterpart to the
live Coinbase WS consumer — both produce the same tick schema downstream.

Tick CSV columns (no header):
    trade_id, price, qty, quote_qty, time_us, is_buyer_maker, is_best_match

Aggressor-side convention (Binance):
    is_buyer_maker == True  → seller was the aggressor (down-tick)
    is_buyer_maker == False → buyer was the aggressor (up-tick)

Normalized output columns:
    ts          datetime64[ns, UTC]
    price       float64
    qty         float64
    signed_qty  float64   (+qty = buy aggressor, -qty = sell aggressor)
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

TICK_COLS = [
    "trade_id", "price", "qty", "quote_qty",
    "time_us", "is_buyer_maker", "is_best_match",
]

TICK_DTYPES = {
    "trade_id": "int64",
    "price": "float64",
    "qty": "float64",
    "quote_qty": "float64",
    "time_us": "int64",
    "is_buyer_maker": "bool",
    "is_best_match": "bool",
}


def load_trade_zip(zip_path: Path) -> pd.DataFrame:
    """Read one Binance daily trade zip → normalized tick DataFrame."""
    with zipfile.ZipFile(zip_path) as z:
        inner = z.namelist()[0]
        with z.open(inner) as f:
            raw = pd.read_csv(f, header=None, names=TICK_COLS, dtype=TICK_DTYPES)

    # Force ns precision; pandas 3.x preserves the source unit (us) otherwise,
    # which breaks downstream int64 arithmetic that assumes nanoseconds.
    ts = pd.to_datetime(raw["time_us"], unit="us", utc=True).dt.as_unit("ns")
    # +qty when buyer is the aggressor (is_buyer_maker == False)
    sign = np.where(raw["is_buyer_maker"], -1.0, 1.0)
    df = pd.DataFrame({
        "ts": ts,
        "price": raw["price"].to_numpy(),
        "qty": raw["qty"].to_numpy(),
        "signed_qty": raw["qty"].to_numpy() * sign,
    })
    return df.sort_values("ts").reset_index(drop=True)


def list_zips(zip_dir: Path, pattern: str = "BTCUSDT-trades-*.zip") -> list[Path]:
    """Return all matching daily zips, chronologically sorted."""
    zips = sorted(Path(zip_dir).glob(pattern))
    if not zips:
        raise FileNotFoundError(f"No zips matching {pattern!r} under {zip_dir}")
    return zips
