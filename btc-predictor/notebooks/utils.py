"""Helpers for exploring training_rows.parquet in a notebook.

Typical use:

    from notebooks.utils import *
    set_display()
    df = load()
    head(df)
    window(df, idx=5000)
    label_balance(df, by="session_bucket")
    feature_label_corr(df, by_sample_sec=True)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET = REPO_ROOT / "data" / "processed" / "training_rows.parquet"

META_COLS = ["window_open", "sample_sec", "label"]


def _round_numeric(df: pd.DataFrame, n: int = 4) -> pd.DataFrame:
    """Round only numeric columns — leaves datetime columns alone."""
    out = df.copy()
    num = out.select_dtypes(include="number").columns
    out[num] = out[num].round(n)
    return out


def set_display(width: int = 220, max_cols: int = 60, float_fmt: str = "{:.4f}") -> None:
    """Set wider pandas display + nicer floats for notebook output."""
    pd.set_option("display.width", width)
    pd.set_option("display.max_columns", max_cols)
    pd.set_option("display.float_format", float_fmt.format)


def load(path: str | Path = DEFAULT_PARQUET) -> pd.DataFrame:
    """Load the training parquet."""
    return pd.read_parquet(Path(path))


def feature_cols(df: pd.DataFrame) -> list[str]:
    """All non-meta, non-label columns."""
    return [c for c in df.columns if c not in META_COLS]


def head(df: pd.DataFrame, n: int = 10, cols: list[str] | None = None) -> pd.DataFrame:
    """df.head() with consistent rounding + optional column subset."""
    sub = df[META_COLS + cols] if cols else df
    return _round_numeric(sub.head(n))


def window(
    df: pd.DataFrame,
    idx: int | None = None,
    ts: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """All 14 sample rows for ONE window (by integer index OR timestamp)."""
    if (idx is None) == (ts is None):
        raise ValueError("pass exactly one of idx or ts")
    if idx is not None:
        unique_windows = df["window_open"].drop_duplicates().sort_values().to_numpy()
        target = unique_windows[idx]
    else:
        t = pd.Timestamp(ts)
        target = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
    sub = df[df["window_open"] == target].sort_values("sample_sec")
    if sub.empty:
        raise ValueError(f"no rows for window_open={target}")
    return _round_numeric(sub.drop(columns=["window_open"]))


def windows_head(df: pd.DataFrame, n: int = 3, start_idx: int = 0) -> pd.DataFrame:
    """First n full windows (n × 14 rows) starting at start_idx."""
    unique_windows = df["window_open"].drop_duplicates().sort_values().to_numpy()
    targets = unique_windows[start_idx : start_idx + n]
    return _round_numeric(
        df[df["window_open"].isin(targets)]
        .sort_values(["window_open", "sample_sec"])
    )


def stats(df: pd.DataFrame) -> pd.DataFrame:
    """Describe-style table over the 21 feature columns."""
    cols = feature_cols(df)
    return df[cols].describe().T[["count", "min", "mean", "max", "std"]].round(4)


def label_balance(df: pd.DataFrame, by: str | None = None) -> pd.DataFrame:
    """Green/red counts and ratio, overall or grouped by a column."""
    if by is None:
        n = len(df); green = int(df["label"].sum())
        return pd.DataFrame(
            {"n": [n], "green": [green], "red": [n - green],
             "green_ratio": [round(green / n, 4)]},
            index=["all"],
        )
    g = df.groupby(by)["label"].agg(n="count", green="sum")
    g["red"] = g["n"] - g["green"]
    g["green_ratio"] = (g["green"] / g["n"]).round(4)
    return g


def feature_label_corr(
    df: pd.DataFrame,
    by_sample_sec: bool = False,
) -> pd.DataFrame:
    """Correlation between each feature and the binary label.

    by_sample_sec=True returns a feature × sample_sec matrix — useful for
    seeing how predictive power evolves through the candle. Rows are sorted
    by |corr| at the final sample_sec.
    """
    feats = feature_cols(df)
    if not by_sample_sec:
        out = (
            df[feats + ["label"]]
            .corr(numeric_only=True)["label"]
            .drop("label")
        )
        out = out.reindex(out.abs().sort_values(ascending=False).index)
        return out.round(4).to_frame("corr_with_label")

    rows = {}
    for sec, sub in df.groupby("sample_sec"):
        rows[sec] = (
            sub[feats + ["label"]]
            .corr(numeric_only=True)["label"]
            .drop("label")
        )
    out = pd.DataFrame(rows)
    last_sec = sorted(out.columns)[-1]
    out = out.reindex(out[last_sec].abs().sort_values(ascending=False).index)
    return out.round(4)


def summary(df: pd.DataFrame) -> None:
    """One-shot overview printed to stdout."""
    print(f"shape: {df.shape}")
    print(f"windows: {df['window_open'].nunique():,}")
    print(f"date range: {df['window_open'].min()} → {df['window_open'].max()}")
    print(f"sample seconds: {sorted(df['sample_sec'].unique().tolist())}")
    n = len(df); g = int(df["label"].sum())
    print(f"label balance: {g:,} green / {n - g:,} red (green ratio {g/n:.4f})")
