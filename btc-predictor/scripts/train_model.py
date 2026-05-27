#!/usr/bin/env python3
"""Train the LightGBM 15-min candle close predictor."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import joblib  # noqa: E402

from src.training.trainer import (  # noqa: E402
    ALL_FEATURE_NAMES,
    VENUE_AGNOSTIC_FEATURE_NAMES,
    candle_level_split,
    evaluate,
    load_and_prepare,
    train_model,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
        default=REPO_ROOT / "data" / "processed" / "training_rows.parquet",
    )
    parser.add_argument(
        "--model-out",
        type=Path,
        default=REPO_ROOT / "src" / "model" / "model.pkl",
    )
    parser.add_argument(
        "--feature-names-out",
        type=Path,
        default=REPO_ROOT / "src" / "model" / "feature_names.json",
    )
    parser.add_argument(
        "--metadata-out",
        type=Path,
        default=REPO_ROOT / "src" / "model" / "training_metadata.json",
    )
    parser.add_argument("--half-life-days", type=float, default=90.0)
    parser.add_argument(
        "--feature-set", choices=["all", "venue_agnostic"], default="all",
        help="'all' uses every feature; 'venue_agnostic' drops volume + flow "
             "features (Group B + D + derived flow) so a model trained on "
             "Binance ticks transfers cleanly to a Coinbase tick stream at "
             "serve time. See trainer.py:_VENUE_SENSITIVE for the exact list.",
    )
    args = parser.parse_args()

    feats = (VENUE_AGNOSTIC_FEATURE_NAMES if args.feature_set == "venue_agnostic"
             else ALL_FEATURE_NAMES)

    t0 = time.time()
    print(f"[1/4] Loading {args.data}")
    df = load_and_prepare(args.data)
    print(f"  rows = {len(df):,}   windows = {df['window_open'].nunique():,}")
    print(f"  feature-set = {args.feature_set}  →  {len(feats)} features")

    print("\n[2/4] Chronological candle-level split (70/15/15)")
    train_df, val_df, test_df = candle_level_split(df)
    for name, sub in [("train", train_df), ("val", val_df), ("test", test_df)]:
        print(f"  {name:>5}: {sub['window_open'].nunique():>5,} candles, "
              f"{len(sub):>7,} rows, "
              f"{sub['window_open'].min()} → {sub['window_open'].max()}, "
              f"green_ratio={sub['label'].mean():.3f}")

    print(f"\n[3/4] Training LightGBM (recency half_life = {args.half_life_days} days)")
    model = train_model(train_df, val_df,
                        half_life_days=args.half_life_days,
                        feature_names=feats)
    print(f"  best_iteration = {model.best_iteration}")

    args.model_out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"model": model, "feature_cols": feats},
        args.model_out,
    )
    args.feature_names_out.write_text(json.dumps(feats, indent=2))
    print(f"  Saved → {args.model_out}")
    print(f"  Saved → {args.feature_names_out}")

    print("\n[4/4] Evaluating on held-out test set")
    metrics = evaluate(model, test_df, feature_names=feats)

    metadata = {
        # Position features come from the TWAP-60 series during training; the
        # live server reads this flag to decide whether to feed TWAP features
        # to the model at inference time. If you ever rebuild a raw-tick
        # parquet, flip this to False manually.
        "trained_on_twap": True,
        "rows": int(len(df)),
        "windows": int(df["window_open"].nunique()),
        "date_range": [str(df["window_open"].min()), str(df["window_open"].max())],
        "split": {
            "train_candles": int(train_df["window_open"].nunique()),
            "val_candles": int(val_df["window_open"].nunique()),
            "test_candles": int(test_df["window_open"].nunique()),
            "train_rows": int(len(train_df)),
            "val_rows": int(len(val_df)),
            "test_rows": int(len(test_df)),
        },
        "best_iteration": int(model.best_iteration),
        "half_life_days": float(args.half_life_days),
        "feature_set": args.feature_set,
        "n_features": len(feats),
        "feature_names": feats,
        "metrics": metrics,
    }
    args.metadata_out.write_text(json.dumps(metadata, indent=2, default=str))
    print(f"\nMetadata → {args.metadata_out}")
    print(f"\nDone in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
